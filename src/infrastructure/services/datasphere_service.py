"""Service for SAP Datasphere data access via Consumption API."""
import logging
import httpx
import xml.etree.ElementTree as ET
import json
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
import asyncio
from datetime import datetime
from pathlib import Path
import pandas as pd
import polars as pl

# Make KeyVaultService import optional (Azure libraries may not be installed)
try:
    from .key_vault_service import get_key_vault_service, KeyVaultService
except ImportError:
    # KeyVaultService not available - will handle gracefully
    get_key_vault_service = None
    KeyVaultService = None
    logger = logging.getLogger(__name__)
    logger.warning("KeyVaultService not available - SAP Datasphere token retrieval will not work. Install Azure libraries: pip install azure-identity azure-keyvault-secrets")
from .odata_converter import get_sql_to_odata_converter, SQLToODataConverter, ODataParams
# Simple rate limiter for API calls
import asyncio
from collections import deque
from time import time

class RateLimiter:
    """Simple rate limiter for API calls."""
    def __init__(self, max_calls_per_minute: int = 60):
        self.max_calls = max_calls_per_minute
        self.calls = deque()
        self.lock = asyncio.Lock()
    
    async def acquire(self):
        """Acquire permission to make an API call."""
        async with self.lock:
            now = time()
            # Remove calls older than 1 minute
            while self.calls and self.calls[0] < now - 60:
                self.calls.popleft()
            
            # Wait if we've exceeded the limit
            if len(self.calls) >= self.max_calls:
                sleep_time = 60 - (now - self.calls[0])
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
                    # Clean up again after sleep
                    while self.calls and self.calls[0] < now - 60:
                        self.calls.popleft()
            
            # Record this call
            self.calls.append(time())
from config.settings import settings

logger = logging.getLogger(__name__)



class DatasphereError(Exception):
    """Base exception for Datasphere operations."""
    pass


class TokenNotFoundError(DatasphereError):
    """Raised when user's access token is not found in Key Vault."""
    pass


class TokenExpiredError(DatasphereError):
    """Raised when the access token has expired."""
    pass


class RefreshTokenExpiredError(DatasphereError):
    """
    Raised when the refresh token has expired.
    
    This indicates the user needs to re-authenticate with SAP Datasphere.
    """
    pass


class DatasphereAPIError(DatasphereError):
    """Raised when the Datasphere API returns an error."""
    def __init__(self, message: str, status_code: int, response_body: Optional[str] = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body


@dataclass
class DatasphereAsset:
    """
    Represents a SAP Datasphere asset (view/table) from the catalog.
    
    Stores only essential fields needed for querying:
    - name: View name (asset_id for queries)
    - label: Human-readable label
    - space_id: Space ID where the asset belongs (extracted from catalog)
    - data_url: URL for data queries (relational preferred)
    - metadata_url: URL for schema queries (relational preferred)
    """
    name: str
    label: Optional[str] = None
    space_id: Optional[str] = None  # Space ID from catalog
    data_url: Optional[str] = None
    metadata_url: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary with only essential fields."""
        return {
            "name": self.name,
            "label": self.label,
            "space_id": self.space_id,
            "data_url": self.data_url,
            "metadata_url": self.metadata_url,
        }
    
    @classmethod
    def from_catalog_item(cls, item: Dict[str, Any]) -> 'DatasphereAsset':
        """
        Create from catalog API response item.
        
        Extracts:
        - name: Asset name (asset_id)
        - space_id: From 'spaceId' or 'space_id' field in catalog response
        - data_url: Prefer relational over analytical
        - metadata_url: Prefer relational over analytical
        """
        # Prefer relational URLs over analytical
        data_url = item.get("assetRelationalDataUrl") or item.get("assetAnalyticalDataUrl")
        metadata_url = item.get("assetRelationalMetadataUrl") or item.get("assetAnalyticalMetadataUrl")
        
        # Extract space_id from catalog response
        # Catalog API may return it as 'spaceId', 'space_id', or in a nested object
        space_id = (
            item.get("spaceId") or 
            item.get("space_id") or 
            item.get("space", {}).get("id") if isinstance(item.get("space"), dict) else None
        )
        
        return cls(
            name=item.get("name", ""),
            label=item.get("label"),
            space_id=space_id,
            data_url=data_url,
            metadata_url=metadata_url,
        )


@dataclass
class DatasphereColumn:
    """
    Represents a column from a Datasphere view/table.
    
    Only essential fields for LLM and query generation:
    - name: Column name (Property Name from $metadata)
    - data_type: OData type (Edm.String, Edm.Int32, etc.)
    - max_length: Maximum length for string columns
    """
    name: str
    data_type: str
    max_length: Optional[int] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary with only essential fields."""
        result = {
            "name": self.name,
            "type": self.data_type,
        }
        if self.max_length is not None:
            result["max_length"] = self.max_length
        return result
    
    def to_schema_string(self) -> str:
        """Get column as schema string for LLM context."""
        type_str = self.data_type.replace("Edm.", "")  # Simplify: Edm.String -> String
        if self.max_length:
            return f"{self.name} ({type_str}, max_length={self.max_length})"
        return f"{self.name} ({type_str})"


@dataclass
class DatasphereParameter:
    """
    Represents an input parameter of a Datasphere view (from Parameters EntityType).
    
    Views may expose parameters via a NavigationProperty "Parameters" pointing to
    an EntityType with key/input properties (e.g. REPORT_UOM_DEF_CS, REPORT_CURRENCY_DEF_USD).
    """
    name: str
    data_type: str
    max_length: Optional[int] = None
    nullable: Optional[bool] = None
    default_value: Optional[str] = None
    precision: Optional[int] = None
    scale: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        out = {"name": self.name, "type": self.data_type}
        if self.max_length is not None:
            out["max_length"] = self.max_length
        if self.nullable is not None:
            out["nullable"] = self.nullable
        if self.default_value is not None:
            out["default_value"] = self.default_value
        if self.precision is not None:
            out["precision"] = self.precision
        if self.scale is not None:
            out["scale"] = self.scale
        return out


@dataclass
class DatasphereViewSchema:
    """
    Schema information for a Datasphere view.

    Contains column info, optional view parameters, and view_type for routing
    (relational vs analytical in mixed flows).
    """
    view_name: str
    columns: List[DatasphereColumn] = field(default_factory=list)
    parameters: List[DatasphereParameter] = field(default_factory=list)
    view_type: Optional[str] = None  # "relational" | "analytical" — which $metadata endpoint succeeded

    @property
    def column_names(self) -> List[str]:
        """Get list of column names."""
        return [col.name for col in self.columns]

    @property
    def has_parameters(self) -> bool:
        """True if this view has input parameters."""
        return len(self.parameters) > 0

    def get_column_names(self) -> List[str]:
        """Return column names (API compatibility)."""
        return self.column_names

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary with essential fields only."""
        out = {
            "view_name": self.view_name,
            "columns": [col.to_dict() for col in self.columns],
            "column_names": self.column_names,
            "has_parameters": self.has_parameters,
            "parameters": [p.to_dict() for p in self.parameters],
        }
        if self.view_type is not None:
            out["view_type"] = self.view_type
        return out

    def get_schema_for_llm(self) -> str:
        """
        Get schema formatted for LLM context (compact format).
        Includes parameters when the view has any.
        """
        cols_str = ", ".join(col.to_schema_string() for col in self.columns)
        out = f"{self.view_name}: {cols_str}"
        if self.parameters:
            params_str = ", ".join(
                f"{p.name} ({p.data_type})" for p in self.parameters
            )
            out += f" | Parameters: {params_str}"
        return out


@dataclass
class DatasphereQueryResult:
    """Container for Datasphere query results."""
    data: List[Dict[str, Any]]
    count: Optional[int] = None
    next_link: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    lazy_frame: Optional[pl.LazyFrame] = None  # LazyFrame for efficient operations
    api_url: Optional[str] = None  # Full API URL used to fetch the data
    
    def __post_init__(self):
        """Create LazyFrame immediately from data for efficient operations."""
        if self.data and self.lazy_frame is None:
            try:
                # Create LazyFrame immediately - operations will be lazy-evaluated
                # Use infer_schema_length=None to infer from all rows (prevents type mismatch errors)
                self.lazy_frame = pl.LazyFrame(self.data, infer_schema_length=None)
                logger.debug(f"Created LazyFrame with {len(self.data):,} rows for efficient operations")
            except Exception as e:
                logger.warning(f"Failed to create LazyFrame: {e}, will use data list instead")
                self.lazy_frame = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON response."""
        result = {
            "data": self.data,
            "row_count": len(self.data),
        }
        if self.count is not None:
            result["total_count"] = self.count
        if self.next_link:
            result["next_link"] = self.next_link
        if self.metadata:
            result["metadata"] = self.metadata
        return result
    
    def get_lazy_frame(self) -> Optional[pl.LazyFrame]:
        """Get LazyFrame for efficient operations. Creates it if not already created."""
        if self.lazy_frame is None and self.data:
            try:
                # Use infer_schema_length=None to infer from all rows (prevents type mismatch errors)
                self.lazy_frame = pl.LazyFrame(self.data, infer_schema_length=None)
            except Exception as e:
                logger.warning(f"Failed to create LazyFrame: {e}")
        return self.lazy_frame


@dataclass 
class DatasphereAssetsResult:
    """
    Result from listing Datasphere assets.
    
    Contains both the list of view names (for LLM) and full asset details (for state).
    """
    view_names: List[str]  # Just names for LLM/table selection
    assets: Dict[str, DatasphereAsset]  # Full asset info keyed by name for state storage
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "view_names": self.view_names,
            "assets": {name: asset.to_dict() for name, asset in self.assets.items()},
        }


class DatasphereService:
    """
    Service for accessing SAP Datasphere data via the Catalog and Consumption APIs.
    
    Uses the new SAP Datasphere API format (v1/datasphere) as per SAP documentation:
    https://help.sap.com/docs/SAP_DATASPHERE
    
    API Endpoints (per SAP documentation):
    - List Spaces: /api/v1/datasphere/consumption/catalog/spaces
    - List All Assets: /api/v1/datasphere/consumption/catalog/assets
    - List Space Assets: /api/v1/datasphere/consumption/catalog/spaces('<space_id>')/assets
    - Get Asset: /api/v1/datasphere/consumption/catalog/spaces('<space_id>')/assets('<asset_id>')
    - Analytical Data: /api/v1/datasphere/consumption/analytical/<space_id>/<asset_id>/<asset_id>[(<params>)/Set]
    - Relational Data: /api/v1/datasphere/consumption/relational/<space_id>/<asset_id>/<asset_id>[(<params>)/Set]
    Note: <asset_id> appears TWICE in the path (once after space, once before params)
    
    Note: The former /api/v1/dwc/ format is deprecated.
    
    This service:
    1. Retrieves user-specific OAuth tokens from Azure Key Vault
    2. Lists available views via Catalog API
    3. Gets column metadata via $metadata endpoint
    4. Executes OData queries against views
    5. Returns formatted JSON responses
    
    The bot does not access Azure Key Vault directly - all token
    management is handled by this service.
    """
    
    # API path prefix (new format per SAP documentation)
    API_PREFIX = "/api/v1/datasphere"
    
    def __init__(
        self,
        key_vault_service: Optional[KeyVaultService] = None,
        odata_converter: Optional[SQLToODataConverter] = None
    ):
        """
        Initialize the Datasphere service.
        
        Args:
            key_vault_service: Optional KeyVaultService instance (uses singleton if not provided)
            odata_converter: Optional SQLToODataConverter instance (uses singleton if not provided)
        """
        # Handle optional KeyVaultService
        if key_vault_service:
            self._key_vault_service = key_vault_service
        elif get_key_vault_service:
            try:
                self._key_vault_service = get_key_vault_service()
            except Exception as e:
                logger.warning(f"Failed to get KeyVaultService: {e}. Token retrieval will not work.")
                self._key_vault_service = None
        else:
            self._key_vault_service = None
            logger.warning("KeyVaultService not available - SAP Datasphere token retrieval will not work.")
        self._odata_converter = odata_converter or get_sql_to_odata_converter()
        self._base_url = self._normalize_base_url(settings.sap_odata_url)
        self._space_id = settings.sap_datasphere_space_id
        self._timeout = settings.sap_datasphere_timeout
        
        # Rate limiter for API calls (200 calls per minute per SAP limits)
        self._rate_limiter = RateLimiter(settings.sap_max_api_calls_per_minute)
        logger.info(f"Rate limiter initialized: {settings.sap_max_api_calls_per_minute} calls/minute")
        
        if not self._base_url:
            logger.warning("SAP Datasphere base URL not configured")
        else:
            logger.info(f"SAP Datasphere service initialized with API: {self.API_PREFIX}")
    
    def _normalize_base_url(self, base_url: Optional[str]) -> Optional[str]:
        """
        Normalize the base URL to ensure it has a protocol.
        
        Args:
            base_url: Base URL (may or may not have protocol)
            
        Returns:
            Normalized URL with protocol, or None if input is None/empty
        """
        if not base_url:
            logger.warning("SAP Datasphere base URL is not set in configuration")
            return None
        
        base_url = base_url.strip()
        
        if not base_url:
            logger.warning("SAP Datasphere base URL is empty after trimming")
            return None
        
        # If already has protocol, return as-is (remove trailing slash)
        if base_url.startswith("http://") or base_url.startswith("https://"):
            normalized = base_url.rstrip('/')
            logger.info(f"SAP Datasphere base URL (with protocol): {normalized}")
            return normalized
        
        # Add https:// by default (SAP Datasphere uses HTTPS)
        normalized = f"https://{base_url}".rstrip('/')
        logger.info(f"Normalized SAP Datasphere base URL (added https://): {normalized}")
        return normalized
    
    def _build_url(self, path: str) -> str:
        """
        Build a full URL from base URL and path with validation.
        
        Args:
            path: URL path (e.g., "{API_PREFIX}/consumption/catalog/assets")
            
        Returns:
            Full URL with protocol
            
        Raises:
            DatasphereError: If base URL is not configured
        """
        if not self._base_url:
            # Check what the raw setting value is for better error message
            raw_url = settings.sap_odata_url
            error_msg = (
                "SAP Datasphere base URL is not configured. "
                "Please set SAP_ODATA_URL environment variable. "
            )
            if raw_url:
                error_msg += f"Current value: '{raw_url}' (may be missing protocol or invalid)"
            else:
                error_msg += "Current value is None or empty."
            raise DatasphereError(error_msg)
        
        # Ensure path starts with /
        if not path.startswith('/'):
            path = '/' + path
        
        # Remove trailing / from base_url if present (already done in _normalize_base_url)
        url = f"{self._base_url}{path}"
        logger.debug(f"Built URL: {url}")
        return url
    
    def _format_input_parameters(
        self,
        view_name: str,
        input_parameters: Dict[str, Any],
        underscore_prefix: bool = True,
    ) -> str:
        """
        Format SAP Datasphere input parameters for the URL path.
        
        Expected SAP consumption format (both relational and analytical):
        /<space>/<asset_id>/_<asset_id>(Param1='val',Param2=123)/Set
        
        Args:
            view_name: Asset/view technical name (asset_id)
            input_parameters: Dict of input parameter name -> value
            underscore_prefix: When True, prefix the second asset_id with "_"
        
        Returns:
            URL path suffix like "/_ViewName(Param='Value')/Set"
        """
        if not input_parameters:
            prefix = f"/_{view_name}" if underscore_prefix else f"/{view_name}"
            return f"{prefix}/Set"
        
        parts: list[str] = []
        for key, val in input_parameters.items():
            if val is None:
                continue
            if isinstance(val, str):
                escaped = val.replace("'", "''")
                parts.append(f"{key}='{escaped}'")
            elif isinstance(val, bool):
                parts.append(f"{key}={'true' if val else 'false'}")
            else:
                parts.append(f"{key}={val}")
        
        params_str = ",".join(parts)
        prefix = f"/_{view_name}" if underscore_prefix else f"/{view_name}"
        return f"{prefix}({params_str})/Set"
    
    def _get_user_token(self, user_id: str, state: Optional[Dict[str, Any]] = None) -> str:
        """
        Retrieve the user's SAP Datasphere access token from state or Key Vault.
        
        **IMPORTANT**: This method ONLY retrieves tokens - it does NOT refresh them.
        Token refresh only happens before query start via refresh_user_token().
        
        Priority order:
        1. Token from state (if provided) - fastest, no Key Vault call
        2. Token from Key Vault (fallback for API calls outside LangGraph workflow)
        
        Args:
            user_id: The user identifier
            state: Optional state dictionary to check for sap_access_token first
            
        Returns:
            The access token string
            
        Raises:
            TokenNotFoundError: If token is not found
        """
        # First, check state for token (fastest, no Key Vault call)
        if state and state.get("sap_access_token"):
            token = state.get("sap_access_token")
            logger.debug(f"Using SAP access token from state for user: {user_id}")
            return token
        
        # Fetch token from Key Vault (fallback for API calls outside LangGraph workflow)
        if not self._key_vault_service:
            raise TokenNotFoundError(
                f"KeyVaultService is not available. "
                f"Azure Key Vault libraries may not be installed. "
                f"Install with: pip install azure-identity azure-keyvault-secrets"
            )
        
        logger.info(f"Retrieving SAP Datasphere token from Key Vault for user: {user_id}")
        
        token = self._key_vault_service.get_user_token(user_id)
        
        if not token:
            logger.error(f"No SAP Datasphere token found for user: {user_id}")
            raise TokenNotFoundError(
                f"SAP Datasphere access token not found for user: {user_id}. "
                "Please ensure the user has authenticated with SAP Datasphere."
            )
        
        return token
    
    async def refresh_user_token(self, user_id: str) -> str:
        """
        Refresh the user's SAP Datasphere access token using refresh token.
        
        This method:
        1. Checks the current token's creation date from Key Vault
        2. Only refreshes if the token is older than 45 minutes
        3. Gets the refresh token from Key Vault
        4. Calls SAP OAuth token endpoint to get new access token
        5. Updates only the access token in Key Vault (refresh token is not updated)
        
        Args:
            user_id: The user identifier
            
        Returns:
            The access token string (either existing or newly refreshed)
            
        Raises:
            TokenNotFoundError: If refresh token is not found
            DatasphereError: If token refresh fails
        """
        import base64
        
        # Get refresh token from Key Vault
        if not self._key_vault_service:
            raise TokenNotFoundError(
                f"KeyVaultService is not available. "
                f"Azure Key Vault libraries may not be installed. "
                f"Install with: pip install azure-identity azure-keyvault-secrets"
            )
        
        # Check if token needs refresh based on creation date
        token_with_metadata = self._key_vault_service.get_user_token_with_metadata(user_id)
        if token_with_metadata:
            token, created_on = token_with_metadata
            if created_on:
                # Calculate age of token (handle timezone-aware and naive datetimes)
                now = datetime.now(created_on.tzinfo) if created_on.tzinfo else datetime.now()
                token_age = now - created_on
                age_minutes = token_age.total_seconds() / 60
                
                logger.info(f"Token for user {user_id} is {age_minutes:.1f} minutes old (created: {created_on})")
                
                # Only refresh if token is older than 45 minutes
                if age_minutes <= 45:
                    logger.info(f"Token for user {user_id} is still valid (age: {age_minutes:.1f} minutes <= 45 minutes). Skipping refresh.")
                    return token
                else:
                    logger.info(f"Token for user {user_id} is older than 45 minutes (age: {age_minutes:.1f} minutes). Proceeding with refresh.")
            else:
                logger.warning(f"Token creation date not available for user {user_id}. Proceeding with refresh to be safe.")
        else:
            logger.warning(f"No existing token found for user {user_id}. Proceeding with refresh.")
        
        refresh_token = self._key_vault_service.get_user_refresh_token(user_id)
        if not refresh_token:
            logger.error(f"Refresh token not found for user: {user_id}")
            raise TokenNotFoundError(
                f"SAP Datasphere refresh token not found for user: {user_id}. "
                "Please ensure the user has authenticated with SAP Datasphere."
            )
        
        # Get OAuth configuration from settings
        token_url = settings.sap_oauth_token_url
        client_id = settings.sap_client_id
        client_secret = settings.sap_client_secret
        
        if not token_url:
            raise DatasphereError(
                "SAP OAuth token URL not configured. Set SAP_OAUTH_TOKEN_URL environment variable."
            )
        if not client_id or not client_secret:
            raise DatasphereError(
                "SAP OAuth credentials not configured. Set SAP_CLIENT_ID and SAP_CLIENT_SECRET environment variables."
            )
        
        # Prepare Basic Auth header
        credentials = f"{client_id}:{client_secret}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        
        headers = {
            "Authorization": f"Basic {encoded_credentials}",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        
        # Prepare form data
        data = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }
        
        logger.info(f"Refreshing SAP Datasphere token for user: {user_id}")
        
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                response = await client.post(token_url, headers=headers, data=data)
                
                if response.status_code == 200:
                    token_data = response.json()
                    new_access_token = token_data.get("access_token")
                    
                    if not new_access_token:
                        raise DatasphereError("Token refresh response missing access_token")
                    
                    logger.info(
                        f"Token refresh successful for user {user_id}. "
                        f"New access token received."
                    )
                    
                    # Update only access token in Key Vault (don't update refresh token)
                    if self._key_vault_service:
                        self._key_vault_service.update_user_token(
                            user_id=user_id,
                            access_token=new_access_token,
                            refresh_token=None,  # Don't update refresh token
                        )
                        
                        # Verify access token was updated
                        try:
                            updated_access_token = self._key_vault_service.get_user_token(user_id)
                            if updated_access_token == new_access_token:
                                logger.info(f"✅ Verified: Access token successfully updated in Key Vault for user: {user_id}")
                            else:
                                logger.warning(f"⚠️ Warning: Access token may not have been updated correctly for user: {user_id}")
                        except Exception as verify_error:
                            logger.warning(f"Could not verify token update in Key Vault: {verify_error}")
                    else:
                        logger.warning("KeyVaultService not available - token not saved to Key Vault")
                    
                    logger.info(f"✅ Successfully refreshed and updated access token in Key Vault for user: {user_id}")
                    return new_access_token
                else:
                    # Parse error response to check for expired refresh token
                    error_data = {}
                    try:
                        error_data = response.json()
                    except:
                        # If response is not JSON, use text
                        error_text = response.text
                    else:
                        error_text = response.text
                    
                    error_code = error_data.get("error", "")
                    error_description = error_data.get("error_description", "")
                    
                    # Check if refresh token has expired
                    if error_code == "invalid_grant" and "refresh token expired" in error_description.lower():
                        logger.error(
                            f"❌ Refresh token expired for user {user_id}. "
                            f"User needs to re-authenticate with SAP Datasphere."
                        )
                        raise RefreshTokenExpiredError(
                            "Your SAP Datasphere credentials have expired. Please login again."
                        )
                    
                    # Other token refresh errors
                    error_msg = f"Token refresh failed: {response.status_code} - {error_text}"
                    logger.error(f"Failed to refresh token for user {user_id}: {error_msg}")
                    raise DatasphereError(error_msg)
                    
        except httpx.RequestError as e:
            logger.error(f"Token refresh request failed for user {user_id}: {e}")
            raise DatasphereError(f"Failed to refresh token: {str(e)}")
    
    # =========================================================================
    # Catalog API - List Spaces and Assets
    # =========================================================================
    
    async def list_catalog_spaces(self, user_id: str, token: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List all spaces the user has access to.
        
        Uses endpoint: {base_url}{API_PREFIX}/consumption/catalog/spaces
        
        Args:
            user_id: The user identifier (for token retrieval)
            token: Optional pre-fetched token (if None, will fetch from Key Vault or state)
            
        Returns:
            List of space dictionaries with id, name, etc.
        """
        logger.info(f"[SAP Catalog] 📂 Fetching spaces for user {user_id} via {self.API_PREFIX}/consumption/catalog/spaces")
        
        if not token:
            token = self._get_user_token(user_id)
        url = self._build_url(f"{self.API_PREFIX}/consumption/catalog/spaces")
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
        
        # Acquire rate limit before API call
        await self._rate_limiter.acquire()
        
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                response = await client.get(url, headers=headers)
                
                if response.status_code == 200:
                    data = response.json()
                    spaces = data.get("value", [])
                    logger.info(f"[SAP Catalog] ✅ Found {len(spaces)} spaces")
                    return spaces
                
                elif response.status_code == 401:
                    logger.error(f"Token expired for user {user_id}")
                    raise TokenExpiredError(
                        f"SAP Datasphere access token has expired. Token should be refreshed before starting queries."
                    )
                
                else:
                    logger.error(f"Catalog Spaces API error: {response.status_code} - {response.text}")
                    raise DatasphereAPIError(
                        f"Failed to list catalog spaces: {response.status_code}",
                        status_code=response.status_code,
                        response_body=response.text
                    )
                    
        except httpx.RequestError as e:
            logger.error(f"Catalog Spaces API request error: {e}")
            raise DatasphereAPIError(
                f"Failed to connect to SAP Datasphere: {str(e)}",
                status_code=503
            )
    
    async def list_catalog_assets(self, user_id: str, token: Optional[str] = None) -> DatasphereAssetsResult:
        """
        List all available views/tables from the Datasphere Catalog API.
        
        Uses endpoint: {base_url}{API_PREFIX}/consumption/catalog/assets
        
        Args:
            user_id: The user identifier (for token retrieval)
            token: Optional pre-fetched token (if None, will fetch from Key Vault or state)
            
        Returns:
            DatasphereAssetsResult with view names and simplified asset details
        """
        logger.info(f"[SAP Catalog] 📂 Fetching views for user {user_id} via {self.API_PREFIX}/consumption/catalog/assets")
        
        if not token:
            token = self._get_user_token(user_id)
        url = self._build_url(f"{self.API_PREFIX}/consumption/catalog/assets")
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
        
        # Acquire rate limit before API call
        await self._rate_limiter.acquire()
        
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                response = await client.get(url, headers=headers)
                
                if response.status_code == 200:
                    data = response.json()
                    return self._parse_catalog_assets(data)
                
                elif response.status_code == 401:
                    logger.error(f"Token expired for user {user_id}")
                    raise TokenExpiredError(
                        f"SAP Datasphere access token has expired. Token should be refreshed before starting queries."
                    )
                
                else:
                    logger.error(f"Catalog API error: {response.status_code} - {response.text}")
                    raise DatasphereAPIError(
                        f"Failed to list catalog assets: {response.status_code}",
                        status_code=response.status_code,
                        response_body=response.text
                    )
                    
        except httpx.TimeoutException:
            logger.error(f"Catalog API timeout for user: {user_id}")
            raise DatasphereAPIError(
                "Request to SAP Datasphere catalog timed out.",
                status_code=504
            )
        except httpx.RequestError as e:
            logger.error(f"Catalog API request error: {e}")
            raise DatasphereAPIError(
                f"Failed to connect to SAP Datasphere: {str(e)}",
                status_code=503
            )
    
    def _parse_catalog_assets(self, data: Dict[str, Any]) -> DatasphereAssetsResult:
        """
        Parse catalog assets response into simplified asset objects.
        
        Extracts essential fields:
        - name: View name (asset_id)
        - label: Human-readable label  
        - space_id: Space ID where asset belongs (extracted from catalog)
        - data_url: URL for data queries (relational preferred)
        - metadata_url: URL for schema queries (relational preferred)
        """
        view_names = []
        assets = {}
        total_items = len(data.get("value", []))
        skipped = 0
        spaces_found = set()
        
        for item in data.get("value", []):
            name = item.get("name")
            if not name:
                skipped += 1
                continue
            
            # Create asset from catalog item (handles URL preference logic and space_id extraction)
            asset = DatasphereAsset.from_catalog_item(item)
            
            # Track spaces found
            if asset.space_id:
                spaces_found.add(asset.space_id)
            
            # Only include assets that have a data URL
            if asset.data_url:
                view_names.append(name)
                assets[name] = asset
            else:
                skipped += 1
        
        logger.info(f"[SAP Catalog] ✅ {len(view_names)} views parsed ({skipped} skipped, no data URL)")
        if spaces_found:
            logger.info(f"[SAP Catalog] 📍 Found {len(spaces_found)} unique space(s): {', '.join(sorted(spaces_found))}")
        return DatasphereAssetsResult(view_names=view_names, assets=assets)
    
    # =========================================================================
    # Metadata API - Get Column Information
    # =========================================================================
    
    async def get_view_schema(
        self,
        user_id: str,
        view_name: str,
        metadata_url: Optional[str] = None,
        space_id: Optional[str] = None,
        token: Optional[str] = None
    ) -> DatasphereViewSchema:
        """
        Get column schema for a Datasphere view using $metadata endpoint.
        
        Args:
            user_id: The user identifier
            view_name: Name of the view
            metadata_url: Direct metadata URL (from catalog asset)
            space_id: Optional space ID (used if metadata_url not provided)
            token: Optional pre-fetched token (if None, will fetch from Key Vault or state)
            
        Returns:
            DatasphereViewSchema with column information (name, type, max_length)
        """
        logger.debug(f"SAP Schema: Fetching {view_name}")
        
        if not token:
            token = self._get_user_token(user_id)
        
        # Use provided metadata URL or build one
        if metadata_url:
            url = metadata_url
        else:
            space = space_id or self._space_id
            if not space:
                raise DatasphereError(
                    "Space ID is required. Provide space_id parameter or set SAP_DATASPHERE_SPACE_ID environment variable."
                )
            # Try relational endpoint first, then analytical as fallback
            url = self._build_url(f"{self.API_PREFIX}/consumption/relational/{space}/{view_name}/$metadata")
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/xml",
        }
        
        # Acquire rate limit before API call
        await self._rate_limiter.acquire()
        
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                response = await client.get(url, headers=headers)

                if response.status_code == 200:
                    schema = self._parse_metadata_xml(view_name, response.text)
                    schema.view_type = "analytical" if "analytical" in url else "relational"
                    return schema

                elif response.status_code == 401:
                    logger.error(f"Token expired for user {user_id}")
                    raise TokenExpiredError(
                        f"SAP Datasphere access token has expired. Token should be refreshed before starting queries."
                    )
                
                elif response.status_code == 404:
                    # Try analytical endpoint if relational failed
                    if "relational" in url:
                        space = space_id or self._space_id
                        if not space:
                            raise DatasphereError("Space ID is required for analytical metadata endpoint")
                        analytical_url = self._build_url(f"{self.API_PREFIX}/consumption/analytical/{space}/{view_name}/$metadata")
                        logger.info(f"Trying analytical metadata URL: {analytical_url}")
                        # Acquire rate limit before retry API call
                        await self._rate_limiter.acquire()
                        response = await client.get(analytical_url, headers=headers)
                        if response.status_code == 200:
                            schema = self._parse_metadata_xml(view_name, response.text)
                            schema.view_type = "analytical"
                            return schema

                    raise DatasphereAPIError(
                        f"View '{view_name}' not found.",
                        status_code=404,
                        response_body=response.text
                    )
                
                else:
                    raise DatasphereAPIError(
                        f"Failed to get metadata: {response.status_code}",
                        status_code=response.status_code,
                        response_body=response.text
                    )
                    
        except httpx.RequestError as e:
            logger.error(f"Metadata request error: {e}")
            raise DatasphereAPIError(
                f"Failed to connect to SAP Datasphere: {str(e)}",
                status_code=503
            )
    
    def _parse_metadata_xml(self, view_name: str, xml_content: str) -> DatasphereViewSchema:
        """
        Parse OData $metadata XML to extract column information and view parameters.

        Columns come from the main EntityType. Parameters come from the EntityType
        referenced by NavigationProperty Name="Parameters" (e.g. ...Parameters type).
        """
        columns: List[DatasphereColumn] = []
        parameters: List[DatasphereParameter] = []

        try:
            root = ET.fromstring(xml_content)
            namespaces = {
                "edmx": "http://docs.oasis-open.org/odata/ns/edmx",
                "edm": "http://docs.oasis-open.org/odata/ns/edm",
            }

            # Build a map of EntityType Name -> element (for resolving Parameters type)
            entity_types_by_name: Dict[str, ET.Element] = {}
            for entity_type in root.iter():
                if not entity_type.tag.endswith("EntityType"):
                    continue
                name = entity_type.get("Name", "")
                if name:
                    entity_types_by_name[name] = entity_type

            # Find main entity (view data columns) — prefer one that does NOT end with "Parameters"
            main_entity: Optional[ET.Element] = None
            for entity_type in root.findall(".//edm:EntityType", namespaces):
                entity_name = entity_type.get("Name", "")
                if not entity_name:
                    continue
                if view_name in entity_name or entity_name in view_name:
                    if entity_name.endswith("Parameters"):
                        continue
                    main_entity = entity_type
                    break
            if main_entity is None:
                for entity_type in root.iter():
                    if not entity_type.tag.endswith("EntityType"):
                        continue
                    entity_name = entity_type.get("Name", "")
                    if not entity_name or entity_name.endswith("Parameters"):
                        continue
                    if view_name in entity_name or entity_name in view_name:
                        main_entity = entity_type
                        break

            if main_entity is not None:
                for prop in main_entity.findall("edm:Property", namespaces):
                    col = self._parse_property_element(prop)
                    if col:
                        columns.append(col)
                if not columns:
                    for prop in main_entity:
                        if prop.tag.endswith("Property"):
                            col = self._parse_property_element(prop)
                            if col:
                                columns.append(col)

                # Resolve Parameters NavigationProperty -> parameters EntityType
                parameters_type_name: Optional[str] = None
                for nav in main_entity.findall("edm:NavigationProperty", namespaces):
                    if nav.get("Name") == "Parameters":
                        parameters_type_name = nav.get("Type", "").strip()
                        break
                if not parameters_type_name:
                    for nav in main_entity:
                        if nav.tag.endswith("NavigationProperty") and nav.get("Name") == "Parameters":
                            parameters_type_name = nav.get("Type", "").strip()
                            break

                if parameters_type_name:
                    # Type can be "Namespace.EntityTypeName" or "Collection(...)"
                    if parameters_type_name.startswith("Collection("):
                        parameters_type_name = parameters_type_name[11:-1].strip()
                    if "." in parameters_type_name:
                        parameters_type_name = parameters_type_name.split(".")[-1]
                    params_entity = entity_types_by_name.get(parameters_type_name)
                    if params_entity is None:
                        for name, elem in entity_types_by_name.items():
                            if name.endswith("Parameters") and (
                                parameters_type_name in name or name in parameters_type_name
                            ):
                                params_entity = elem
                                break
                    if params_entity is not None:
                        for prop in params_entity.findall("edm:Property", namespaces):
                            p = self._parse_parameter_element(prop)
                            if p:
                                parameters.append(p)
                        if not parameters:
                            for prop in params_entity:
                                if prop.tag.endswith("Property"):
                                    p = self._parse_parameter_element(prop)
                                    if p:
                                        parameters.append(p)
                        logger.debug(f"SAP Schema: {view_name} -> {len(parameters)} parameters")
            else:
                # Fallback: any matching EntityType (original behavior)
                for entity_type in root.findall(".//edm:EntityType", namespaces):
                    entity_name = entity_type.get("Name", "")
                    if view_name in entity_name or entity_name in view_name:
                        for prop in entity_type.findall("edm:Property", namespaces):
                            col = self._parse_property_element(prop)
                            if col:
                                columns.append(col)
                        break
                if not columns:
                    for entity_type in root.iter():
                        if entity_type.tag.endswith("EntityType"):
                            for prop in entity_type:
                                if prop.tag.endswith("Property"):
                                    col = self._parse_property_element(prop)
                                    if col:
                                        columns.append(col)
                            if columns:
                                break

            logger.debug(f"SAP Schema: {view_name} -> {len(columns)} columns, has_parameters={len(parameters) > 0}")
        except ET.ParseError as e:
            logger.warning(f"SAP Schema: XML parse failed for {view_name}, trying JSON")
            try:
                data = json.loads(xml_content)
                columns = self._parse_metadata_json(data)
            except Exception:
                logger.error(f"SAP Schema: Failed to parse metadata for {view_name}")

        return DatasphereViewSchema(view_name=view_name, columns=columns, parameters=parameters)
    
    def _parse_property_element(self, prop: ET.Element) -> Optional[DatasphereColumn]:
        """
        Parse a Property XML element into a DatasphereColumn.
        
        Extracts only essential fields: name, type, max_length
        """
        name = prop.get('Name')
        if not name:
            return None
        
        # Get data type (keep as-is for OData compatibility)
        data_type = prop.get('Type', 'Edm.String')
        
        # Get max length if present
        max_length = prop.get('MaxLength')
        
        return DatasphereColumn(
            name=name,
            data_type=data_type,
            max_length=int(max_length) if max_length else None,
        )

    def _parse_parameter_element(self, prop: ET.Element) -> Optional[DatasphereParameter]:
        """
        Parse a Property element from a Parameters EntityType into a DatasphereParameter.
        Extracts name, type, max_length, nullable, default_value, precision, scale.
        """
        name = prop.get("Name")
        if not name:
            return None
        data_type = prop.get("Type", "Edm.String")
        max_length = prop.get("MaxLength")
        nullable = prop.get("Nullable")
        if nullable is not None:
            nullable = nullable.lower() in ("true", "1")
        default_value = prop.get("DefaultValue")
        precision = prop.get("Precision")
        scale = prop.get("Scale")
        return DatasphereParameter(
            name=name,
            data_type=data_type,
            max_length=int(max_length) if max_length else None,
            nullable=nullable,
            default_value=default_value,
            precision=int(precision) if precision else None,
            scale=int(scale) if scale else None,
        )

    def _parse_metadata_json(self, data: Dict[str, Any]) -> List[DatasphereColumn]:
        """Fallback: Parse metadata from JSON format."""
        columns = []
        
        if "value" in data:
            for item in data["value"]:
                if "Name" in item:
                    columns.append(DatasphereColumn(
                        name=item["Name"],
                        data_type=item.get("Type", "Edm.String"),
                        max_length=item.get("MaxLength"),
                    ))
        
        return columns
    
    async def get_multiple_view_schemas(
        self,
        user_id: str,
        view_names: List[str],
        assets: Optional[Dict[str, DatasphereAsset]] = None,
        max_concurrent: int = 10,
        state: Optional[Dict[str, Any]] = None
    ) -> Dict[str, DatasphereViewSchema]:
        """
        Get schemas for multiple views in parallel.
        
        Args:
            user_id: The user identifier
            view_names: List of view names to get schemas for
            assets: Optional dict of DatasphereAsset (to get metadata URLs)
            max_concurrent: Maximum number of concurrent schema requests
            state: Optional state dictionary to check for sap_access_token first
            
        Returns:
            Dict mapping view name to DatasphereViewSchema
        """
        import asyncio
        
        # Fetch token once and reuse for all parallel calls (checks state first)
        token = self._get_user_token(user_id, state=state)
        
        async def fetch_schema(view_name: str) -> tuple[str, DatasphereViewSchema]:
            """Fetch schema for a single view."""
            try:
                # Get metadata URL from asset (simplified structure: metadata_url field)
                metadata_url = None
                if assets and view_name in assets:
                    metadata_url = assets[view_name].metadata_url
                
                schema = await self.get_view_schema(
                    user_id=user_id,
                    view_name=view_name,
                    metadata_url=metadata_url,
                    token=token  # Reuse the same token
                )
                return (view_name, schema)
                
            except Exception as e:
                logger.error(f"Schema fetch failed for {view_name}: {e}")
                return (view_name, DatasphereViewSchema(view_name=view_name, columns=[]))
        
        # Create semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def fetch_with_semaphore(view_name: str):
            """Fetch with semaphore to limit concurrency."""
            async with semaphore:
                return await fetch_schema(view_name)
        
        # Execute all schema fetches in parallel
        logger.info(f"SAP Schema: Fetching {len(view_names)} views in parallel")
        tasks = [fetch_with_semaphore(view_name) for view_name in view_names]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Build schemas dict and count columns
        schemas = {}
        total_columns = 0
        failed = 0
        for result in results:
            if isinstance(result, Exception):
                logger.warning(f"SAP Schema: Parallel fetch exception: {result}")
                failed += 1
                continue
            view_name, schema = result
            schemas[view_name] = schema
            total_columns += len(schema.columns)
        
        logger.info(f"SAP Schema: {len(schemas)} views, {total_columns} total columns{f', {failed} failed' if failed else ''}")
        return schemas
    
    # =========================================================================
    # Data Query API - Execute OData Queries
    # =========================================================================
    
    async def execute_odata_query(
        self,
        user_id: str,
        view_name: str,
        select: Optional[str] = None,
        filter: Optional[str] = None,
        top: Optional[int] = None,
        skip: Optional[int] = None,
        orderby: Optional[str] = None,
        count: bool = False,
        data_url: Optional[str] = None,
        space_id: Optional[str] = None,
        token: Optional[str] = None,
        input_parameters: Optional[Dict[str, Any]] = None,
        apply: Optional[str] = None,  # $apply parameter for aggregation
        progress_callback: Optional[callable] = None,  # Callback for progress updates: (view_name, page_num, rows_fetched, total_rows)
        view_schema: Optional[DatasphereViewSchema] = None,  # Schema for date columns in orderby
        total_count: Optional[int] = None,  # Cached total count to avoid repeated $count calls
        view_type: Optional[str] = None,  # "analytical" | "relational" — determines endpoint prefix
    ) -> DatasphereQueryResult:
        """
        Execute an OData query against a SAP Datasphere view.
        
        **IMPORTANT**: This method enforces a maximum of 100,000 rows per page.
        If more rows are requested, pagination will automatically fetch additional pages.
        
        Args:
            user_id: The user identifier (for token retrieval)
            view_name: The view name
            select: $select parameter (comma-separated columns)
            filter: $filter parameter (OData filter expression)
            top: $top parameter (limit) - will be capped at 100,000 if larger
            skip: $skip parameter (offset)
            orderby: $orderby parameter
            count: Whether to include $count
            data_url: Direct data URL (from catalog asset)
            space_id: Optional space ID (used if data_url not provided)
            token: Optional pre-fetched token
            input_parameters: SAP Datasphere input parameters/variables (Dict)
                Example: {"Region": "US", "Category": "Apparel"}
                These are formatted as URL path: /ViewName(Region='US',Category='Apparel')/Set
            view_type: Optional view type ("analytical" or "relational") —
                when building URL from scratch (no data_url), selects the correct
                endpoint prefix (/consumption/analytical/ vs /consumption/relational/).
                If unset, defaults to "relational" for backward compatibility.
            
        Returns:
            DatasphereQueryResult with the query results (all pages combined)
        """
        # Use configurable row limit per page from settings
        MAX_ROWS_PER_PAGE = settings.sap_rows_per_page
        original_top = top
        
        # Special handling for count-only queries (top=0)
        is_count_only = (top == 0 and count)
        
        if is_count_only:
            # For count-only queries, preserve top=0 to get only count, no data
            logger.debug(f"[SAP OData] {view_name}: Count-only query (top=0, count=true) - will return only count")
        elif top is not None and top > MAX_ROWS_PER_PAGE:
            logger.info(
                f"[SAP OData] 📊 {view_name}: Requested top={top} exceeds page limit {MAX_ROWS_PER_PAGE}. "
                f"Using pagination with $skip."
            )
            top = MAX_ROWS_PER_PAGE
        elif top is None:
            # If no top specified, use configured default for consistent pagination
            top = MAX_ROWS_PER_PAGE
            logger.debug(f"[SAP OData] {view_name}: Using default page size {MAX_ROWS_PER_PAGE}")
        
        # Extract column count for logging
        column_count = len(select.split(',')) if select else 0
        select_columns = [c.strip() for c in select.split(',')] if select else []
        
        # Compact logging - avoid excessive detail
        select_preview = f"$select={column_count} cols" if select else "no $select"
        filter_preview = f"$filter={filter[:30]}..." if filter and len(filter) > 30 else (f"$filter={filter}" if filter else "no $filter")
        params_preview = f" | input_params={list(input_parameters.keys())}" if input_parameters else ""
        top_info = f" | $top={top}" + (f" (capped from {original_top})" if original_top and original_top > MAX_ROWS_PER_PAGE else "")
        
        # Create query identifier based on column count and first few column names
        query_id = f"{column_count}cols"
        if select_columns:
            query_id += f"_{select_columns[0][:10]}"
        
        # Log filter status - important for debugging date filter issues
        if filter:
            logger.info(f"[SAP OData] 🚀 [{query_id}] {view_name}: {select_preview} | {filter_preview}{top_info}{params_preview}")
            logger.debug(f"[SAP OData] 📋 [{query_id}] Full filter: {filter}")
        else:
            logger.info(f"[SAP OData] 🚀 [{query_id}] {view_name}: {select_preview} | {filter_preview}{top_info}{params_preview}")
            logger.debug(f"[SAP OData] ⚠️ [{query_id}] No filter provided - ensure LLM includes filters from SQL plan if needed")
        if select_columns:
            logger.debug(f"[SAP OData] 📋 [{query_id}] Columns: {', '.join(select_columns[:10])}{'...' if len(select_columns) > 10 else ''}")
        
        if not token:
            token = self._get_user_token(user_id)
        
        # Use provided data URL or build one
        if data_url:
            base = data_url.rstrip('/')
            # SAP consumption API with input params: /relational/<space>/<asset_id>/_<asset_id>(params)/Set (leading underscore on second segment)
            # Without params: /relational/<space>/<asset_id>/<asset_id>
            if input_parameters:
                # Only drop trailing /view_name when catalog URL has duplicate ( .../asset_id/asset_id ); keep single asset_id.
                suffix = '/' + view_name
                if base.endswith(suffix):
                    candidate = base[: -len(suffix)]
                    if candidate.endswith(suffix):
                        base = candidate  # had .../asset_id/asset_id → keep .../asset_id
                # Build input-parameter path: /_<view_name>(Param='Value')/Set
                input_params_path = self._format_input_parameters(view_name, input_parameters, underscore_prefix=True)
                url = base + input_params_path
            else:
                # No params: append view_name once (catalog may have one asset_id, we add the second)
                url = f"{base}/{view_name}"
            
            logger.info(f"[SAP OData] 📍 URL: {url}")
        else:
            space = space_id or self._space_id
            if not space:
                raise DatasphereError(
                    "Space ID is required. Provide space_id parameter or set SAP_DATASPHERE_SPACE_ID environment variable."
                )
            # Branch on view_type to select the correct endpoint prefix
            effective_view_type = (view_type or "relational").lower()
            endpoint_prefix = "analytical" if effective_view_type == "analytical" else "relational"
            # SAP consumption format: /<endpoint>/<space>/<asset_id>/<asset_id> or with params: /<asset_id>/_<asset_id>(params)/Set
            base_url = f"{self.API_PREFIX}/consumption/{endpoint_prefix}/{space}/{view_name}"
            if input_parameters:
                input_params_path = self._format_input_parameters(view_name, input_parameters, underscore_prefix=True)
                url = self._build_url(f"{base_url}{input_params_path}")
            else:
                url = self._build_url(f"{base_url}/{view_name}")
            logger.info(f"[SAP OData] 📍 Built URL ({endpoint_prefix}): {url}")
        
        # Calculate effective $top based on max pages to avoid SAP's MaxResultRecords limit
        # SAP analytical views (fallback) have a 1M row limit - we must stay under this
        # CRITICAL: Do NOT modify top, filter, or orderby here
        # All filters, orderby, and top are set manually in sap_odata_generation.py
        # We only use what's passed in the params - no modifications
        
        # Build OData params - keep it simple, pass what we have
        # SAP will return error if something isn't supported, we'll handle it
        odata_params = ODataParams(
            select=select,
            filter=filter,
            top=top,
            skip=skip,
            orderby=orderby,
            count=count,
            apply=apply  # Include $apply for aggregation if provided
        )
        
        return await self._execute_odata_request(
            user_id=user_id,
            token=token,
            url=url,
            odata_params=odata_params,
            view_name=view_name,
            space_id=space_id,
            input_parameters=input_parameters,
            view_schema=view_schema,  # Pass view_schema parameter directly
            progress_callback=progress_callback,
            total_count=total_count  # Pass cached total_count to skip $count call
        )
    
    async def execute_multiple_odata_queries(
        self,
        user_id: str,
        queries: List[Dict[str, Any]],
        assets: Optional[Dict[str, DatasphereAsset]] = None,
        max_concurrent: int = 10,
        token: Optional[str] = None,
    ) -> Dict[str, DatasphereQueryResult]:
        """
        Execute multiple OData queries in parallel.
        
        Args:
            user_id: The user identifier
            queries: List of query dicts, each containing:
                - view_name: Name of the view
                - select: Optional $select parameter
                - filter: Optional $filter parameter
                - top: Optional $top parameter
                - skip: Optional $skip parameter
                - orderby: Optional $orderby parameter
                - count: Optional count flag
                - data_url: Optional direct data URL
                - space_id: Optional space ID
                - input_parameters: Optional SAP input parameters dict
            assets: Optional dict of DatasphereAsset (to get data URLs)
            max_concurrent: Maximum number of concurrent queries (default 10)
            token: Optional pre-fetched token (will fetch if not provided)
            
        Returns:
            Dict mapping view_name to DatasphereQueryResult
        """
        import asyncio
        
        if not token:
            token = self._get_user_token(user_id)
        
        async def execute_single_query(query: Dict[str, Any]) -> tuple[str, DatasphereQueryResult]:
            """Execute a single OData query."""
            view_name = query.get("view_name")
            if not view_name:
                raise ValueError("Query missing 'view_name'")
            
            # Get data URL from assets if available
            data_url = query.get("data_url")
            if not data_url and assets and view_name in assets:
                data_url = assets[view_name].data_url
            
            try:
                result = await self.execute_odata_query(
                    user_id=user_id,
                    view_name=view_name,
                    select=query.get("select"),
                    filter=query.get("filter"),
                    top=query.get("top"),
                    skip=query.get("skip"),
                    orderby=query.get("orderby"),
                    count=query.get("count", False),
                    data_url=data_url,
                    space_id=query.get("space_id"),
                    token=token,
                    input_parameters=query.get("input_parameters"),
                )
                return (view_name, result)
            except Exception as e:
                logger.error(f"Failed to execute query for view '{view_name}': {e}")
                raise
        
        # Create semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def execute_with_semaphore(query: Dict[str, Any]):
            """Execute query with semaphore to limit concurrency."""
            async with semaphore:
                return await execute_single_query(query)
        
        # Execute all queries in parallel
        logger.info(f"Executing {len(queries)} OData queries in parallel (max {max_concurrent} concurrent)")
        tasks = [execute_with_semaphore(query) for query in queries]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Build results dict
        query_results = {}
        failed = 0
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Query execution exception: {result}", exc_info=True)
                failed += 1
                continue
            view_name, query_result = result
            query_results[view_name] = query_result
        
        logger.info(f"Completed {len(query_results)} queries successfully{f', {failed} failed' if failed else ''}")
        return query_results
    
    def _filter_odata_params(self, params: Dict[str, Any], url: str) -> Dict[str, Any]:
        """
        Pass through OData query parameters.
        
        Keep it simple - let SAP tell us what's supported via error responses.
        The error correction logic will handle unsupported params.
        
        Args:
            params: Dictionary of OData query parameters
            url: The API URL (not used, kept for API compatibility)
            
        Returns:
            Same params dictionary (pass-through)
        """
        return params
    
    async def _execute_odata_request(
        self,
        user_id: str,
        token: str,
        url: str,
        odata_params: ODataParams,
        view_name: str,
        space_id: Optional[str] = None,
        input_parameters: Optional[Dict[str, Any]] = None,
        progress_callback: Optional[callable] = None,  # Callback for progress updates
        view_schema: Optional[DatasphereViewSchema] = None,  # Schema for date columns in orderby
        total_count: Optional[int] = None  # Cached total count to avoid repeated $count calls
    ) -> DatasphereQueryResult:
        """
        Execute an OData request against the Datasphere API.
        
        This method automatically filters query parameters based on endpoint type:
        - Analytical endpoints: Only $format is allowed
        - Relational endpoints: Full OData query options supported
        
        **IMPORTANT**: This method enforces pagination with a maximum of 50,000 rows per page.
        It uses $skip parameter to fetch subsequent pages (not @odata.nextLink).
        
        Pagination pattern:
        - Page 1: $top=50000, $skip=0
        - Page 2: $top=50000, $skip=50000
        - Page 3: $top=50000, $skip=100000
        - Continues until no data is returned
        
        Args:
            user_id: The user identifier
            token: The access token to use
            url: The API URL (may include input parameters suffix)
            odata_params: OData query parameters (top should already be capped at 50,000)
            view_name: The view name
            space_id: Optional space ID
            input_parameters: SAP Datasphere input parameters/variables for URL path
            
        Returns:
            DatasphereQueryResult with the query results (all pages combined)
        """
        # Use configurable row limit per page from settings
        # Per SAP documentation:
        # - analytical: 50KB default (up to 50k records), 100KB max (up to 100k records)
        # - relational: 50KB default (up to 50k records), 100KB max (up to 100k records)
        # - catalog: 500 records max
        MAX_ROWS_PER_PAGE = settings.sap_rows_per_page
        MAX_ROWS_PER_PAGE_ABSOLUTE = 100000  # Absolute max for analytical/relational endpoints
        
        # Detect endpoint type from URL
        endpoint_type = "analytical"  # Default
        if "/relational/" in url:
            endpoint_type = "relational"
        elif "/catalog/" in url:
            endpoint_type = "catalog"
            MAX_ROWS_PER_PAGE = 500  # Catalog has 500 record limit
            MAX_ROWS_PER_PAGE_ABSOLUTE = 500
        # Convert to dict - no manual filtering, let LLM handle corrections
        params = odata_params.to_dict()
        
        # Analytical views do not support $count (Capabilities.CountRestrictions.Countable: false)
        if endpoint_type == "analytical" and params.get("$count"):
            params.pop("$count", None)
            logger.debug(f"[SAP OData] {view_name}: Analytical endpoint — removed $count (not supported)")
        
        # Relational: keep $select when provided (plan columns) so we request all select cols in each call.
        # Count call uses count_params with $select removed separately below.
        if endpoint_type == "relational" and params.get("$select"):
            logger.info(f"[SAP OData] [{view_name}] Relational view - using $select (all plan columns in single call)")
        
        # Create query identifier for logging (based on select columns)
        select_param = params.get("$select", "")
        if select_param:
            select_columns = [c.strip() for c in select_param.split(',')]
            column_count = len(select_columns)
            query_id = f"{column_count}cols"
            if select_columns:
                query_id += f"_{select_columns[0][:10]}"
        else:
            query_id = "allcols"
        
        # Ensure token is valid
        if not token:
            raise DatasphereError("Access token is required but not provided")
        token = token.strip()
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        
        # Build and log full URL (only log for first request, not pagination)
        # URL logging is handled in execute_odata_query for better context
        from urllib.parse import urlencode
        # urlencode automatically converts spaces to + and encodes special characters
        # This ensures filters like "Calendar_Day ge 2023-01-01 and Calendar_Day le 2023-01-31"
        # become "Calendar_Day+ge+2023-01-01+and+Calendar_Day+le+2023-01-31" in the URL
        query_string = urlencode(params, doseq=False) if params else ""
        full_url = f"{url}?{query_string}" if query_string else url
        
        # Store the full URL for returning in the result
        stored_api_url = full_url
        
        # Log filter presence in URL for verification
        has_filter = "$filter" in params or "$filter" in query_string
        if has_filter:
            filter_value = params.get("$filter", "")
            logger.info(f"[SAP OData] ✅ [{query_id}] Filter included in URL: {filter_value[:100]}{'...' if len(filter_value) > 100 else ''}")
        else:
            logger.warning(f"[SAP OData] ⚠️ [{query_id}] No $filter parameter in URL - check if filters should be included")
        
        # Log $apply parameter if present
        apply_value = params.get("$apply", "")
        if apply_value:
            logger.info(f"[SAP OData] 🔧 [{query_id}] $apply parameter: {apply_value[:200]}{'...' if len(apply_value) > 200 else ''}")
        else:
            logger.debug(f"[SAP OData] [{query_id}] No $apply parameter")
        
        # Log full URL with all parameters (select, filter, top, skip, orderby, etc.) - DEBUG level
        logger.debug(f"[SAP OData] 🌐 [{query_id}] Full Request URL: {full_url}")
        logger.debug(f"[SAP OData] 📝 [{query_id}] All OData params: {json.dumps(params, indent=2)}")
        
        # CRITICAL: Do NOT add external filters or orderby here
        # All filters, orderby, and top are set manually in sap_odata_generation.py
        # We only use what's passed in the params - no modifications
        
        # Acquire rate limit before API call
        await self._rate_limiter.acquire()
        
        # Log API call details before making the call
        select_cols = params.get("$select", "")
        filter_val = params.get("$filter", "")
        top_val = params.get("$top", "")
        skip_val = params.get("$skip", "")
        orderby_val = params.get("$orderby", "")
        col_count = len(select_cols.split(",")) if select_cols else 0
        
        logger.info(f"[SAP OData] 📞 [{query_id}] Making API call for '{view_name}'")
        logger.info(
            f"[SAP OData] 📋 [{query_id}] Request details: "
            f"columns={col_count if select_cols else 'all'}, "
            f"filter={'yes' if filter_val else 'no'}, "
            f"top={top_val}, "
            f"skip={skip_val}, "
            f"orderby={'yes' if orderby_val else 'no'}"
        )
        logger.info(f"[SAP OData] 🔗 [{query_id}] API URL: {full_url}")
        
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                # For relational views: Make $count call FIRST before any data fetching
                # OPTIMIZATION: Only make $count call if total_count is not already provided (cached)
                if endpoint_type == "relational" and total_count is None:
                    # Make $count call first
                    logger.info(f"[SAP OData] 📊 [{query_id}] {view_name}: Making $count call FIRST to get total rows...")
                    count_params = params.copy()
                    count_params["$count"] = "true"
                    count_params["$top"] = 0  # No data, just count
                    # Count call: no $select, no $orderby (orderby used only in page-wise data calls)
                    count_params.pop("$select", None)
                    count_params.pop("$orderby", None)
                    
                    # Acquire rate limit before count API call
                    await self._rate_limiter.acquire()
                    # Build count URL for debug logging
                    from urllib.parse import urlencode
                    count_query_string = urlencode(count_params) if count_params else ""
                    count_url_full = f"{url}?{count_query_string}" if count_query_string else url
                    logger.info(f"[SAP OData] 🔗 [{query_id}] API URL ($count): {count_url_full}")
                    count_response = await client.get(url, params=count_params, headers=headers)
                    
                    if count_response.status_code == 200:
                        count_data = count_response.json()
                        total_count = count_data.get("@odata.count")
                        if total_count is None:
                            # Try alternative location
                            total_count = count_data.get("value", [{}])[0].get("@odata.count") if count_data.get("value") else None
                        
                        if total_count is not None:
                            logger.info(f"[SAP OData] ✅ [{query_id}] {view_name}: Total count: {total_count:,} rows")
                        else:
                            logger.warning(f"[SAP OData] ⚠️ [{query_id}] {view_name}: Could not extract count from response")
                    else:
                        logger.warning(f"[SAP OData] ⚠️ [{query_id}] {view_name}: Count call failed (status {count_response.status_code})")
                elif endpoint_type == "relational" and total_count is not None:
                    # Using cached count - skip $count call
                    logger.info(f"[SAP OData] ✅ [{query_id}] {view_name}: Using cached total count: {total_count:,} rows (skipping $count call)")
                
                # Now make the first data call
                # Build data URL for debug logging (already built above, but log it here too for clarity)
                # The full_url was already logged at DEBUG level above, so this is redundant but kept for consistency
                response = await client.get(url, params=params, headers=headers)
                
                # Log response details
                if response.status_code == 200:
                    logger.info(f"[SAP OData] ✅ [{query_id}] API call successful: Status {response.status_code} for '{view_name}'")
                else:
                    logger.error(f"[SAP OData] ❌ [{query_id}] API call failed: Status {response.status_code} for '{view_name}'")
                    # Log full error response for non-200 status codes
                    try:
                        error_text = response.text
                        logger.error(f"[SAP OData] ❌ Full error response for '{view_name}':")
                        logger.error(f"[SAP OData] Error response body: {error_text}")
                        # Try to parse as JSON for better formatting
                        try:
                            error_json = response.json()
                            logger.error(f"[SAP OData] Error response (JSON): {json.dumps(error_json, indent=2)}")
                        except:
                            pass  # Not JSON, already logged as text
                    except Exception as log_error:
                        logger.error(f"[SAP OData] Failed to log error response: {log_error}")
                
                if response.status_code == 200:
                    # Check if this is a count-only query (top=0) before parsing
                    top_param = params.get("$top", "")
                    is_count_only = (top_param == 0 or top_param == "0") and params.get("$count") == "true"
                    
                    # Parse first page
                    response_data = response.json()
                    first_page_result = self._parse_odata_response(response_data)
                    
                    if is_count_only:
                        # For count-only queries, return immediately with just the count
                        # No data rows should be fetched, no pagination needed
                        count_value = first_page_result.count if first_page_result.count is not None else 0
                        logger.info(
                            f"[SAP OData] ✅ [{query_id}] {view_name}: Count-only query complete - "
                            f"count={count_value:,} rows (no data fetched, no pagination)"
                        )
                        return DatasphereQueryResult(
                            data=[],  # No data for count-only queries
                            count=count_value,
                            next_link=None,
                            metadata=first_page_result.metadata,
                            api_url=stored_api_url
                        )
                    
                    # NEW APPROACH: Count-first approach ONLY for relational views
                    # For relational views:
                    # 1. Make a $count call first to get total rows
                    # 2. Use that count to determine how many rows to fetch
                    # 3. Fetch all rows using $top (or split into parallel chunks if count > max per page)
                    # 4. No sequential pagination loop - all data fetched in parallel chunks
                    # For analytical views: Use sequential pagination (old approach)
                    
                    # Initialize data collection
                    all_data = first_page_result.data.copy()
                    total_rows = len(all_data)
                    
                    # Log column count and sample data for page 1
                    column_count = len(all_data[0].keys()) if all_data else 0
                    column_names = list(all_data[0].keys()) if all_data else []
                    logger.info(
                        f"[SAP OData] ✅ [{query_id}] {view_name}: Page 1 API call complete - "
                        f"{total_rows:,} rows, {column_count} columns "
                        f"(limit: {MAX_ROWS_PER_PAGE:,} rows/page)"
                    )
                    if column_count > 0:
                        logger.info(f"[SAP OData] 📊 [{query_id}] Page 1 columns: {', '.join(column_names[:10])}{'...' if column_count > 10 else ''}")
                    
                    # Send progress update for page 1
                    if progress_callback:
                        try:
                            await progress_callback(view_name, 1, total_rows, total_rows)
                        except Exception as e:
                            logger.debug(f"[SAP OData] Progress callback error: {e}")
                    
                    # Log sample data for page 1
                    if all_data:
                        sample_rows = all_data[:3]  # First 3 rows
                        logger.info(f"[SAP OData] 📝 [{query_id}] Page 1 sample data (first {min(3, len(sample_rows))} row(s)):")
                        for idx, row in enumerate(sample_rows, 1):
                            sample_data = {}
                            row_items = list(row.items())[:10]  # First 10 columns
                            for k, v in row_items:
                                val_str = str(v) if v is not None else "null"
                                sample_data[k] = val_str[:100] + ('...' if len(val_str) > 100 else '')
                            logger.info(f"[SAP OData]    Row {idx}: {sample_data}")
                            if column_count > 10:
                                logger.info(f"[SAP OData]    ... ({column_count - 10} more columns)")
                                break  # Only show first row if many columns
                    
                    # No page limit - all pagination logic is handled by sap_data_fetch_simple.py
                    # This service only fetches a single page per call
                    
                    # Check if we need pagination
                    needs_pagination = len(first_page_result.data) >= MAX_ROWS_PER_PAGE
                    
                    # Note: All pagination logic is handled by sap_data_fetch_simple.py
                    # This service only fetches a single page per call
                    
                    # COUNT-FIRST APPROACH: Only for relational views
                    # SIMPLIFIED: Just return the single page result
                    # All pagination logic is handled by sap_data_fetch_simple.py
                    # This service just fetches one page at a time based on top/skip parameters
                    all_data = first_page_result.data.copy()
                    total_rows = len(all_data)
                    
                    logger.info(
                        f"[SAP OData] ✅ [{query_id}] {view_name}: Fetched {total_rows:,} rows "
                        f"(top={params.get('$top', 'not set')}, skip={params.get('$skip', 0)})"
                    )
                    
                    # Send progress update
                    if progress_callback:
                        try:
                            await progress_callback(view_name, 1, total_rows, total_rows)
                        except Exception as e:
                            logger.debug(f"[SAP OData] Progress callback error: {e}")
                    
                    # Note: Pagination is handled by the caller (sap_data_fetch_simple.py)
                    # This service only fetches a single page per call
                    # All pagination logic (parallel fetching, retry, etc.) is in sap_data_fetch_simple.py
                    
                    # Log final column count and sample data (for both relational and analytical)
                    column_count = len(all_data[0].keys()) if all_data else 0
                    column_names = list(all_data[0].keys()) if all_data else []
                    logger.info(
                        f"[SAP OData] ✅ [{query_id}] {view_name}: Fetch complete - "
                        f"{total_rows:,} rows, {column_count} columns"
                    )
                    if column_count > 0:
                        logger.info(f"[SAP OData] 📊 [{query_id}] Columns: {', '.join(column_names[:10])}{'...' if column_count > 10 else ''}")
                    
                    # Log sample data
                    if all_data:
                        sample_rows = all_data[:3]  # First 3 rows
                        logger.info(f"[SAP OData] 📝 [{query_id}] Sample data (first {min(3, len(sample_rows))} row(s)):")
                        for idx, row in enumerate(sample_rows, 1):
                            sample_data = {}
                            row_items = list(row.items())[:10]  # First 10 columns
                            for k, v in row_items:
                                val_str = str(v) if v is not None else "null"
                                sample_data[k] = val_str[:100] + ('...' if len(val_str) > 100 else '')
                            logger.info(f"[SAP OData]    Row {idx}: {sample_data}")
                            if column_count > 10:
                                logger.info(f"[SAP OData]    ... ({column_count - 10} more columns)")
                                break  # Only show first row if many columns
                    
                    # Pagination complete - all data collected
                    
                    # Calculate total API calls made
                    # This service only makes 1 API call per execute_odata_query call
                    # Pagination is handled by sap_data_fetch_simple.py which makes multiple calls
                    total_api_calls_for_query = 1  # Single page fetch
                    final_page_num = 1
                    
                    # Return combined result
                    final_column_count = len(all_data[0].keys()) if all_data else 0
                    logger.info(
                        f"[SAP OData] ✅ [{query_id}] {view_name}: COMPLETE - "
                        f"{total_rows:,} rows, {final_column_count} cols, "
                        f"{final_page_num} page(s), {total_api_calls_for_query} API call(s)"
                    )
                    
                    
                    return DatasphereQueryResult(
                        data=all_data,
                        count=first_page_result.count,
                        next_link=None,  # Clear next_link since we've fetched all pages using $skip
                        metadata=first_page_result.metadata,
                        api_url=stored_api_url
                    )
                
                elif response.status_code == 401:
                    logger.error(f"Token expired for user {user_id}")
                    raise TokenExpiredError(
                        f"SAP Datasphere access token has expired. Token should be refreshed before starting queries."
                    )
                
                elif response.status_code == 404:
                    # Try analytical endpoint if relational failed (no manual filtering)
                    if "relational" in url:
                        space = space_id or self._space_id
                        if not space:
                            raise DatasphereError("Space ID is required for analytical data endpoint")
                        # SAP format: /analytical/<space_id>/<asset_id>/<asset_id>[(<params>)/Set]
                        base_analytical_url = f"{self.API_PREFIX}/consumption/analytical/{space}/{view_name}"
                        # Append duplicate asset_id and input parameters if provided
                        if input_parameters:
                            # Analytical endpoint uses same pattern: /<space>/<asset_id>/_<asset_id>(params)/Set
                            input_params_path = self._format_input_parameters(view_name, input_parameters, underscore_prefix=True)
                            analytical_url = self._build_url(f"{base_analytical_url}{input_params_path}")
                        else:
                            analytical_url = self._build_url(f"{base_analytical_url}/{view_name}")
                        logger.info(f"[SAP OData] Trying analytical endpoint for '{view_name}' (relational returned 404)")
                        logger.info(f"[SAP OData] 📍 Analytical URL: {analytical_url}")
                        analytical_query = urlencode(params, doseq=False) if params else ""
                        analytical_full_url = f"{analytical_url}?{analytical_query}" if analytical_query else analytical_url
                        logger.info(f"[SAP OData] 🔗 [{query_id}] API URL (analytical fallback): {analytical_full_url}")
                        # Acquire rate limit before retry API call
                        await self._rate_limiter.acquire()
                        # Use original params without filtering - let LLM handle corrections if needed
                        response = await client.get(analytical_url, params=params, headers=headers)
                        if response.status_code == 200:
                            logger.info(f"[SAP OData] ✅ Analytical endpoint succeeded for '{view_name}'")
                            return self._parse_odata_response(response.json())
                    
                    logger.error(f"[SAP OData] ❌ View '{view_name}' not found (404)")
                    raise DatasphereAPIError(
                        f"View '{view_name}' not found.",
                        status_code=404,
                        response_body=response.text
                    )
                
                elif response.status_code == 400:
                    # Parse error message
                    response_text = response.text
                    error_data = {}
                    error_message = ""
                    try:
                        error_data = response.json()
                        # Try different error message locations
                        if isinstance(error_data.get("error"), dict):
                            error_message = error_data["error"].get("message", "")
                        elif isinstance(error_data.get("details"), dict):
                            error_message = error_data["details"].get("message", "")
                        elif "message" in error_data:
                            error_message = error_data["message"]
                    except:
                        error_message = response_text[:300]
                    
                    logger.error(f"[SAP OData] ❌ {view_name}: 400 Bad Request")
                    logger.error(f"[SAP OData] Error: {error_message}")
                    logger.error(f"[SAP OData] Full response: {response_text[:500]}")
                    
                    # Check for MaxResultRecords error - SAP's 1M row limit
                    if "#42709" in error_message or "MaxResultRecords" in error_message:
                        logger.error(
                            f"[SAP OData] ⚠️ {view_name}: SAP MaxResultRecords limit exceeded! "
                            f"This view has too many rows. You MUST use a $filter (e.g., date filter) "
                            f"to reduce the result set below 1M rows."
                        )
                        raise DatasphereAPIError(
                            f"View '{view_name}' exceeds SAP's 1M row limit. "
                            f"Please add a date filter or other filter to reduce the data volume. "
                            f"Example: Add a date range filter like 'DateCol ge date'2025-01-01' and DateCol lt date'2025-02-01''. "
                            f"Original error: {error_message[:200]}",
                            status_code=400,
                            response_body=response_text
                        )
                    
                    raise DatasphereAPIError(
                        f"OData query error for view '{view_name}': {error_message or 'Bad Request'}",
                        status_code=400,
                        response_body=response_text
                    )
                
                elif response.status_code == 403:
                    # Error response already logged above
                    raise DatasphereAPIError(
                        f"Access forbidden to view '{view_name}'.",
                        status_code=403,
                        response_body=response.text
                    )
                
                else:
                    # Error response already logged above
                    raise DatasphereAPIError(
                        f"Datasphere API error: {response.status_code}",
                        status_code=response.status_code,
                        response_body=response.text
                    )
                    
        except httpx.TimeoutException:
            logger.error(f"SAP API: Timeout for {view_name}")
            raise DatasphereAPIError(
                "Request to SAP Datasphere timed out.",
                status_code=504
            )
        except httpx.RequestError as e:
            logger.error(f"SAP API: Request error for {view_name}: {e}")
            raise DatasphereAPIError(
                f"Failed to connect to SAP Datasphere: {str(e)}",
                status_code=503
            )
    
    def _parse_odata_response(self, response_data: Dict[str, Any]) -> DatasphereQueryResult:
        """Parse OData response into DatasphereQueryResult."""
        data = response_data.get("value", [])
        count = response_data.get("@odata.count")
        next_link = response_data.get("@odata.nextLink")
        
        metadata = {}
        if "@odata.context" in response_data:
            metadata["context"] = response_data["@odata.context"]
        
        result = DatasphereQueryResult(
            data=data,
            count=count,
            next_link=next_link,
            metadata=metadata if metadata else None,
            api_url=None  # API URL not available in _parse_odata_response
        )
        
        logger.info(f"Parsed Datasphere response: {len(data)} rows")
        return result
    
    # =========================================================================
    # SQL Query Support (converts SQL to OData)
    # =========================================================================
    
    async def execute_sql_query(
        self,
        user_id: str,
        sql_query: str,
        space_id: Optional[str] = None,
        include_count: bool = False
    ) -> DatasphereQueryResult:
        """
        Execute a SQL query against SAP Datasphere.
        
        The SQL query is converted to OData parameters.
        
        Args:
            user_id: The user identifier
            sql_query: SQL SELECT query
            space_id: Optional Datasphere space ID
            include_count: Whether to include total count
            
        Returns:
            DatasphereQueryResult with the query results
        """
        logger.info(f"Executing SQL query for user {user_id}: {sql_query[:100]}...")
        
        # Convert SQL to OData parameters
        try:
            view_name, odata_params = self._odata_converter.convert(sql_query)
        except ValueError as e:
            logger.error(f"Failed to convert SQL to OData: {e}")
            raise DatasphereError(f"Invalid SQL query: {e}")
        
        if include_count:
            odata_params.count = True
        
        return await self.execute_odata_query(
            user_id=user_id,
            view_name=view_name,
            select=odata_params.select,
            filter=odata_params.filter,
            top=odata_params.top,
            skip=odata_params.skip,
            orderby=odata_params.orderby,
            count=odata_params.count,
            space_id=space_id
        )
    
    # =========================================================================
    # Legacy Methods (for backward compatibility)
    # =========================================================================
    
    async def list_entities(
        self,
        user_id: str,
        space_id: Optional[str] = None
    ) -> List[str]:
        """
        List available entities (views) - legacy method.
        
        Use list_catalog_assets() for full asset information.
        """
        result = await self.list_catalog_assets(user_id)
        return result.view_names
    
    async def get_entity_metadata(
        self,
        user_id: str,
        entity_set: str,
        space_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get metadata for an entity - legacy method.
        
        Use get_view_schema() for structured column information.
        """
        schema = await self.get_view_schema(user_id, entity_set, space_id=space_id)
        return schema.to_dict()


# Singleton instance
_datasphere_service: Optional[DatasphereService] = None


def get_datasphere_service() -> DatasphereService:
    """Get the singleton DatasphereService instance."""
    global _datasphere_service
    if _datasphere_service is None:
        _datasphere_service = DatasphereService()
    return _datasphere_service
