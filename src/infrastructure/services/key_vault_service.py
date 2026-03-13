"""Service for retrieving secrets from Azure Key Vault."""
import logging
from typing import Optional, TYPE_CHECKING, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)

# Make Azure imports optional - handle gracefully if libraries are not installed
try:
    from azure.identity import DefaultAzureCredential
    from azure.keyvault.secrets import SecretClient
    AZURE_AVAILABLE = True
except ImportError as e:
    AZURE_AVAILABLE = False
    DefaultAzureCredential = None
    SecretClient = None
    logger.warning(f"Azure Key Vault libraries not available: {e}. KeyVaultService will not be functional.")
    logger.warning("Install with: pip install azure-identity azure-keyvault-secrets")

# Import settings (needed regardless of Azure availability)
try:
    from config.settings import settings
except ImportError:
    # Fallback for when running directly
    try:
        from config.settings import settings
    except ImportError:
        settings = None
        logger.warning("Settings module not available")


class KeyVaultService:
    """
    Service for retrieving secrets from Azure Key Vault.
    
    This service provides methods to retrieve user-specific tokens and other
    secrets stored in Azure Key Vault.
    """
    
    def __init__(self):
        """Initialize the Key Vault client."""
        if not AZURE_AVAILABLE:
            logger.warning("Azure Key Vault libraries not installed. KeyVaultService will not be functional.")
            logger.warning("Install with: pip install azure-identity azure-keyvault-secrets")
            self._client = None
            self._vault_url = None
            return
        
        self._client: Optional[SecretClient] = None
        self._vault_url = settings.azure_key_vault_url if settings else None
        
        if not self._vault_url:
            logger.warning("Azure Key Vault URL not configured. KeyVaultService will not be functional.")
    
    def _get_client(self) -> SecretClient:
        """
        Get or create the Key Vault client.
        
        Uses DefaultAzureCredential which supports multiple authentication methods:
        - Environment variables (AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID)
        - Managed Identity (when running in Azure)
        - Azure CLI credentials (for local development)
        
        Returns:
            SecretClient instance
            
        Raises:
            ValueError: If Key Vault URL is not configured or Azure libraries are not available
        """
        if not AZURE_AVAILABLE:
            raise ValueError(
                "Azure Key Vault libraries are not installed. "
                "Install with: pip install azure-identity azure-keyvault-secrets"
            )
        
        if not self._vault_url:
            raise ValueError("Azure Key Vault URL is not configured. Set AZURE_KEY_VAULT_URL environment variable.")
        
        if self._client is None:
            logger.debug(f"Initializing Azure Key Vault client for vault: {self._vault_url}")
            try:
                # Suppress Azure SDK verbose logging during credential initialization
                credential = DefaultAzureCredential()
                self._client = SecretClient(vault_url=self._vault_url, credential=credential)
                logger.debug("Azure Key Vault client initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize Azure Key Vault client: {e}")
                logger.error("Check Azure authentication: Managed Identity, Service Principal, or Azure CLI")
                raise
        
        return self._client
    
    def get_user_token(self, user_id: str) -> Optional[str]:
        """
        Retrieve SAP Datasphere OAuth access token for a specific user.
        
        The token is stored in Key Vault with a naming convention:
        sap-datasphere-token-{user_id}
        
        Args:
            user_id: The unique identifier of the user
            
        Returns:
            The access token string if found, None otherwise
            
        Raises:
            ValueError: If Key Vault URL is not configured
            Exception: If token retrieval fails
        """
        token_with_metadata = self.get_user_token_with_metadata(user_id)
        return token_with_metadata[0] if token_with_metadata else None
    
    def get_user_token_with_metadata(self, user_id: str) -> Optional[Tuple[str, Optional[datetime]]]:
        """
        Retrieve SAP Datasphere OAuth access token with creation date metadata.
        
        The token is stored in Key Vault with a naming convention:
        sap-datasphere-token-{user_id}
        
        Args:
            user_id: The unique identifier of the user
            
        Returns:
            Tuple of (token_string, created_on_datetime) if found, None otherwise.
            created_on_datetime will be None if creation date is not available.
            
        Raises:
            ValueError: If Key Vault URL is not configured
            Exception: If token retrieval fails
        """
        secret_name = f"sap-datasphere-token-{user_id}"
        
        try:
            logger.debug(f"Retrieving SAP Datasphere token with metadata for user: {user_id}")
            client = self._get_client()
            secret = client.get_secret(secret_name)
            
            if secret and secret.value:
                # Get creation date from secret properties
                created_on = None
                if hasattr(secret, 'properties') and secret.properties:
                    created_on = secret.properties.created_on
                elif hasattr(secret, 'created_on'):
                    created_on = secret.created_on
                
                if created_on:
                    logger.debug(f"Token creation date for user {user_id}: {created_on}")
                else:
                    logger.debug(f"Token creation date not available for user {user_id}")
                
                logger.debug(f"Successfully retrieved token for user: {user_id}")
                return (secret.value, created_on)
            else:
                logger.warning(f"Token not found or empty for user: {user_id}")
                return None
                
        except Exception as e:
            error_msg = str(e)
            # Check for common Azure Key Vault errors
            if "SecretNotFound" in error_msg or "NotFound" in error_msg:
                logger.warning(f"Token not found in Key Vault for user: {user_id}")
                return None
            elif "Forbidden" in error_msg or "Unauthorized" in error_msg or "401" in error_msg:
                logger.error(f"❌ Key Vault authentication failed for user: {user_id}")
                logger.error("   Check: Managed Identity permissions, Service Principal credentials, or Azure CLI login")
                raise
            else:
                logger.error(f"Failed to retrieve token for user {user_id}: {error_msg[:200]}")
                raise
    
    def get_user_refresh_token(self, user_id: str) -> Optional[str]:
        """
        Retrieve SAP Datasphere OAuth refresh token for a specific user.
        
        The refresh token is stored in Key Vault with a naming convention:
        sap-datasphere-refresh-token-{user_id}
        
        Args:
            user_id: The unique identifier of the user
            
        Returns:
            The refresh token string if found, None otherwise
        """
        secret_name = f"sap-datasphere-refresh-token-{user_id}"
        return self.get_secret(secret_name)
    
    def update_user_token(self, user_id: str, access_token: str, refresh_token: Optional[str] = None) -> None:
        """
        Update SAP Datasphere access token (and optionally refresh token) in Key Vault.
        
        This method updates the tokens in Azure Key Vault using the same secret names
        as used for retrieval, ensuring consistency.
        
        Args:
            user_id: The unique identifier of the user
            access_token: The new access token to store
            refresh_token: Optional new refresh token to store (will be updated if provided)
            
        Raises:
            ValueError: If Key Vault URL is not configured
            Exception: If token update fails
        """
        if not access_token:
            raise ValueError("Access token cannot be empty")
        
        try:
            client = self._get_client()
            
            # Update access token (always update this)
            access_token_name = f"sap-datasphere-token-{user_id}"
            logger.info(f"🔄 Updating access token in Key Vault for user: {user_id} (secret: {access_token_name})")
            client.set_secret(access_token_name, access_token)
            logger.info(f"✅ Successfully updated access token in Key Vault for user: {user_id}")
            
            # Update refresh token if provided
            if refresh_token:
                refresh_token_name = f"sap-datasphere-refresh-token-{user_id}"
                logger.info(f"🔄 Updating refresh token in Key Vault for user: {user_id} (secret: {refresh_token_name})")
                client.set_secret(refresh_token_name, refresh_token)
                logger.info(f"✅ Successfully updated refresh token in Key Vault for user: {user_id}")
            else:
                logger.debug(f"ℹ️ No refresh token provided, skipping refresh token update for user: {user_id}")
                
        except Exception as e:
            error_msg = f"Failed to update token for user {user_id}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise Exception(error_msg) from e
    
    def get_secret(self, secret_name: str) -> Optional[str]:
        """
        Retrieve any secret from Azure Key Vault by name.
        
        Args:
            secret_name: The name of the secret in Key Vault
            
        Returns:
            The secret value if found, None otherwise
            
        Raises:
            ValueError: If Key Vault URL is not configured
            Exception: If secret retrieval fails
        """
        try:
            logger.info(f"Retrieving secret: {secret_name}")
            client = self._get_client()
            secret = client.get_secret(secret_name)
            
            if secret and secret.value:
                logger.info(f"Successfully retrieved secret: {secret_name}")
                return secret.value
            else:
                logger.warning(f"Secret not found or empty: {secret_name}")
                return None
                
        except Exception as e:
            error_msg = str(e)
            if "SecretNotFound" in error_msg or "NotFound" in error_msg:
                logger.warning(f"Secret not found in Key Vault: {secret_name}")
                return None
            else:
                logger.error(f"Failed to retrieve secret {secret_name}: {error_msg}")
                raise


# Singleton instance
_key_vault_service: Optional[KeyVaultService] = None


def get_key_vault_service() -> KeyVaultService:
    """
    Get the singleton KeyVaultService instance.
    
    Returns:
        KeyVaultService instance
    """
    global _key_vault_service
    if _key_vault_service is None:
        _key_vault_service = KeyVaultService()
    return _key_vault_service

