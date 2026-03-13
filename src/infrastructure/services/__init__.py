"""Services for data processing and analysis."""

from .data_source_analysis_service import DataSourceAnalysisService

# Make KeyVaultService import optional (Azure libraries may not be installed)
try:
    from .key_vault_service import KeyVaultService, get_key_vault_service
except ImportError as e:
    # Azure libraries not available - create stub classes
    KeyVaultService = None
    get_key_vault_service = None
    import logging
    logger = logging.getLogger(__name__)
    logger.warning(f"Failed to import KeyVaultService: {e}. Azure Key Vault functionality will not be available.")
from .odata_converter import SQLToODataConverter, ODataParams, get_sql_to_odata_converter
from .datasphere_service import (
    DatasphereService,
    DatasphereError,
    TokenNotFoundError,
    TokenExpiredError,
    RefreshTokenExpiredError,
    DatasphereAPIError,
    DatasphereQueryResult,
    DatasphereAsset,
    DatasphereAssetsResult,
    DatasphereColumn,
    DatasphereViewSchema,
    get_datasphere_service,
)
from .sap_relational_fetch_service import fetch_view_data as sap_relational_fetch_view_data
from .sap_analytical_fetch_service import execute_analytical_fetch as sap_execute_analytical_fetch

__all__ = [
    # Data source analysis
    "DataSourceAnalysisService",
    # Key Vault (may be None if Azure libraries not installed)
    "KeyVaultService",
    "get_key_vault_service",
    # OData converter
    "SQLToODataConverter",
    "ODataParams",
    "get_sql_to_odata_converter",
    # Datasphere service
    "DatasphereService",
    "DatasphereError",
    "TokenNotFoundError",
    "TokenExpiredError",
    "RefreshTokenExpiredError",
    "DatasphereAPIError",
    "DatasphereQueryResult",
    "DatasphereAsset",
    "DatasphereAssetsResult",
    "DatasphereColumn",
    "DatasphereViewSchema",
    "get_datasphere_service",
    "sap_relational_fetch_view_data",
    "sap_execute_analytical_fetch",
]
