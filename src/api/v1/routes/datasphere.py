"""SAP Datasphere API endpoints for bot integration."""
import logging
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, HTTPException, status, Query
from pydantic import BaseModel, Field

from infrastructure.services.datasphere_service import (
    get_datasphere_service,
    DatasphereService,
    DatasphereError,
    TokenNotFoundError,
    TokenExpiredError,
    DatasphereAPIError,
    DatasphereAsset,
    DatasphereViewSchema,
)

router = APIRouter(prefix="/datasphere", tags=["datasphere"])
logger = logging.getLogger(__name__)


# ============================================================================
# Request/Response Models
# ============================================================================

class SQLQueryRequest(BaseModel):
    """Request model for SQL query execution."""
    user_id: str = Field(..., description="User identifier for token retrieval")
    sql_query: str = Field(..., description="SQL SELECT query to execute")
    space_id: Optional[str] = Field(None, description="Optional Datasphere space ID")
    include_count: bool = Field(False, description="Include total count in response")
    
    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "user_id": "user-123",
                    "sql_query": "SELECT product_name, quantity, price FROM sales WHERE region = 'EMEA' ORDER BY quantity DESC LIMIT 100",
                    "space_id": "ANALYTICS_SPACE",
                    "include_count": True
                }
            ]
        }
    }


class ODataQueryRequest(BaseModel):
    """Request model for OData query execution."""
    user_id: str = Field(..., description="User identifier for token retrieval")
    view_name: str = Field(..., description="View name to query")
    select: Optional[str] = Field(None, description="$select - comma-separated column names")
    filter: Optional[str] = Field(None, description="$filter - OData filter expression")
    top: Optional[int] = Field(None, ge=1, description="$top - maximum number of rows")
    skip: Optional[int] = Field(None, ge=0, description="$skip - number of rows to skip")
    orderby: Optional[str] = Field(None, description="$orderby - sorting expression")
    count: bool = Field(False, description="Include total count in response")
    data_url: Optional[str] = Field(None, description="Direct data URL from catalog asset")
    space_id: Optional[str] = Field(None, description="Optional Datasphere space ID")
    
    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "user_id": "user-123",
                    "view_name": "SalesData",
                    "select": "product_name,quantity,price",
                    "filter": "region eq 'EMEA' and quantity gt 100",
                    "top": 50,
                    "orderby": "quantity desc",
                    "count": True
                }
            ]
        }
    }


class ViewSchemaRequest(BaseModel):
    """Request model for getting view schema."""
    user_id: str = Field(..., description="User identifier for token retrieval")
    view_name: str = Field(..., description="View name to get schema for")
    metadata_url: Optional[str] = Field(None, description="Direct metadata URL from catalog asset")
    space_id: Optional[str] = Field(None, description="Optional Datasphere space ID")


class MultipleViewSchemaRequest(BaseModel):
    """Request model for getting multiple view schemas."""
    user_id: str = Field(..., description="User identifier for token retrieval")
    view_names: List[str] = Field(..., description="List of view names to get schemas for")
    assets: Optional[Dict[str, Dict[str, Any]]] = Field(None, description="Optional asset info with metadata URLs")


class DatasphereResponse(BaseModel):
    """Response model for Datasphere query results."""
    success: bool = Field(..., description="Whether the query was successful")
    data: List[dict] = Field(default_factory=list, description="Query result rows")
    row_count: int = Field(..., description="Number of rows returned")
    total_count: Optional[int] = Field(None, description="Total count (if requested)")
    next_link: Optional[str] = Field(None, description="Link for next page of results")
    metadata: Optional[dict] = Field(None, description="Additional metadata")
    error: Optional[str] = Field(None, description="Error message if failed")


class CatalogAssetsResponse(BaseModel):
    """Response model for catalog assets listing."""
    success: bool
    view_names: List[str] = Field(default_factory=list, description="List of view names (for LLM/table selection)")
    assets: Dict[str, dict] = Field(default_factory=dict, description="Full asset details keyed by name (for state storage)")
    total_count: int = Field(0, description="Total number of assets")
    error: Optional[str] = None


class ColumnInfo(BaseModel):
    """Column information model."""
    name: str
    data_type: str
    nullable: bool = True
    max_length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    label: Optional[str] = None


class ViewSchemaResponse(BaseModel):
    """Response model for view schema."""
    success: bool
    view_name: str
    columns: List[ColumnInfo] = Field(default_factory=list)
    column_names: List[str] = Field(default_factory=list, description="Just column names for quick access")
    column_info_for_llm: Optional[str] = Field(None, description="Formatted column info for LLM context")
    error: Optional[str] = None


class MultipleViewSchemaResponse(BaseModel):
    """Response model for multiple view schemas."""
    success: bool
    schemas: Dict[str, ViewSchemaResponse] = Field(default_factory=dict)
    error: Optional[str] = None


class ErrorResponse(BaseModel):
    """Standard error response."""
    success: bool = False
    error: str
    error_code: Optional[str] = None
    details: Optional[str] = None


# ============================================================================
# Helper Functions
# ============================================================================

def _handle_datasphere_error(e: Exception) -> HTTPException:
    """Convert Datasphere exceptions to HTTP exceptions."""
    if isinstance(e, TokenNotFoundError):
        return HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"success": False, "error": str(e), "error_code": "TOKEN_NOT_FOUND"}
        )
    elif isinstance(e, TokenExpiredError):
        return HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"success": False, "error": str(e), "error_code": "TOKEN_EXPIRED"}
        )
    elif isinstance(e, DatasphereAPIError):
        status_map = {
            403: status.HTTP_403_FORBIDDEN,
            404: status.HTTP_404_NOT_FOUND,
            503: status.HTTP_503_SERVICE_UNAVAILABLE,
            504: status.HTTP_503_SERVICE_UNAVAILABLE,
        }
        http_status = status_map.get(e.status_code, status.HTTP_500_INTERNAL_SERVER_ERROR)
        return HTTPException(
            status_code=http_status,
            detail={"success": False, "error": str(e), "error_code": "API_ERROR", "details": e.response_body}
        )
    elif isinstance(e, DatasphereError):
        return HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"success": False, "error": str(e), "error_code": "DATASPHERE_ERROR"}
        )
    else:
        return HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"success": False, "error": "Internal server error", "error_code": "INTERNAL_ERROR"}
        )


# ============================================================================
# Catalog Endpoints - List Available Views
# ============================================================================

@router.get(
    "/catalog/assets",
    response_model=CatalogAssetsResponse,
    summary="List Catalog Assets",
    description="""
    List all available views/tables from the SAP Datasphere Catalog API.
    
    Returns:
    - **view_names**: List of view names (use for LLM table selection)
    - **assets**: Full asset details including data URLs (store in state for later queries)
    
    The assets dictionary contains metadata URLs and data URLs that should be
    stored in state for efficient querying later.
    """
)
async def list_catalog_assets(
    user_id: str = Query(..., description="User identifier for token retrieval")
) -> CatalogAssetsResponse:
    """List all available views from the Datasphere catalog."""
    logger.info("=" * 60)
    logger.info("🔷 [Datasphere] Catalog Assets Request")
    logger.info(f"🔷 [Datasphere] User: {user_id}")
    logger.info("=" * 60)
    
    try:
        service = get_datasphere_service()
        result = await service.list_catalog_assets(user_id)
        
        logger.info(f"✅ [Datasphere] Found {len(result.view_names)} assets")
        
        return CatalogAssetsResponse(
            success=True,
            view_names=result.view_names,
            assets={name: asset.to_dict() for name, asset in result.assets.items()},
            total_count=len(result.view_names)
        )
        
    except Exception as e:
        logger.error(f"❌ [Datasphere] Catalog error: {e}", exc_info=True)
        raise _handle_datasphere_error(e)


# ============================================================================
# Schema Endpoints - Get Column Information
# ============================================================================

@router.post(
    "/schema",
    response_model=ViewSchemaResponse,
    summary="Get View Schema",
    description="""
    Get column schema for a SAP Datasphere view using the $metadata endpoint.
    
    Returns column names and types that can be used for:
    - LLM context (column_info_for_llm field)
    - Query planning
    - OData query construction
    
    No data is fetched - only metadata.
    """
)
async def get_view_schema(request: ViewSchemaRequest) -> ViewSchemaResponse:
    """Get column schema for a Datasphere view."""
    logger.info(f"🔷 [Datasphere] Schema Request for view: {request.view_name}")
    
    try:
        service = get_datasphere_service()
        schema = await service.get_view_schema(
            user_id=request.user_id,
            view_name=request.view_name,
            metadata_url=request.metadata_url,
            space_id=request.space_id
        )
        
        logger.info(f"✅ [Datasphere] Got schema with {len(schema.columns)} columns")
        
        return ViewSchemaResponse(
            success=True,
            view_name=schema.view_name,
            columns=[ColumnInfo(**col.to_dict()) for col in schema.columns],
            column_names=schema.get_column_names(),
            column_info_for_llm=schema.get_column_info_for_llm()
        )
        
    except Exception as e:
        logger.error(f"❌ [Datasphere] Schema error: {e}", exc_info=True)
        raise _handle_datasphere_error(e)


@router.post(
    "/schemas",
    response_model=MultipleViewSchemaResponse,
    summary="Get Multiple View Schemas",
    description="""
    Get column schemas for multiple SAP Datasphere views in one request.
    
    Efficient for getting schemas of all selected tables at once.
    """
)
async def get_multiple_view_schemas(request: MultipleViewSchemaRequest) -> MultipleViewSchemaResponse:
    """Get schemas for multiple views."""
    logger.info(f"🔷 [Datasphere] Multi-Schema Request for {len(request.view_names)} views")
    
    try:
        service = get_datasphere_service()
        
        # Convert assets dict if provided
        assets = None
        if request.assets:
            assets = {}
            for name, asset_dict in request.assets.items():
                assets[name] = DatasphereAsset(
                    name=asset_dict.get("name", name),
                    label=asset_dict.get("label"),
                    space_name=asset_dict.get("space_name"),
                    relational_metadata_url=asset_dict.get("relational_metadata_url"),
                    relational_data_url=asset_dict.get("relational_data_url"),
                    analytical_metadata_url=asset_dict.get("analytical_metadata_url"),
                    analytical_data_url=asset_dict.get("analytical_data_url"),
                    supports_analytical_queries=asset_dict.get("supports_analytical_queries", False),
                    has_parameters=asset_dict.get("has_parameters", False),
                )
        
        schemas = await service.get_multiple_view_schemas(
            user_id=request.user_id,
            view_names=request.view_names,
            assets=assets
        )
        
        response_schemas = {}
        for view_name, schema in schemas.items():
            response_schemas[view_name] = ViewSchemaResponse(
                success=True,
                view_name=schema.view_name,
                columns=[ColumnInfo(**col.to_dict()) for col in schema.columns],
                column_names=schema.get_column_names(),
                column_info_for_llm=schema.get_column_info_for_llm()
            )
        
        logger.info(f"✅ [Datasphere] Got schemas for {len(schemas)} views")
        
        return MultipleViewSchemaResponse(
            success=True,
            schemas=response_schemas
        )
        
    except Exception as e:
        logger.error(f"❌ [Datasphere] Multi-schema error: {e}", exc_info=True)
        raise _handle_datasphere_error(e)


# ============================================================================
# Query Endpoints - Execute OData Queries
# ============================================================================

@router.post(
    "/query/odata",
    response_model=DatasphereResponse,
    summary="Execute OData Query",
    description="""
    Execute an OData query against a SAP Datasphere view.
    
    Use this endpoint when the LLM has generated OData filter parameters.
    
    Parameters:
    - **select**: $select - comma-separated column names
    - **filter**: $filter - OData filter expression (e.g., "region eq 'EMEA' and quantity gt 100")
    - **top**: $top - limit number of rows
    - **skip**: $skip - offset for pagination
    - **orderby**: $orderby - sorting (e.g., "quantity desc")
    - **data_url**: Direct data URL from catalog asset (optional, for efficiency)
    """
)
async def execute_odata_query(request: ODataQueryRequest) -> DatasphereResponse:
    """Execute an OData query against a Datasphere view."""
    logger.info("=" * 60)
    logger.info("🔷 [Datasphere] OData Query Request")
    logger.info(f"🔷 [Datasphere] User: {request.user_id}")
    logger.info(f"🔷 [Datasphere] View: {request.view_name}")
    logger.info(f"🔷 [Datasphere] Filter: {request.filter}")
    logger.info(f"🔷 [Datasphere] Select: {request.select}")
    logger.info("=" * 60)
    
    try:
        service = get_datasphere_service()
        result = await service.execute_odata_query(
            user_id=request.user_id,
            view_name=request.view_name,
            select=request.select,
            filter=request.filter,
            top=request.top,
            skip=request.skip,
            orderby=request.orderby,
            count=request.count,
            data_url=request.data_url,
            space_id=request.space_id
        )
        
        response_data = result.to_dict()
        logger.info(f"✅ [Datasphere] Query successful: {response_data['row_count']} rows returned")
        
        return DatasphereResponse(
            success=True,
            data=response_data["data"],
            row_count=response_data["row_count"],
            total_count=response_data.get("total_count"),
            next_link=response_data.get("next_link"),
            metadata=response_data.get("metadata")
        )
        
    except Exception as e:
        logger.error(f"❌ [Datasphere] Query error: {e}", exc_info=True)
        raise _handle_datasphere_error(e)


@router.post(
    "/query/sql",
    response_model=DatasphereResponse,
    summary="Execute SQL Query (Converted to OData)",
    description="""
    Execute a SQL query against SAP Datasphere.
    
    **Note**: The SQL is converted to OData parameters internally.
    For SAP Datasphere, prefer using the OData endpoint directly with
    LLM-generated OData filter expressions.
    
    SQL clauses are converted as follows:
    - SELECT columns → $select
    - WHERE conditions → $filter
    - LIMIT n → $top
    - OFFSET n → $skip
    - ORDER BY → $orderby
    """
)
async def execute_sql_query(request: SQLQueryRequest) -> DatasphereResponse:
    """Execute a SQL query (converted to OData)."""
    logger.info("=" * 60)
    logger.info("🔷 [Datasphere] SQL Query Request")
    logger.info(f"🔷 [Datasphere] User: {request.user_id}")
    logger.info(f"🔷 [Datasphere] Query: {request.sql_query[:100]}...")
    logger.info("=" * 60)
    
    try:
        service = get_datasphere_service()
        result = await service.execute_sql_query(
            user_id=request.user_id,
            sql_query=request.sql_query,
            space_id=request.space_id,
            include_count=request.include_count
        )
        
        response_data = result.to_dict()
        logger.info(f"✅ [Datasphere] Query successful: {response_data['row_count']} rows returned")
        
        return DatasphereResponse(
            success=True,
            data=response_data["data"],
            row_count=response_data["row_count"],
            total_count=response_data.get("total_count"),
            next_link=response_data.get("next_link"),
            metadata=response_data.get("metadata")
        )
        
    except Exception as e:
        logger.error(f"❌ [Datasphere] SQL query error: {e}", exc_info=True)
        raise _handle_datasphere_error(e)


# ============================================================================
# Health Check
# ============================================================================

@router.get(
    "/health",
    summary="Datasphere Service Health Check",
    description="Check if the Datasphere service is configured and accessible."
)
async def health_check():
    """Health check endpoint for Datasphere service."""
    from config.settings import settings
    
    return {
        "service": "datasphere",
        "status": "configured" if settings.sap_odata_url else "not_configured",
        "base_url_configured": bool(settings.sap_odata_url),
        "key_vault_configured": bool(settings.azure_key_vault_url),
        "default_space_id": settings.sap_datasphere_space_id or "not_set"
    }


# ============================================================================
# Legacy Endpoints (for backward compatibility)
# ============================================================================

@router.get(
    "/entities",
    response_model=CatalogAssetsResponse,
    summary="List Entities (Legacy)",
    description="Legacy endpoint - use /catalog/assets instead for full asset information."
)
async def list_entities(
    user_id: str = Query(..., description="User identifier"),
    space_id: Optional[str] = Query(None, description="Optional space ID")
) -> CatalogAssetsResponse:
    """Legacy endpoint for listing entities."""
    return await list_catalog_assets(user_id)
