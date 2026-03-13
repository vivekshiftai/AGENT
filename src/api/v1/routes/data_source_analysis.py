"""Data source analysis endpoints."""
from fastapi import APIRouter, HTTPException, status, Body
from typing import Dict, Any
import logging
import asyncio

from infrastructure.database.postgres_client_singleton import get_shared_postgres_client
from infrastructure.database.data_source_gateway import DataSourceGateway
from infrastructure.services.data_source_analysis_service import DataSourceAnalysisService
from shared.exceptions import DatabaseException

router = APIRouter(prefix="/datasource_analysis", tags=["datasource_analysis"])
logger = logging.getLogger(__name__)


def get_postgres_client():
    """Get shared PostgreSQL client."""
    try:
        return get_shared_postgres_client(ensure_tables=False)
    except Exception as e:
        logger.error(f"Failed to get PostgreSQL client: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="PostgreSQL service unavailable."
        )


@router.post("/start/{data_source_id}")
async def start_analysis(
    data_source_id: int,
    user_id: str = Body(...),
    description: str = Body(...)
):
    """
    Start a data source analysis.
    
    Args:
        data_source_id: Data source ID
        user_id: User ID
        description: User-provided description of the data source
        
    Returns:
        Analysis ID and WebSocket URL
    """
    try:
        # Get data source configuration
        client = get_postgres_client()
        results = await client.execute_query_async(
            "SELECT * FROM data_source_config WHERE id = %s",
            (data_source_id,)
        )
        
        if not results:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Data source not found"
            )
        
        config = results[0]
        gateway_config = {
            "type": config["type"],
            "host": config.get("host"),
            "port": config.get("port"),
            "username": config.get("username"),
            "password": config.get("password"),
            "database_name": config.get("database_name"),
            "file_path": config.get("file_path"),
        }
        
        # Start analysis
        analysis_service = DataSourceAnalysisService()
        analysis_id = await analysis_service.start_analysis(
            data_source_id,
            user_id,
            description,
            gateway_config
        )
        
        # Start background processing task
        asyncio.create_task(analysis_service.process_analysis(analysis_id))
        logger.info(f"Started background processing for analysis {analysis_id}")
        
        return {
            "analysis_id": analysis_id,
            "status": "started"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to start analysis: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start analysis: {str(e)}"
        )


@router.get("/status/{analysis_id}")
async def get_analysis_status(analysis_id: int):
    """Get current analysis status."""
    try:
        analysis_service = DataSourceAnalysisService()
        status = await analysis_service.get_analysis_status(analysis_id)
        
        if not status:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Analysis not found"
            )
        
        return status
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get analysis status: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get analysis status: {str(e)}"
        )


@router.get("/active")
async def get_active_analyses():
    """Get all active analyses (visible to all users)."""
    try:
        analysis_service = DataSourceAnalysisService()
        analyses = await analysis_service.get_all_active_analyses()
        return analyses
        
    except Exception as e:
        logger.error(f"Failed to get active analyses: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get active analyses: {str(e)}"
        )


@router.get("/latest")
async def get_latest_analyses():
    """Get the latest analysis status for each data source (including completed ones)."""
    try:
        analysis_service = DataSourceAnalysisService()
        analyses = await analysis_service.get_latest_analyses_by_data_source()
        return analyses
        
    except Exception as e:
        logger.error(f"Failed to get latest analyses: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get latest analyses: {str(e)}"
        )


@router.get("/columns/{analysis_id}")
async def get_column_descriptions(analysis_id: int):
    """Get column descriptions for an analysis."""
    try:
        client = get_postgres_client()
        results = await client.execute_query_async(
            """
            SELECT * FROM column_descriptions 
            WHERE analysis_id = %s 
            ORDER BY table_name, column_name
            """,
            (analysis_id,)
        )
        
        return results
        
    except Exception as e:
        logger.error(f"Failed to get column descriptions: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get column descriptions: {str(e)}"
        )

