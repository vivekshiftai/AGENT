"""LLM Usage analytics endpoints."""
from fastapi import APIRouter, HTTPException, status, Query
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import logging
from infrastructure.database.postgres_client_singleton import get_shared_postgres_client

router = APIRouter(prefix="/llm-usage", tags=["llm-usage"])
logger = logging.getLogger(__name__)


@router.get("", status_code=status.HTTP_200_OK)
async def get_llm_usage(
    query_id: Optional[str] = Query(None, description="Filter by query_id"),
    node_name: Optional[str] = Query(None, description="Filter by node name"),
    limit: Optional[int] = Query(None, ge=1, description="Maximum number of records to return (None = all records)"),
    days: int = Query(7, ge=1, le=365, description="Number of days to look back (1-365)")
) -> Dict[str, Any]:
    """
    Get LLM usage analytics.
    
    Args:
        query_id: Optional query ID to filter by
        node_name: Optional node name to filter by
        limit: Maximum number of records
        days: Number of days to look back
        
    Returns:
        Dictionary with usage data and summary statistics
    """
    try:
        postgres_client = get_shared_postgres_client(ensure_tables=False)
        
        # Build WHERE clause
        conditions = ["created_at >= %s"]
        params = [datetime.utcnow() - timedelta(days=days)]
        
        if query_id:
            conditions.append("query_id = %s")
            params.append(query_id)
        
        if node_name:
            conditions.append("node_name = %s")
            params.append(node_name)
        
        where_clause = " AND ".join(conditions)
        base_params = tuple(params)
        
        # Execute queries in parallel using a single connection (more efficient)
        # Get usage records - fetch all records if limit is None, otherwise use limit
        if limit is None:
            records_sql = f"""
            SELECT 
                id,
                created_at,
                query_id,
                query_text,
                node_name,
                provider,
                model,
                input_tokens,
                output_tokens,
                total_tokens,
                config
            FROM llm_usage
            WHERE {where_clause}
            ORDER BY created_at DESC
            """
            records = await postgres_client.execute_query_async(records_sql, base_params)
        else:
            records_sql = f"""
            SELECT 
                id,
                created_at,
                query_id,
                query_text,
                node_name,
                provider,
                model,
                input_tokens,
                output_tokens,
                total_tokens,
                config
            FROM llm_usage
            WHERE {where_clause}
            ORDER BY created_at DESC
            LIMIT %s
            """
            records_params = base_params + (limit,)
            records = await postgres_client.execute_query_async(records_sql, records_params)
        
        # Get summary statistics (optimized with single query)
        summary_sql = f"""
        SELECT 
            COUNT(*) as total_calls,
            SUM(input_tokens) as total_input_tokens,
            SUM(output_tokens) as total_output_tokens,
            SUM(total_tokens) as total_tokens,
            COUNT(DISTINCT query_id) as unique_queries,
            COUNT(DISTINCT node_name) as unique_nodes,
            COUNT(DISTINCT model) as unique_models
        FROM llm_usage
        WHERE {where_clause}
        """
        
        summary = await postgres_client.execute_query_async(summary_sql, base_params)
        summary_data = summary[0] if summary else {}
        
        # Get breakdown by node (only if we have data)
        node_breakdown = []
        if summary_data.get('total_calls', 0) > 0:
            node_breakdown_sql = f"""
            SELECT 
                node_name,
                COUNT(*) as call_count,
                SUM(input_tokens) as total_input_tokens,
                SUM(output_tokens) as total_output_tokens,
                SUM(total_tokens) as total_tokens,
                AVG(total_tokens) as avg_tokens
            FROM llm_usage
            WHERE {where_clause}
            GROUP BY node_name
            ORDER BY total_tokens DESC
            LIMIT 500
            """
            node_breakdown = await postgres_client.execute_query_async(node_breakdown_sql, base_params)
        
        # Get breakdown by model (only if we have data)
        model_breakdown = []
        if summary_data.get('total_calls', 0) > 0:
            model_breakdown_sql = f"""
            SELECT 
                model,
                provider,
                COUNT(*) as call_count,
                SUM(input_tokens) as total_input_tokens,
                SUM(output_tokens) as total_output_tokens,
                SUM(total_tokens) as total_tokens
            FROM llm_usage
            WHERE {where_clause}
            GROUP BY model, provider
            ORDER BY total_tokens DESC
            LIMIT 50
            """
            model_breakdown = await postgres_client.execute_query_async(model_breakdown_sql, base_params)
        
        return {
            "records": records,
            "summary": summary_data,
            "breakdown_by_node": node_breakdown,
            "breakdown_by_model": model_breakdown,
            "filters": {
                "query_id": query_id,
                "node_name": node_name,
                "days": days,
                "limit": limit
            }
        }
        
    except Exception as e:
        logger.error(f"Failed to fetch LLM usage: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch LLM usage: {str(e)}"
        )


@router.get("/by-query/{query_id}", status_code=status.HTTP_200_OK)
async def get_llm_usage_by_query(query_id: str) -> Dict[str, Any]:
    """
    Get LLM usage for a specific query.
    
    Args:
        query_id: Query ID to filter by
        
    Returns:
        Dictionary with usage data for the query
    """
    return await get_llm_usage(query_id=query_id, limit=None)  # Fetch all records for the query

