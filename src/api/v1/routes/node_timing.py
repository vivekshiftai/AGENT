"""API routes for node timing analytics."""
from fastapi import APIRouter, Query, status
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
import logging

from infrastructure.database.postgres_client_singleton import get_shared_postgres_client

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/node-timing", tags=["node-timing"])


@router.get("", status_code=status.HTTP_200_OK)
async def get_node_timing(
    query_id: Optional[str] = Query(None, description="Filter by query_id"),
    node_name: Optional[str] = Query(None, description="Filter by node name"),
    pipeline: Optional[str] = Query(None, description="Filter by pipeline"),
    limit: Optional[int] = Query(None, ge=1, description="Maximum number of records to return (None = all records)"),
    days: int = Query(7, ge=1, le=365, description="Number of days to look back (1-365)")
) -> Dict[str, Any]:
    """
    Get node timing analytics.
    
    Args:
        query_id: Optional query ID to filter by
        node_name: Optional node name to filter by
        pipeline: Optional pipeline name to filter by
        limit: Maximum number of records
        days: Number of days to look back
        
    Returns:
        Dictionary with timing data and summary statistics
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
        
        if pipeline:
            conditions.append("pipeline = %s")
            params.append(pipeline)
        
        where_clause = " AND ".join(conditions)
        base_params = tuple(params)
        
        # Fetch all records first (without limit for accurate unique counts and grouping)
        all_records_sql = f"""
        SELECT 
            id,
            created_at,
            query_id,
            query_text,
            node_name,
            duration_seconds,
            pipeline,
            status,
            metadata
        FROM node_timing
        WHERE {where_clause}
        ORDER BY created_at DESC
        """
        
        all_records_raw = await postgres_client.execute_query_async(all_records_sql, base_params)
        
        # Process all records and extract unique query_ids
        all_records = []
        unique_query_ids = set()
        unique_node_names = set()
        unique_pipelines = set()
        total_duration_sum = 0.0
        min_duration = None
        max_duration = None
        
        for row in all_records_raw:
            query_id = row.get("query_id", "").strip() if row.get("query_id") else ""
            node_name = row.get("node_name", "").strip() if row.get("node_name") else ""
            pipeline = row.get("pipeline", "").strip() if row.get("pipeline") else "Unknown"
            duration = float(row.get("duration_seconds", 0) or 0)
            
            # Track unique values
            if query_id:
                unique_query_ids.add(query_id)
            if node_name:
                unique_node_names.add(node_name)
            if pipeline:
                unique_pipelines.add(pipeline)
            
            # Track duration stats
            total_duration_sum += duration
            if min_duration is None or duration < min_duration:
                min_duration = duration
            if max_duration is None or duration > max_duration:
                max_duration = duration
            
            # Build record (only include in limited results)
            all_records.append({
                "id": row.get("id"),
                "created_at": row.get("created_at"),
                "query_id": query_id,
                "query_text": row.get("query_text", ""),
                "node_name": node_name,
                "duration_seconds": duration,
                "pipeline": pipeline,
                "status": row.get("status", "completed"),
                "metadata": row.get("metadata", {}),
            })
        
        # Limit records for response if limit is specified (already sorted by created_at DESC)
        records = all_records[:limit] if limit is not None else all_records
        
        # Calculate summary statistics
        total_records = len(all_records)
        unique_queries_count = len(unique_query_ids)
        unique_nodes_count = len(unique_node_names)
        unique_pipelines_count = len(unique_pipelines)
        avg_duration_per_record = total_duration_sum / total_records if total_records > 0 else 0.0
        min_duration = min_duration if min_duration is not None else 0.0
        max_duration = max_duration if max_duration is not None else 0.0
        
        summary_data = {
            "total_records": total_records,
            "total_duration": total_duration_sum,
            "avg_duration": avg_duration_per_record,
            "min_duration": min_duration,
            "max_duration": max_duration,
            "unique_queries": unique_queries_count,
            "unique_nodes": unique_nodes_count,
            "unique_pipelines": unique_pipelines_count,
        }
        
        # Log for debugging
        logger.info(f"Node timing summary - Total records: {total_records}, Total duration: {total_duration_sum}s, Unique queries: {unique_queries_count}, Unique query IDs: {list(unique_query_ids)}")
        
        # Get breakdown by node (only if we have data)
        node_breakdown = []
        if summary_data.get('total_records', 0) > 0:
            breakdown_sql = f"""
            SELECT 
                node_name,
                COUNT(*) as record_count,
                SUM(duration_seconds) as total_duration,
                AVG(duration_seconds) as avg_duration,
                MIN(duration_seconds) as min_duration,
                MAX(duration_seconds) as max_duration
            FROM node_timing
            WHERE {where_clause}
            GROUP BY node_name
            ORDER BY total_duration DESC
            """
            node_breakdown_raw = await postgres_client.execute_query_async(breakdown_sql, base_params)
            # Ensure all numeric fields are properly converted to float
            node_breakdown = [
                {
                    "node_name": row.get("node_name", ""),
                    "record_count": int(row.get("record_count", 0) or 0),
                    "total_duration": float(row.get("total_duration", 0) or 0),
                    "avg_duration": float(row.get("avg_duration", 0) or 0),
                    "min_duration": float(row.get("min_duration", 0) or 0),
                    "max_duration": float(row.get("max_duration", 0) or 0),
                }
                for row in node_breakdown_raw
            ]
        
        # Get breakdown by pipeline
        pipeline_breakdown = []
        if summary_data.get('total_records', 0) > 0:
            pipeline_sql = f"""
            SELECT 
                pipeline,
                COUNT(*) as record_count,
                SUM(duration_seconds) as total_duration,
                AVG(duration_seconds) as avg_duration
            FROM node_timing
            WHERE {where_clause}
            GROUP BY pipeline
            ORDER BY total_duration DESC
            """
            pipeline_breakdown_raw = await postgres_client.execute_query_async(pipeline_sql, base_params)
            # Ensure all numeric fields are properly converted to float
            pipeline_breakdown = [
                {
                    "pipeline": row.get("pipeline", "") or "Unknown",
                    "record_count": int(row.get("record_count", 0) or 0),
                    "total_duration": float(row.get("total_duration", 0) or 0),
                    "avg_duration": float(row.get("avg_duration", 0) or 0),
                }
                for row in pipeline_breakdown_raw
            ]

        # Calculate average duration as total_duration / unique_queries
        total_duration = summary_data.get("total_duration", 0.0)
        unique_queries = summary_data.get("unique_queries", 0)
        avg_duration = total_duration / unique_queries if unique_queries > 0 else 0.0
        
        return {
             "records": records,
             "summary": {
                 "total_records": summary_data.get("total_records", 0),
                 "total_duration": total_duration,
                 "avg_duration": avg_duration,
                 "min_duration": summary_data.get("min_duration", 0.0),
                 "max_duration": summary_data.get("max_duration", 0.0),
                 "unique_queries": unique_queries,
                 "unique_nodes": summary_data.get("unique_nodes", 0),
                 "unique_pipelines": summary_data.get("unique_pipelines", 0),
             },
             "breakdown_by_node": node_breakdown,
             "breakdown_by_pipeline": pipeline_breakdown,
         }
    except Exception as e:
        logger.error(f"Failed to get node timing data: {e}")
        raise


@router.get("/by-query/{query_id}", status_code=status.HTTP_200_OK)
async def get_node_timing_by_query(query_id: str) -> Dict[str, Any]:
    """
    Get node timing data for a specific query.
    
    Args:
        query_id: Query ID to filter by
        
    Returns:
        Dictionary with timing data for the query
    """
    return await get_node_timing(query_id=query_id, limit=None)  # Fetch all records for the query

