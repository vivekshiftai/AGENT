"""Repository for persisting node timing analytics to PostgreSQL."""
from typing import Any, Dict, Optional
import logging
from datetime import datetime

from .postgres_client_singleton import get_shared_postgres_client

logger = logging.getLogger(__name__)


class NodeTimingRepository:
    """
    Simple repository to record node execution timings per query into a PostgreSQL table.

    This is intentionally lightweight and best-effort only; failures here
    should never break the main analytics pipeline.
    """

    TABLE_NAME = "node_timing"

    def _get_client(self):
        """Get shared PostgreSQL client."""
        return get_shared_postgres_client(ensure_tables=False)

    def ensure_table(self) -> None:
        """
        Ensure the `node_timing` table exists in PostgreSQL.

        Schema is designed for append-only analytics:
        - created_at: event time
        - query_id: correlation id for a user query/session
        - node_name: langgraph node name
        - duration_seconds: execution time in seconds
        - pipeline: which pipeline the node belongs to (main, analysis, visualization, finalization)
        - status: node execution status (completed, failed, etc.)
        - metadata: JSON blob for additional context
        """
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
            id SERIAL PRIMARY KEY,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            query_id TEXT NOT NULL DEFAULT '',
            query_text TEXT,
            node_name TEXT NOT NULL DEFAULT '',
            duration_seconds NUMERIC(10, 3) DEFAULT 0,
            pipeline TEXT DEFAULT '',
            status TEXT DEFAULT 'completed',
            metadata JSONB DEFAULT '{{}}'::jsonb
        );
        
        DO $$ 
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name='{self.TABLE_NAME}' AND column_name='query_text'
            ) THEN
                ALTER TABLE {self.TABLE_NAME} ADD COLUMN query_text TEXT;
            END IF;
        END $$;

        CREATE INDEX IF NOT EXISTS idx_node_timing_created_at
            ON {self.TABLE_NAME}(created_at);

        CREATE INDEX IF NOT EXISTS idx_node_timing_query_id
            ON {self.TABLE_NAME}(query_id);

        CREATE INDEX IF NOT EXISTS idx_node_timing_node_name
            ON {self.TABLE_NAME}(node_name);

        CREATE INDEX IF NOT EXISTS idx_node_timing_pipeline
            ON {self.TABLE_NAME}(pipeline);
        """
        try:
            client = self._get_client()
            client.execute_update(ddl)
        except Exception as e:
            logger.debug(f"Failed to ensure node_timing table exists: {e}")

    def insert_timing(
        self,
        *,
        query_id: str,
        query_text: str = "",
        node_name: str,
        duration_seconds: float,
        pipeline: Optional[str] = None,
        status: str = "completed",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Insert a single node timing row. Best-effort only.
        
        Args:
            query_id: Query session identifier
            node_name: LangGraph node name
            duration_seconds: Execution duration in seconds
            pipeline: Pipeline name (main, analysis, visualization, finalization)
            status: Node execution status
            metadata: Optional additional metadata
        """
        try:
            self.ensure_table()
            client = self._get_client()

            ts = datetime.utcnow()
            meta = metadata or {}

            # Serialize metadata to JSON string
            import json
            meta_json = json.dumps(meta, default=str)

            sql = f"""
            INSERT INTO {self.TABLE_NAME} 
                (created_at, query_id, query_text, node_name, duration_seconds, pipeline, status, metadata)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """

            params = (
                ts,
                query_id or "",
                query_text or "",
                node_name or "",
                float(duration_seconds or 0),
                pipeline or "",
                status or "completed",
                meta_json,
            )

            client.execute_update(sql, params)
            logger.debug(f"Saved node timing: {node_name} ({duration_seconds:.3f}s) for query {query_id}")
        except Exception as e:
            # Log at debug so we don't spam logs if PostgreSQL is unavailable
            logger.debug(f"Failed to insert node_timing row: {e}")

    def insert_batch_timings(
        self,
        *,
        query_id: str,
        query_text: str = "",
        node_timings: Dict[str, float],
        pipeline_mapping: Optional[Dict[str, str]] = None,
        status: str = "completed",
    ) -> None:
        """
        Insert multiple node timings for a single query in batch.
        
        Args:
            query_id: Query session identifier
            query_text: User query text
            node_timings: Dictionary mapping node_name to duration_seconds
            pipeline_mapping: Optional dictionary mapping node_name to pipeline
            status: Node execution status (default: completed)
        """
        try:
            self.ensure_table()
            client = self._get_client()

            ts = datetime.utcnow()
            import json

            # Build batch insert
            values = []
            params_list = []
            
            for node_name, duration in node_timings.items():
                if duration is None or duration <= 0:
                    continue  # Skip invalid timings
                
                pipeline = (pipeline_mapping or {}).get(node_name, "")
                meta_json = json.dumps({}, default=str)
                
                values.append("(%s, %s, %s, %s, %s, %s, %s, %s::jsonb)")
                params_list.extend([
                    ts,
                    query_id or "",
                    query_text or "",
                    node_name or "",
                    float(duration),
                    pipeline,
                    status or "completed",
                    meta_json,
                ])

            if not values:
                return  # No valid timings to insert

            sql = f"""
            INSERT INTO {self.TABLE_NAME} 
                (created_at, query_id, query_text, node_name, duration_seconds, pipeline, status, metadata)
            VALUES
                {', '.join(values)}
            """

            client.execute_update(sql, tuple(params_list))
            logger.info(f"Saved {len(values)} node timings for query {query_id}")
        except Exception as e:
            logger.debug(f"Failed to insert batch node_timing rows: {e}")

