"""Repository for persisting LLM token usage analytics to PostgreSQL."""
from typing import Any, Dict, Optional, List
import logging
from datetime import datetime
import json

from .postgres_client_singleton import get_shared_postgres_client

logger = logging.getLogger(__name__)


class LLMUsageRepository:
    """
    Simple repository to record LLM usage per call into a PostgreSQL table.

    This is intentionally lightweight and best-effort only; failures here
    should never break the main analytics pipeline.
    """

    TABLE_NAME = "llm_usage"

    def _get_client(self):
        """Get shared PostgreSQL client."""
        return get_shared_postgres_client(ensure_tables=False)

    def ensure_table(self) -> None:
        """
        Ensure the `llm_usage` table exists in PostgreSQL.

        Schema is designed for append-only analytics:
        - created_at: event time
        - query_id: optional correlation id for a user query/session
        - node_name: langgraph node name
        - provider/model: which LLM
        - input_tokens/output_tokens/total_tokens: token utilization
        - config: JSON blob for temperature, max_tokens, json_mode etc.
        """
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
            id SERIAL PRIMARY KEY,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            query_id TEXT DEFAULT '',
            query_text TEXT,
            node_name TEXT DEFAULT '',
            provider TEXT DEFAULT '',
            model TEXT DEFAULT '',
            input_tokens INTEGER DEFAULT 0,
            output_tokens INTEGER DEFAULT 0,
            total_tokens INTEGER DEFAULT 0,
            config JSONB DEFAULT '{{}}'::jsonb
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

        CREATE INDEX IF NOT EXISTS idx_llm_usage_created_at
            ON {self.TABLE_NAME}(created_at);

        CREATE INDEX IF NOT EXISTS idx_llm_usage_node_name
            ON {self.TABLE_NAME}(node_name);
        """
        try:
            client = self._get_client()
            client.execute_update(ddl)
        except Exception as e:
            logger.debug(f"Failed to ensure llm_usage table exists: {e}")

    def insert_usage(
        self,
        *,
        provider: str,
        model: str,
        node_name: str,
        query_id: str = "",
        query_text: str = "",
        input_tokens: int = 0,
        output_tokens: int = 0,
        total_tokens: int = 0,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Insert a single LLM usage row. Best-effort only.
        """
        try:
            self.ensure_table()
            client = self._get_client()

            ts = datetime.utcnow()
            cfg = config or {}

            # psycopg2 cannot adapt bare dicts by default; serialize to JSON string
            cfg_json = json.dumps(cfg, default=str)

            sql = f"""
            INSERT INTO {self.TABLE_NAME} 
                (created_at, query_id, query_text, node_name, provider, model, input_tokens, output_tokens, total_tokens, config)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """

            params = (
                ts,
                query_id or "",
                query_text or "",
                node_name or "",
                provider or "",
                model or "",
                int(input_tokens or 0),
                int(output_tokens or 0),
                int(total_tokens or 0),
                cfg_json,
            )

            client.execute_update(sql, params)
        except Exception as e:
            # Log at debug so we don't spam logs if PostgreSQL is unavailable
            logger.debug(f"Failed to insert llm_usage row: {e}")

    def insert_batch_usage(
        self,
        *,
        query_id: str,
        query_text: str = "",
        usage_records: List[Dict[str, Any]],
    ) -> None:
        """
        Insert multiple LLM usage records for a single query in batch.
        
        Args:
            query_id: Query session identifier
            query_text: User query text
            usage_records: List of usage record dictionaries with keys:
                - provider: str
                - model: str
                - node_name: str
                - input_tokens: int
                - output_tokens: int
                - total_tokens: int
                - config: Optional[Dict[str, Any]]
        """
        if not usage_records:
            return
        
        try:
            self.ensure_table()
            client = self._get_client()

            ts = datetime.utcnow()
            values = []
            params_list = []
            
            for record in usage_records:
                provider = record.get("provider", "")
                model = record.get("model", "")
                node_name = record.get("node_name", "")
                input_tokens = int(record.get("input_tokens", 0))
                output_tokens = int(record.get("output_tokens", 0))
                total_tokens = int(record.get("total_tokens", 0) or (input_tokens + output_tokens))
                config = record.get("config", {})
                
                # Skip invalid records
                if not provider or not model or not node_name:
                    continue
                
                cfg_json = json.dumps(config, default=str)
                
                values.append("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)")
                params_list.extend([
                    ts,
                    query_id or "",
                    query_text or "",
                    node_name,
                    provider,
                    model,
                    input_tokens,
                    output_tokens,
                    total_tokens,
                    cfg_json,
                ])

            if not values:
                return  # No valid records to insert

            sql = f"""
            INSERT INTO {self.TABLE_NAME} 
                (created_at, query_id, query_text, node_name, provider, model, input_tokens, output_tokens, total_tokens, config)
            VALUES
                {', '.join(values)}
            """

            client.execute_update(sql, tuple(params_list))
            logger.info(f"Saved {len(values)} LLM usage records to database for query {query_id}")
        except Exception as e:
            logger.debug(f"Failed to insert batch llm_usage rows: {e}")

