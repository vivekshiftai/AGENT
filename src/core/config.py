"""Application configuration loaded from environment variables."""
from pathlib import Path
from typing import Optional

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Settings loaded from environment variables."""

    # API server (PORT or API_PORT in .env; 8001 avoids Windows port 8000 block)
    api_port: int = Field(default=8001, validation_alias=AliasChoices("PORT", "API_PORT"))

    query_timeout_seconds: int = 600

    # ClickHouse
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_database: str = "default"
    clickhouse_user: str = "default"
    clickhouse_password: str = ""

    # PostgreSQL
    postgres_host: Optional[str] = None
    postgres_port: int = 5432
    postgres_database: str = "insightforge"
    postgres_user: str = "postgres"
    postgres_password: str = ""

    # Platform system database (psdb) - datasource metadata
    psdb_host: Optional[str] = None
    psdb_port: int = 5432
    psdb_database: str = "psdb"
    psdb_user: str = "postgres"
    psdb_password: str = ""

    # SAP Datasphere
    sap_odata_url: Optional[str] = None
    sap_datasphere_space_id: Optional[str] = "PP"
    sap_datasphere_timeout: float = 300.0
    sap_oauth_token_url: Optional[str] = None
    sap_client_id: Optional[str] = None
    sap_client_secret: Optional[str] = None
    sap_column_batch_size: int = 20
    sap_max_columns_per_view: int = 200
    sap_max_api_calls_per_minute: int = 200
    sap_rows_per_page: int = 30000
    sap_max_total_rows: int = 10_000_000
    sap_batch_concurrency: int = 50
    sap_max_chunk_calls: int = 159
    sap_max_pages: int = 5
    sap_date_chunk_days: int = 15
    sap_plan_batch_size: int = 300

    # Azure Key Vault
    azure_key_vault_url: Optional[str] = None

    # Logging
    log_level: str = "INFO"

    # LLM (for AI layer) — unified client routes by model name (claude* → Claude, else → OpenAI/Azure)
    # Claude deployments we use: claude-sonnet-4-6, claude-haiku-4-5 (set these as deployment names)
    claude_api_key: Optional[str] = None
    claude_endpoint: str = ""  # Custom base URL (e.g. proxy); if set, use AnthropicFoundry
    anthropic_api_key: Optional[str] = None  # Fallback if claude_api_key not set
    anthropic_model: str = "claude-sonnet-4-6"  # Default Claude deployment
    # Azure OpenAI (for gpt-4o etc.)
    azure_openai_endpoint: Optional[str] = None
    azure_openai_api_key: Optional[str] = None
    azure_openai_api_version: str = "2025-01-01-preview"
    azure_openai_deployment_name: str = "gpt-4o"
    azure_openai_temperature: float = 0.0
    azure_openai_max_tokens: int = 4000
    # OpenAI (fallback when Azure not set)
    openai_api_key: Optional[str] = None
    openai_base_url: Optional[str] = None
    openai_model: str = "gpt-4o-mini"

    # Planning / cortex — use deployment names: claude-sonnet-4-6, claude-haiku-4-5
    planning_query_understanding_model: str = "claude-sonnet-4-6"  # QueryAnalysisNode
    planning_planner_model: str = "claude-sonnet-4-6"  # ProductionPlanningNode

    model_config = SettingsConfigDict(
        env_file=None,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    @classmethod
    def _find_env_file(cls) -> Optional[str]:
        for path in [
            Path(__file__).parent.parent.parent / ".env",
            Path(".env"),
            Path(__file__).parent.parent.parent.parent / ".env",
        ]:
            if path.exists():
                return str(path)
        return None


_env_file = Settings._find_env_file()
if _env_file:
    Settings.model_config = SettingsConfigDict(
        env_file=_env_file,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

settings = Settings()
