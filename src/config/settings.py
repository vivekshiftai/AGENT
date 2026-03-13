"""Application configuration settings."""
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional
from pathlib import Path


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # API Settings
    api_title: str = "InsightForge Analytics API"
    api_version: str = "1.0.0"
    # Use /v1 to match frontend (e.g. /v1/datasource/list). Set API_PREFIX=/api/v1 if you need /api/v1.
    api_prefix: str = "/v1"
    api_port: int = 8023
    debug: bool = False
    
    # Azure OpenAI Settings
    azure_openai_endpoint: Optional[str] = None
    azure_openai_api_key: Optional[str] = None
    azure_openai_api_version: str = "2025-01-01-preview"
    azure_openai_deployment_name: str = "gpt-4o"
    azure_openai_temperature: float = 0.0
    azure_openai_max_tokens: int = 4000
    
    # Claude API Configuration (separate from OpenAI)
    claude_api_key: Optional[str] = None  # Separate API key for Claude
    claude_endpoint: str = "https://auraaresource.services.ai.azure.com/anthropic"  # Claude endpoint base URL
    
    # DeepSeek API Configuration (backup for Sonnet)
    # Note: DeepSeek uses the same API key as Azure OpenAI (azure_openai_api_key)
    # No separate DEEPSEEK_API_KEY needed - it uses the same credentials as GPT-4o
    deepseek_api_key: Optional[str] = None  # Deprecated: Not used anymore, uses azure_openai_api_key instead
    deepseek_endpoint: str = "https://auraaresource.services.ai.azure.com/openai/v1/"  # DeepSeek endpoint
    deepseek_model_name: str = "DeepSeek-R1"  # DeepSeek model name
    deepseek_deployment_name: str = "DeepSeek-R1"  # DeepSeek deployment name
    
    # Analytics workflow node-specific models (configurable per node; override via .env)
    # Grouped by LangGraph flow phase — see data_analysis_graph.py for flow structure.
    # -------------------------------------------------------------------------------
    # Phase I: Context & Strategy (sequential — query_analysis, orchestration_agent, table_identification, load_data, get_schema)
    # -------------------------------------------------------------------------------
    # Use Claude Haiku for query_analysis so extended thinking works when LLM_THINKING_ENABLED=true (GPT-4o does not support thinking)
    analytics_parse_query_model: str = "claude-haiku-4-5"  # query_analysis (parse user intent)
    analytics_orchestration_agent_model: str = "claude-haiku-4-5"  # orchestration_agent (simple vs moderate vs clarification)
    analytics_select_tables_model: str = "claude-haiku-4-5"  # table_identification (select tables)
    analytics_load_data_model: str = "claude-haiku-4-5"  # load_data (column normalization / date-type detection LLM)
    # get_schema: no LLM
    # -------------------------------------------------------------------------------
    # Analytical Schema Pipeline (SAP only; parallel with Phase I; prepare_analytical_schema + analytical_column_selection)
    # -------------------------------------------------------------------------------
    analytics_analytical_column_selection_model: str = "claude-haiku-4-5"  # Column selection (chunks) only
    analytics_analytical_date_filter_model: str = "claude-haiku-4-5"  # Date filter LLM (when Edm.Date cols exist)
    analytics_analytical_fiscal_filter_model: str = "claude-haiku-4-5"  # Fiscal filter LLM (when no date cols, fiscal cols)
    # prepare_analytical_schema: no LLM (parses SAP metadata XML)
    # -------------------------------------------------------------------------------
    # Phase II: Data Source Routing (SAP vs Non-SAP — plan + fetch)
    # -------------------------------------------------------------------------------
    analytics_sql_plan_model: str = "claude-haiku-4-5"  # sql_plan_synthesis (non-SAP); also used by sap_api_filter
    analytics_sap_fetch_plan_model: str = "claude-haiku-4-5"  # sap_fetch_plan (SAP flow)
    analytics_generate_sql_model: str = "claude-sonnet-4-5"  # sql_generation (non-SAP)
    # -------------------------------------------------------------------------------
    # Phase II (Parallel): Planning Pipelines (4 nodes — each has its own model)
    # -------------------------------------------------------------------------------
    analytics_financial_analyst_planner_model: str = "claude-sonnet-4-5"  # financial_analyst_planner (Pipeline A)
    analytics_chart_preplan_model: str = "claude-sonnet-4-5"  # chart_preplan (Pipeline B: suggested charts)
    analytics_chart_plan_model: str = "claude-haiku-4-5"  # chart_planning (Pipeline B: detailed chart specs)
    # Maximum number of category groups to process in parallel when planning charts (per-batch concurrency)
    analytics_max_parallel_chart_groups: int = 4
    analytics_operation_specification_model: str = "claude-haiku-4-5"  # operation_specification (KPI / operation plan after data fetch)
    # -------------------------------------------------------------------------------
    # Phase III: Execution (after data fetch — computation_engine, chart_preparation, then summary + intelligence)
    # -------------------------------------------------------------------------------
    analytics_analytical_summary_model: str = "claude-sonnet-4-5"  # analytical_summary (overall summary using all metrics)
    analytics_analytical_group_summary_model: str = "claude-haiku-4-5"  # analytical_summary (per-category group summaries)
    analytics_gantt_preparation_model: str = "claude-haiku-4-5"  # gantt_preparation (LLM-driven column mapping for Gantt chart)
    # computation_engine: no LLM
    # -------------------------------------------------------------------------------
    # Other / fallback
    # -------------------------------------------------------------------------------
    analytics_synthesize_model: str = "gpt-4o"  # No LLM, kept for consistency
    analytics_fallback_model: str = "gpt-4o"
    analytics_data_source_analysis_model: str = "gpt-4o"  # data_source_analysis_service (column descriptions for tables)
    
    # ClickHouse Settings (HTTP Interface)
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123  # HTTP interface port
    clickhouse_database: str = "analytics"
    clickhouse_user: str = "default"
    clickhouse_password: str = ""
    clickhouse_pool_size: int = 10
    
    # PostgreSQL Settings (for data source config storage)
    postgres_host: Optional[str] = None
    postgres_port: int = 5432
    postgres_database: str = "insightforge"
    postgres_user: str = "postgres"
    postgres_password: str = ""
    
    # LLM Thinking / Reasoning Settings
    # When enabled, Claude uses extended thinking and DeepSeek R1 reasoning_content
    # is captured. Thinking tokens are streamed to the UI for transparency.
    llm_thinking_enabled: bool = True  # Set to True via LLM_THINKING_ENABLED env var
    llm_thinking_budget_tokens: int = 10000  # Max tokens for Claude extended thinking budget
    
    # LangGraph Settings
    langgraph_max_iterations: int = 5
    langgraph_debug: bool = False
    
    # Concurrency Settings
    max_concurrent_queries: int = 5  # Maximum number of simultaneous query requests
    max_concurrent_llm_calls: int = 10  # Maximum number of simultaneous LLM API calls
    query_timeout_seconds: int = 1800  # Timeout for query processing (30 minutes for 10M+ row datasets)
    
    # WebSocket Settings (for long-running LLM queries)
    ws_ping_interval: int = 30  # Seconds between WebSocket pings
    ws_ping_timeout: int = 1800  # Seconds to wait for pong response (30 min for 10M+ row queries)
    
    # Logging Settings
    # Use WARNING in production for 10M+ row datasets (reduces log overhead)
    # Set to INFO or DEBUG in .env for development/debugging
    log_level: str = "WARNING"
    
    # Data Cache Settings (for large dataset exports)
    # Cache fetched data to avoid re-querying database on export
    data_cache_enabled: bool = True
    data_cache_dir: str = "data_cache"  # Directory for cached data files
    data_cache_ttl_minutes: int = 60  # Time-to-live for cached data (1 hour default)
    data_cache_max_size_gb: float = 10.0  # Maximum cache size in GB
    
    # Cleaned Data Cache Settings (for Excel/CSV files)
    # Cache cleaned/normalized DataFrames to avoid re-cleaning on every query
    cleaned_data_cache_enabled: bool = True
    cleaned_data_cache_dir: str = "cleaned_data_cache"  # Directory for cached cleaned DataFrames
    
    # Azure Key Vault Settings (for SAP Datasphere token storage)
    azure_key_vault_url: Optional[str] = None  # e.g., https://your-vault.vault.azure.net/
    
    # SAP Datasphere Settings
    sap_odata_url: Optional[str] = None  # e.g., https://tenant.datasphere.cloud.sap (env: SAP_ODATA_URL)
    sap_datasphere_space_id: Optional[str] = "PP"  # Default space ID for production planning view (env: SAP_DATASPHERE_SPACE_ID)
    sap_datasphere_timeout: float = 300.0  # API request timeout in seconds (5 minutes default) (env: SAP_DATASPHERE_TIMEOUT)
    
    # SAP OAuth Settings (for token refresh)
    sap_oauth_token_url: Optional[str] = None  # e.g., https://tenant.authentication.region.hana.ondemand.com/oauth/token (env: SAP_OAUTH_TOKEN_URL)
    sap_client_id: Optional[str] = None  # SAP OAuth client ID (env: SAP_CLIENT_ID)
    sap_client_secret: Optional[str] = None  # SAP OAuth client secret (env: SAP_CLIENT_SECRET)
    
    # SAP Datasphere Column Batching Settings
    # When views have many columns, split into batches to respect API limits
    sap_column_batch_size: int = 20  # Number of columns per API call (env: SAP_COLUMN_BATCH_SIZE)
    sap_max_columns_per_view: int = 200  # Maximum columns to fetch per view (env: SAP_MAX_COLUMNS_PER_VIEW)
    sap_max_api_calls_per_minute: int = 200  # API rate limit per user query (env: SAP_MAX_API_CALLS_PER_MINUTE)
    sap_rows_per_page: int = 30000  # Maximum rows per API request (env: SAP_ROWS_PER_PAGE)
    sap_max_total_rows: int = 10000000  # Safety limit: max total rows to fetch (env: SAP_MAX_TOTAL_ROWS)
    sap_batch_concurrency: int = 50  # Number of concurrent batch requests (env: SAP_BATCH_CONCURRENCY)
    sap_max_chunk_calls: int = 159  # Max chunk API calls per view (env: SAP_MAX_CHUNK_CALLS) - API supports up to 159
    sap_max_pages: int = 5  # Maximum pages to fetch per view (env: SAP_MAX_PAGES) - limits data to ~150k rows (30k per page)
    sap_date_chunk_days: int = 15  # Number of days per date chunk query (env: SAP_DATE_CHUNK_DAYS)
    sap_plan_batch_size: int = 300  # Number of columns per plan batch (env: SAP_PLAN_BATCH_SIZE) - split large views into multiple plans
    
    model_config = SettingsConfigDict(
        env_file=None,  # Will be set dynamically below
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"  # Ignore extra environment variables
    )
    
    @classmethod
    def _find_env_file(cls) -> Optional[str]:
        """Find .env file in analytics-backend directory or current directory."""
        # Try analytics-backend/.env first (when running from project root)
        backend_env = Path(__file__).parent.parent.parent / ".env"
        if backend_env.exists():
            return str(backend_env)
        
        # Try current directory .env (when running from analytics-backend)
        current_env = Path(".env")
        if current_env.exists():
            return str(current_env)
        
        # Try parent directory .env (fallback)
        parent_env = Path(__file__).parent.parent.parent.parent / ".env"
        if parent_env.exists():
            return str(parent_env)
        
        return None


# Find and set env_file path
_env_file = Settings._find_env_file()
if _env_file:
    Settings.model_config = SettingsConfigDict(
        env_file=_env_file,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )

settings = Settings()

