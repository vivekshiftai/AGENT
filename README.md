# PSBot — Production Planning & Scheduling Backend

AI-powered production planning and scheduling backend with LangGraph orchestration, FastAPI, multi-LLM support (Azure OpenAI, Claude), and multi-source data (ClickHouse, Excel, CSV, SAP Datasphere). Outputs **Gantt-style schedule data** (machines, jobs, dates) for visualization.

## Architecture

The backend follows Clean Architecture with clear separation of concerns:

- **API Layer** (`src/api/`): FastAPI routes and middleware
- **Application Layer** (`src/application/`): Use cases and DTOs
- **Infrastructure Layer** (`src/infrastructure/`): LangGraph, LLM, database, **Data Source Abstraction** (connectors), analytics
- **Shared Layer** (`src/shared/`): Shared utilities, exceptions, and logging

## Features

- **LangGraph orchestration**: Production-planning workflow with query analysis, table/schema resolution, data fetch (SQL or SAP), computation engine, **Gantt preparation**, and analytical summary
- **Multi-LLM support**: Unified client for Azure OpenAI (GPT-4o) and Claude (Sonnet 4.5, Haiku 4.5) with node-specific model routing
- **Multi-data sources**: Connector abstraction for **ClickHouse**, **Excel**, **CSV**, and **SAP Datasphere** (analytical/relational views)
- **SAP Datasphere**: Production view `AM_Production_Analysis_v2` (space `PP`). Date filters use normal date columns when available, otherwise fiscal period columns from schema
- **Gantt output**: Response includes `gantt_data`: machines, jobs (id, name, start, end, progress), and suggested follow-up queries
- **Polars & Pandas**: Fast data processing; Polars LazyFrame for pipeline steps
- **Concurrency control**: Configurable limits for simultaneous queries and LLM calls
- **Standard Python logging** and robust JSON parsing for LLM responses

## LangGraph Workflow

Simplified **production planning** pipeline (no separate chart/finance pipelines):

### Phase I: Context & strategy (sequential)
1. **query_analysis** (`parse_query`): Production-focused intent (orders, machines, capacity, schedule)
2. **table_identification** (`select_tables`): Selects tables/views (for SAP: uses `AM_Production_Analysis_v2`)
3. **load_data** / **get_schema**: Schema and metadata (via connector for any supported source)
4. **Routing**: SAP path → analytical schema + column selection + date/fiscal filter; non-SAP → SQL plan + generate_sql

### Phase II: Data fetch
- **SAP**: `sap_fetch_plan` → `sap_data_fetch_simple` (analytical or relational based on view/schema)
- **Non-SAP**: `fetch_data` via ConnectorFactory (ClickHouse, Excel, CSV)

### Phase III: Compute & deliver
1. **computation_engine**: Aggregations and calculations
2. **gantt_preparation**: Builds Gantt payload from raw data (machineId, jobs with id, name, start, end, progress)
3. **analytical_summary**: Production-focused narrative and schedule overview
4. **response_orchestration**: Merges metrics, Gantt data, and summary into dashboard response

### SAP date filtering
- If the view schema has **Edm.Date** columns → standard date filter (start/end)
- If only **fiscal** columns (e.g. fiscal period) → fiscal filter (BT start,end) as input parameters

## Setup

### Prerequisites

- Python 3.11+
- Azure OpenAI (GPT-4o) and optionally Claude for optimal node routing
- For SAP: SAP Datasphere tenant, OAuth, and space `PP` with view `AM_Production_Analysis_v2`
- Optional: ClickHouse, PostgreSQL (for data source config storage)

### Environment variables

Copy `.env.example` to `.env` and configure:

```bash
# Azure OpenAI
AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/
AZURE_OPENAI_API_KEY=your-api-key
AZURE_OPENAI_DEPLOYMENT_NAME=gpt-4o

# Claude (optional)
CLAUDE_API_KEY=your-claude-api-key
CLAUDE_ENDPOINT=https://auraaresource.services.ai.azure.com/anthropic

# SAP Datasphere (for production view)
SAP_ODATA_URL=https://3f8448f3.us10.hcs.cloud.sap
SAP_DATASPHERE_SPACE_ID=PP
# SAP_OAUTH_TOKEN_URL=...
# SAP_CLIENT_ID=...
# SAP_CLIENT_SECRET=...

# ClickHouse (optional)
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=8123
CLICKHOUSE_DATABASE=analytics

# PostgreSQL (data source config). The database must exist on the server.
# If you see "database \"productionplanbot\" does not exist", create it: createdb productionplanbot
# or set POSTGRES_DATABASE=insightforge to use the default.
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=insightforge
POSTGRES_USER=postgres
POSTGRES_PASSWORD=

# Concurrency (optional)
MAX_CONCURRENT_QUERIES=5
MAX_CONCURRENT_LLM_CALLS=10
QUERY_TIMEOUT_SECONDS=300
```

### Installation

```bash
pip install -r requirements.txt
```

### Run

From the project root (`PSBot`):

```bash
# Recommended: uvicorn with reload
uvicorn src.main:app --reload --host 0.0.0.0 --port 2345

# Or run as module
python -m src.main
```

For WebSocket support (long-running queries), the app uses `timeout_keep_alive=0` and WebSocket ping settings by default.

**Verify:** `curl http://localhost:2345/` → `{"name":"...","version":"1.0.0","status":"running"}`

### Running on a VM / remote

1. Use `--host 0.0.0.0` so the API is reachable from other machines.
2. Point the frontend to `http://<VM_IP>:2345`.
3. Open port 2345 in the firewall.

## API

### POST /api/v1/query

Process a production-planning query; returns dashboard with metrics and **Gantt schedule data**.

**Request:**
```json
{
  "query": "Show production schedule for next week by machine",
  "user_id": "user_123"
}
```

**Response (simplified):**
```json
{
  "query": "Show production schedule for next week by machine",
  "gantt_data": {
    "machines": [
      {
        "machineId": "CNC01",
        "jobs": [
          {
            "id": "OP1001",
            "name": "Production Order 1001",
            "start": "2026-03-06",
            "end": "2026-03-08",
            "progress": 20
          }
        ]
      }
    ],
    "suggested_queries": ["Show next week", "Filter by machine"]
  },
  "narrative_summary": "...",
  "status": "success"
}
```

### GET /api/v1/health

Health check.

## Concurrency control

- **MAX_CONCURRENT_QUERIES**: Max simultaneous query requests (default 5)
- **MAX_CONCURRENT_LLM_CALLS**: Max simultaneous LLM calls (default 10)
- **QUERY_TIMEOUT_SECONDS**: Query timeout (default 300)

When limits are reached, new requests can be queued or rejected with HTTP 503.

## Data source abstraction

Connectors live under `src/infrastructure/datasources/`:

- **BaseConnector**: Interface with `fetch_data(query_plan)` → pandas DataFrame
- **ClickHouseConnector**, **ExcelConnector**, **SAPConnector**: Implementations
- **ConnectorFactory**: Chooses connector by configured source type (`clickhouse`, `excel`, `sap`)
- **get_schema** uses the same abstraction to retrieve metadata from the active source

## Project structure

```
PSBot/
├── src/
│   ├── api/                    # FastAPI routes
│   ├── application/            # DTOs (e.g. DashboardResponse with gantt_data)
│   ├── config/                 # settings.py
│   ├── infrastructure/
│   │   ├── datasources/        # ConnectorFactory, ClickHouse, Excel, SAP
│   │   ├── langgraph/
│   │   │   ├── nodes/           # parse_query, select_tables, get_schema,
│   │   │   │                   # gantt_preparation, analytical_summary, etc.
│   │   │   ├── data_analysis_graph.py
│   │   │   ├── prompts.py
│   │   │   └── state.py
│   │   ├── llm/                # Unified LLM client
│   │   ├── database/           # ClickHouse, gateway
│   │   └── services/           # SAP Datasphere, analytical/relational fetch
│   └── shared/
├── prompts/                    # Persisted LLM inputs/outputs
├── requirements.txt
└── README.md
```

## Development

- Type hints, async/await, Pydantic
- Node-specific LLM models in `src/config/settings.py`
- LLM I/O persisted under `prompts/input/` and `prompts/output/` for debugging

## Docker

```bash
docker-compose up -d
docker-compose logs -f backend
docker-compose down
```
