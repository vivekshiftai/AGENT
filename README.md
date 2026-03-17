# PSBot - Data Access Layer

Production Planning System - **Data Source Connection and Data Fetching Layer**

This project provides only the data access layer: connecting to data sources, fetching data, and managing connection configuration.

## Project Structure

```
PSBot/
├── config/
│   └── settings.py          # Data source configuration (env vars)
├── connections/
│   ├── postgres.py          # PostgreSQL connection client
│   └── clickhouse.py        # ClickHouse connection client
├── connectors/
│   ├── base.py              # Base connector interface
│   ├── clickhouse_connector.py
│   ├── excel_connector.py
│   ├── sap_connector.py
│   └── connector_factory.py
├── repositories/
│   └── data_repository.py   # Unified data fetch interface
├── services/
│   ├── datasphere_service.py # SAP Datasphere OData API
│   ├── key_vault_service.py  # Azure Key Vault (SAP tokens)
│   └── odata_converter.py    # SQL to OData conversion
├── models/                   # Data models (empty, for future use)
├── utils/
│   └── file_utils.py         # Excel/CSV read helpers
├── shared/
│   └── exceptions.py        # Data access exceptions
├── index.py                  # Entry point
├── requirements.txt
├── .env.example
└── README.md
```

## Supported Data Sources

| Type | Description |
|------|-------------|
| **ClickHouse** | SQL via HTTP interface |
| **PostgreSQL** | SQL via psycopg2 |
| **Excel** | .xlsx/.xls via DuckDB |
| **CSV** | Via DuckDB with encoding detection |
| **SAP Datasphere** | OData API (Catalog + Consumption) |

## Usage

### Setup

```bash
pip install -r requirements.txt
cp .env.example .env
# Edit .env with your credentials
```

### ClickHouse

```python
import sys
sys.path.insert(0, 'src')
from connectors.connector_factory import ConnectorFactory

config = {
    "type": "clickhouse",
    "host": "localhost",
    "port": 8123,
    "database_name": "analytics",
    "username": "default",
    "password": ""
}
connector = ConnectorFactory.get_connector(config)
result = connector.fetch_data({"queries": ["SELECT * FROM my_table LIMIT 10"]})
# result: Dict[str, pd.DataFrame]
```

### Excel / CSV

```python
from connectors.connector_factory import ConnectorFactory

config = {"type": "excel", "file_path": "/path/to/file.xlsx"}
connector = ConnectorFactory.get_connector(config)
schema = connector.get_schema("Sheet1")
result = connector.fetch_data({"queries": ["SELECT * FROM Sheet1"]})
```

### SAP Datasphere

```python
import asyncio
from services.datasphere_service import get_datasphere_service

ds = get_datasphere_service()
assets = asyncio.run(ds.list_catalog_assets(user_id="user123"))
schema = asyncio.run(ds.get_view_schema(user_id="user123", view_name="MyView"))
result = asyncio.run(ds.execute_odata_query(
    user_id="user123",
    view_name="MyView",
    select="Col1,Col2",
    filter="Col1 eq 'value'",
    top=100
))
```

### Data Repository (Gateway)

```python
from repositories.data_repository import DataRepository

config = {"type": "clickhouse", "host": "localhost"}
repo = DataRepository(config)
tables = repo.list_tables()
schema = repo.get_schema(tables[0])
result = repo.fetch_data(["SELECT * FROM " + tables[0] + " LIMIT 10"])
repo.test_connection()
repo.close()
```

## Environment Variables

See `.env.example` for all configuration options. Key variables:

- `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT`, etc. - ClickHouse connection
- `POSTGRES_HOST`, etc. - PostgreSQL (for postgres data sources)
- `SAP_ODATA_URL`, `SAP_OAUTH_TOKEN_URL`, etc. - SAP Datasphere
- `AZURE_KEY_VAULT_URL` - SAP token storage (optional)

## Configuration

All settings are loaded from environment variables via `config.settings`. Use a `.env` file in the project root for local development.
