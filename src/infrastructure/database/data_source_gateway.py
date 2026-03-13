"""Unified Data Source Gateway for multiple database types."""
from typing import Dict, Any, Optional, List
import logging
import re
import pandas as pd
from pathlib import Path
import duckdb

from .postgres import PostgreSQLClient
from shared.exceptions import DatabaseException

logger = logging.getLogger(__name__)

# Import DatasphereService at module level to avoid import issues
# Use explicit import path to avoid "No module named 'src.services'" error
try:
    from src.infrastructure.services.datasphere_service import get_datasphere_service
except ImportError:
    # Fallback to relative import if absolute import fails
    # From database/ -> infrastructure/ -> services/
    try:
        from ..services.datasphere_service import get_datasphere_service
    except ImportError:
        # Fallback if import fails - will be handled in test_connection
        get_datasphere_service = None
        logger.warning("Failed to import DatasphereService - SAP Datasphere connection tests will fail")

_DATE_KEYWORDS = ("date", "dt", "time", "timestamp", "created", "posted", "updated", "on")

# NOTE: Excel/CSV normalization is handled by the LLM-powered `load_data` node.


_ISO_DATE_COMPARISON_PATTERN = re.compile(
    r'(?P<col>"[^"]+"|\b[a-zA-Z_][a-zA-Z0-9_]*\b)\s*'
    r'(?P<op>>=|<=|<>|!=|<|>|=)\s*'
    r"(?P<q>'|\")(?P<date>\d{4}-\d{2}-\d{2})(?P=q)",
    flags=re.IGNORECASE,
)

_NUMERIC_COMPARISON_PATTERN = re.compile(
    r'(?P<col>"[^"]+"|\b[a-zA-Z_][a-zA-Z0-9_]*\b)\s*'
    r'(?P<op>>=|<=|<>|!=|<|>|=)\s*'
    r'(?P<num>\d+)\b',
    flags=re.IGNORECASE,
)


def _normalize_dataframe_for_duckdb(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize DataFrame to prevent DuckDB type casting errors.
    
    DuckDB may fail when trying to cast mixed-type columns (e.g., numeric values
    mixed with strings like '*'). This function converts such columns to string type.
    
    The issue occurs when DuckDB infers a column type (e.g., INT32) based on most values,
    but then fails when encountering non-numeric strings like '*'. This function proactively
    converts any column with mixed numeric/non-numeric values to string type.
    
    Args:
        df: Input pandas DataFrame
        
    Returns:
        DataFrame with mixed-type columns converted to string
    """
    df = df.copy()
    
    for col in df.columns:
        # Skip date columns - they should remain as datetime
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            continue
        
        # Get non-null values for analysis
        non_null_values = df[col].dropna()
        if len(non_null_values) == 0:
            continue
        
        # Check if column is object type (string/mixed) - these are most likely to cause issues
        # DuckDB will try to infer types for object columns, which can cause casting errors
        if df[col].dtype == 'object':
            # For object columns, check if ANY value cannot be converted to a number
            # If so, convert entire column to string to prevent DuckDB from inferring numeric types
            has_non_numeric = False
            
            for val in non_null_values:
                if isinstance(val, str):
                    val_stripped = val.strip()
                    # Skip empty strings - they're fine
                    if not val_stripped:
                        continue
                    # Try to convert to float - if it fails, it's non-numeric
                    try:
                        float(val_stripped)
                    except (ValueError, TypeError):
                        # Found a non-numeric value (e.g., '*', 'N/A', etc.) - convert entire column to string
                        has_non_numeric = True
                        logger.debug(f"Found non-numeric value '{val}' in column '{col}' - will convert column to string")
                        break
                elif not isinstance(val, (int, float)):
                    # Non-numeric, non-string value
                    has_non_numeric = True
                    logger.debug(f"Found non-numeric value '{val}' (type: {type(val)}) in column '{col}' - will convert column to string")
                    break
            
            if has_non_numeric:
                logger.debug(
                    f"Converting object column '{col}' to explicit string type "
                    f"to prevent DuckDB casting errors (found non-numeric values like '*')"
                )
                try:
                    df[col] = df[col].astype(str)
                except Exception as e:
                    logger.warning(f"Could not convert column '{col}' to string: {e}")
        
        # For numeric columns (int, float), check if there are any string values that can't be cast
        # This shouldn't normally happen with pandas, but handle it just in case
        elif pd.api.types.is_numeric_dtype(df[col]):
            # Check if there are any non-numeric string values
            has_non_numeric = False
            for val in non_null_values:
                if isinstance(val, str):
                    val_stripped = val.strip()
                    try:
                        float(val_stripped)
                    except (ValueError, TypeError):
                        has_non_numeric = True
                        break
            
            if has_non_numeric:
                logger.debug(
                    f"Converting numeric column '{col}' to string type "
                    f"to prevent DuckDB casting errors (found non-numeric string values)"
                )
                try:
                    df[col] = df[col].astype(str)
                except Exception as e:
                    logger.warning(f"Could not convert column '{col}' to string: {e}")
    
    return df


def _enforce_safe_duckdb_date_comparisons(sql: str, date_columns: List[str]) -> str:
    """
    Schema-aware guardrail just before DuckDB execution.
    
    Since date columns are already normalized to datetime64[ns] in load_data node,
    DuckDB recognizes them as TIMESTAMP. DuckDB can compare TIMESTAMP directly with DATE literals.
    
    This function:
    - Rewrites: col >= 'YYYY-MM-DD'  -> col >= DATE 'YYYY-MM-DD'  (convert string to DATE literal)
    - Rejects:  col >= 20251201      (numeric comparisons on date/timestamp columns are unsafe)
    - Leaves:   col >= DATE 'YYYY-MM-DD'  (already correct, no change)
    - Leaves:   CAST(col AS DATE) >= DATE 'YYYY-MM-DD'  (also valid, no change)
    """
    if not sql or not date_columns:
        return sql

    date_cols_norm = {str(c).lower(): c for c in date_columns if isinstance(c, str)}

    # Reject numeric comparisons against known date columns
    for m in _NUMERIC_COMPARISON_PATTERN.finditer(sql):
        col = m.group("col")
        col_name = col.strip('"').strip().lower()
        if col_name in date_cols_norm:
            raise DatabaseException(
                f"Unsafe SQL: numeric comparison against date column '{date_cols_norm[col_name]}'. "
                f"Use DATE literals: {col} >= DATE 'YYYY-MM-DD'."
            )

    def repl(m: re.Match) -> str:
        col = m.group("col")
        op = m.group("op")
        date_str = m.group("date")

        col_name = col.strip('"').strip().lower()
        if col_name not in date_cols_norm:
            return m.group(0)

        # Check if already using DATE literal (not string literal)
        # The pattern matches string literals, so we need to convert to DATE literal
        # If it's already DATE '...', the pattern wouldn't match, so we're safe
        
        # Check if already inside CAST expression - leave it as-is
        start_pos = m.start()
        lookback_start = max(0, start_pos - 50)
        context_before = sql[lookback_start:start_pos]
        if "CAST(" in context_before.upper():
            # Already has CAST, leave it
            return m.group(0)

        # Convert string literal to DATE literal: col >= 'YYYY-MM-DD' -> col >= DATE 'YYYY-MM-DD'
        return f"{col} {op} DATE '{date_str}'"

    rewritten = _ISO_DATE_COMPARISON_PATTERN.sub(repl, sql)
    if rewritten != sql:
        logger.info("Rewrote date comparisons: string literals -> DATE literals (columns are already TIMESTAMP)")
        logger.debug(f"Original SQL (truncated): {sql[:300]}...")
        logger.debug(f"Rewritten SQL (truncated): {rewritten[:300]}...")
    return rewritten


def read_excel_with_engine(file_path: str, sheet_name: Optional[str] = None, engine: Optional[str] = None, **kwargs) -> pd.DataFrame:
    """
    Read Excel file with automatic engine detection (RAW DATA ONLY - no normalization).
    Supports both .xlsx (openpyxl) and .xls (xlrd) formats.
    
    NOTE: This function now only loads raw data. Normalization is handled by LLM in load_data_node.
    
    Args:
        file_path: Path to Excel file
        sheet_name: Optional sheet name to read (reads first sheet if not specified)
        engine: Optional engine to use ('openpyxl' or 'xlrd'). If not provided, auto-detects from file extension.
        **kwargs: Additional arguments to pass to pd.read_excel (engine will be removed if present)
        
    Returns:
        DataFrame with raw Excel data (no normalization applied)
        
    Raises:
        DatabaseException: If file cannot be read
    """
    # Remove engine from kwargs if present to avoid conflicts
    kwargs.pop('engine', None)
    
    file_ext = Path(file_path).suffix.lower()
    
    # Use provided engine or determine based on file extension
    if engine is None:
        if file_ext == '.xlsx':
            engine = 'openpyxl'
        elif file_ext == '.xls':
            engine = 'xlrd'
        else:
            # Try openpyxl first (most common), then xlrd
            engines = ['openpyxl', 'xlrd']
            last_error = None
            
            for eng in engines:
                try:
                    logger.debug(f"Attempting to read Excel with engine: {eng}")
                    df = pd.read_excel(file_path, sheet_name=sheet_name, engine=eng, **kwargs)
                    logger.info(f"Successfully read Excel file using {eng} engine (raw data)")
                    # Only basic cleanup: remove completely empty rows/columns
                    df = df.dropna(how='all').dropna(axis=1, how='all')
                    return df
                except Exception as e:
                    last_error = e
                    logger.debug(f"Failed to read Excel with {eng} engine: {str(e)}")
                    continue
            
            raise DatabaseException(
                f"Failed to read Excel file: Could not read with any engine. "
                f"Tried: {engines}. Last error: {str(last_error)}"
            )
    
    try:
        # Read with determined engine
        df = pd.read_excel(file_path, sheet_name=sheet_name, engine=engine, **kwargs)
        
        # Only basic cleanup: remove completely empty rows/columns
        df = df.dropna(how='all').dropna(axis=1, how='all')
        
        return df
    except Exception as e:
        raise DatabaseException(f"Failed to read Excel file with {engine} engine: {str(e)}") from e


def get_excel_file_engine(file_path: str) -> str:
    """
    Get the appropriate engine for an Excel file.
    
    Args:
        file_path: Path to Excel file
        
    Returns:
        Engine name ('openpyxl' or 'xlrd')
    """
    file_ext = Path(file_path).suffix.lower()
    
    if file_ext == '.xlsx':
        return 'openpyxl'
    elif file_ext == '.xls':
        return 'xlrd'
    else:
        # Default to openpyxl for unknown extensions
        return 'openpyxl'


def read_csv_with_encoding(file_path: str, **kwargs) -> pd.DataFrame:
    """
    Read CSV file with automatic encoding and delimiter detection (RAW DATA ONLY - no normalization).
    Supports multiple encodings, delimiters, and handles BOM markers.
    
    NOTE: This function now only loads raw data. Normalization is handled by LLM in load_data_node.
    
    Args:
        file_path: Path to CSV file
        **kwargs: Additional arguments to pass to pd.read_csv
        
    Returns:
        DataFrame with raw CSV data (no normalization applied)
        
    Raises:
        DatabaseException: If all encoding/delimiter attempts fail
    """
    import csv
    
    # Common delimiters to try (in order of likelihood)
    delimiters = [',', ';', '\t', '|', ' ']
    
    # Encodings to try (including BOM variants)
    encodings = ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252', 'iso-8859-1', 'utf-16']
    
    # If delimiter is explicitly provided, use it
    explicit_delimiter = kwargs.pop('sep', None) or kwargs.pop('delimiter', None)
    if explicit_delimiter:
        delimiters = [explicit_delimiter]
    
    last_error = None
    
    # Try each encoding with each delimiter
    for encoding in encodings:
        for delimiter in delimiters:
            try:
                logger.debug(f"Attempting to read CSV with encoding: {encoding}, delimiter: {repr(delimiter)}")
                
                # Read with current encoding and delimiter
                df = pd.read_csv(
                    file_path,
                    encoding=encoding,
                    sep=delimiter,
                    on_bad_lines='skip',  # Skip bad lines instead of failing
                    engine='python',  # Python engine handles more edge cases
                    **kwargs
                )
                
                # Validate that we got meaningful data
                if len(df.columns) > 1 or (len(df.columns) == 1 and len(df) > 0):
                    if encoding != 'utf-8' or delimiter != ',':
                        logger.info(
                            f"Successfully read CSV file using encoding={encoding}, "
                            f"delimiter={repr(delimiter)} (raw data)"
                        )
                    # Only basic cleanup: remove completely empty rows/columns
                    df = df.dropna(how='all').dropna(axis=1, how='all')
                    return df
                else:
                    # Single column might indicate wrong delimiter, try next
                    logger.debug(f"Single column detected, trying next delimiter")
                    continue
                    
            except UnicodeDecodeError as e:
                last_error = e
                logger.debug(f"Failed to read CSV with {encoding} encoding: {str(e)}")
                continue
            except (pd.errors.ParserError, csv.Error) as e:
                # Parser error might be due to wrong delimiter, try next
                logger.debug(f"Parser error with delimiter {repr(delimiter)}: {str(e)}")
                last_error = e
                continue
            except Exception as e:
                # For other errors, log and try next combination
                logger.debug(f"Error reading CSV: {str(e)}")
                last_error = e
                continue
    
    # If all combinations failed, try with pandas auto-detection as last resort
    try:
        logger.debug("Attempting pandas auto-detection for CSV")
        df = pd.read_csv(file_path, on_bad_lines='skip', engine='python', **kwargs)
        if len(df.columns) > 0:
            logger.info("Successfully read CSV using pandas auto-detection (raw data)")
            # Only basic cleanup: remove completely empty rows/columns
            df = df.dropna(how='all').dropna(axis=1, how='all')
            return df
    except Exception as e:
        last_error = e
    
    # If all attempts failed, raise with the last error
    raise DatabaseException(
        f"Failed to read CSV file: Could not decode with any encoding/delimiter combination. "
        f"Tried encodings: {encodings}, delimiters: {delimiters}. "
        f"Last error: {str(last_error)}"
    )


class DataSourceGateway:
    """
    Unified gateway for executing SQL queries across different data sources.
    
    Supports:
    - PostgreSQL
    - MySQL
    - ClickHouse
    - Excel (via DuckDB)
    - CSV (via DuckDB)
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the gateway with a data source configuration.
        
        Args:
            config: Dictionary containing:
                - type: Data source type (postgres, mysql, clickhouse, excel, csv)
                - host, port, username, password, database_name: For database sources
                - file_path: For Excel or CSV files
        """
        self.config = config
        self.type = config.get("type", "").lower()
        # Map sqlserver to mysql for backward compatibility
        # NOTE: 'sap' and 'sap_datasphere' are NOT mapped to mysql - they use API calls, not database connections
        if self.type == "sqlserver":
            logger.info(f"Mapping '{self.type}' type to 'mysql'")
            self.type = "mysql"
        self.client = None
        self._initialize_client()
    
    def _initialize_client(self):
        """Initialize the appropriate client based on data source type."""
        try:
            if self.type == "postgres":
                self.client = self._create_postgres_client()
            elif self.type == "mysql":
                self.client = self._create_mysql_client()
            elif self.type == "clickhouse":
                self.client = self._create_clickhouse_client()
            elif self.type == "excel":
                # Excel: Initialize DuckDB connection and cache for file data
                self.client = None
                self._duckdb_conn = None  # Will be initialized on first query
                self._excel_sheets_cached = {}  # Cache loaded sheets
                self._excel_file_path = self.config.get('file_path')
                self._duckdb_table_date_columns = {}  # table_name -> set(date cols)
            elif self.type == "csv":
                # CSV: Initialize DuckDB connection and cache for file data
                self.client = None
                self._duckdb_conn = None  # Will be initialized on first query
                self._csv_cached = False  # Track if CSV has been loaded
                self._csv_table_name = None  # Cached table name
                self._csv_file_path = self.config.get('file_path')
                self._duckdb_table_date_columns = {}  # table_name -> set(date cols)
            elif self.type in ("sap", "sap_datasphere"):
                # SAP Datasphere: Uses API calls, not database connections
                # This gateway should not be used for SAP Datasphere - use DatasphereService instead
                self.client = None
                logger.info(f"SAP Datasphere data source detected - use DatasphereService for API calls")
            else:
                raise DatabaseException(f"Unsupported data source type: {self.type}")
            
            logger.info(f"Initialized {self.type} data source gateway")
        except Exception as e:
            logger.error(f"Failed to initialize {self.type} client: {str(e)}")
            raise DatabaseException(f"Failed to initialize data source: {str(e)}") from e
    
    def _create_postgres_client(self):
        """Create PostgreSQL client."""
        try:
            import psycopg2
            # Import settings to get query timeout
            from config.settings import settings
            timeout = getattr(settings, 'query_timeout_seconds', 600)
            
            # Add connection timeout and keepalive settings to prevent connection drops
            connection_string = (
                f"host={self.config.get('host')} "
                f"port={self.config.get('port', 5432)} "
                f"dbname={self.config.get('database_name')} "
                f"user={self.config.get('username')} "
                f"password={self.config.get('password', '')} "
                f"connect_timeout={timeout} keepalives=1 keepalives_idle=30 keepalives_interval=10 keepalives_count=5"
            )
            return PostgreSQLClient(connection_string=connection_string)
        except Exception as e:
            raise DatabaseException(f"Failed to create PostgreSQL client: {str(e)}") from e
    
    def _create_mysql_client(self):
        """Create MySQL client using pymysql."""
        try:
            try:
                import pymysql
            except ImportError:
                raise DatabaseException("pymysql is required for MySQL connections. Install it with: pip install pymysql")
            
            from config.settings import settings
            timeout = getattr(settings, 'query_timeout_seconds', 600)
            
            host = self.config.get('host')
            port = self.config.get('port', 3306)
            database = self.config.get('database_name')
            username = self.config.get('username')
            password = self.config.get('password', '')
            
            # Create a wrapper class for MySQL client
            class MySQLClient:
                def __init__(self, host, port, database, username, password, timeout):
                    self.host = host
                    self.port = int(port) if port else 3306
                    self.database = database
                    self.username = username
                    self.password = password
                    self.timeout = timeout
                
                def _get_connection(self):
                    """Get a MySQL connection."""
                    try:
                        conn = pymysql.connect(
                            host=self.host,
                            port=self.port,
                            user=self.username,
                            password=self.password,
                            database=self.database,
                            connect_timeout=min(self.timeout, 30),  # Connection timeout max 30s
                            read_timeout=self.timeout,
                            write_timeout=self.timeout,
                            cursorclass=pymysql.cursors.DictCursor,
                            autocommit=True,  # Enable autocommit for better compatibility
                            charset='utf8mb4'  # Use utf8mb4 for full Unicode support
                        )
                        return conn
                    except pymysql.Error as e:
                        raise DatabaseException(f"Failed to connect to MySQL: {str(e)}") from e
                
                def _normalize_mysql_sql(self, sql: str) -> str:
                    """
                    Normalize SQL for MySQL by converting double quotes to backticks.
                    MySQL uses backticks (`) for identifiers, not double quotes (").
                    """
                    import re
                    # Pattern to match double-quoted identifiers: "identifier"
                    # This regex matches "identifier" but not string literals like "value"
                    # We look for patterns that are clearly identifiers (after FROM, SELECT, WHERE, etc.)
                    pattern = r'([\s,\(\)=<>!]|^)"([a-zA-Z_][a-zA-Z0-9_]*)"([\s,\(\)=<>!]|$)'
                    
                    def replace_quotes(match):
                        prefix = match.group(1)
                        identifier = match.group(2)
                        suffix = match.group(3)
                        return f'{prefix}`{identifier}`{suffix}'
                    
                    # Replace double-quoted identifiers with backticked identifiers
                    normalized = re.sub(pattern, replace_quotes, sql)
                    
                    # Also handle table names in FROM/JOIN clauses more aggressively
                    # Pattern: FROM "table" or JOIN "table"
                    normalized = re.sub(
                        r'\b(FROM|JOIN|INTO|UPDATE)\s+"([a-zA-Z_][a-zA-Z0-9_]*)"',
                        r'\1 `\2`',
                        normalized,
                        flags=re.IGNORECASE
                    )
                    
                    # Pattern: "table".column or "table"."column"
                    normalized = re.sub(
                        r'"([a-zA-Z_][a-zA-Z0-9_]*)"\s*\.\s*"([a-zA-Z_][a-zA-Z0-9_]*)"',
                        r'`\1`.`\2`',
                        normalized
                    )
                    
                    # Pattern: "table".column (without quotes on column)
                    normalized = re.sub(
                        r'"([a-zA-Z_][a-zA-Z0-9_]*)"\s*\.\s*([a-zA-Z_][a-zA-Z0-9_]*)',
                        r'`\1`.\2',
                        normalized
                    )
                    
                    if normalized != sql:
                        logger.debug(f"Normalized MySQL SQL: converted double quotes to backticks")
                    
                    return normalized
                
                async def execute_query(self, sql: str, parameters: Optional[Dict[str, Any]] = None):
                    """Execute SQL query and return results."""
                    conn = None
                    try:
                        # Normalize SQL for MySQL (convert double quotes to backticks)
                        sql = self._normalize_mysql_sql(sql)
                        
                        conn = self._get_connection()
                        with conn.cursor() as cursor:
                            # Execute query with parameters
                            if parameters:
                                # pymysql accepts tuple/list for %s placeholders or dict for %(name)s placeholders
                                if isinstance(parameters, dict):
                                    # Check if SQL uses named placeholders %(name)s
                                    if '%(' in sql and ')' in sql:
                                        # Use dict directly for named placeholders
                                        cursor.execute(sql, parameters)
                                    else:
                                        # Convert dict to tuple for positional %s placeholders
                                        cursor.execute(sql, tuple(parameters.values()))
                                elif isinstance(parameters, (tuple, list)):
                                    # Already in correct format
                                    cursor.execute(sql, parameters)
                                else:
                                    cursor.execute(sql, parameters)
                            else:
                                cursor.execute(sql)
                            
                            # Fetch all rows
                            rows = cursor.fetchall()
                            
                            # Get column names from cursor description
                            # DictCursor returns dicts, but we need column names for the response
                            if cursor.description:
                                columns = [desc[0] for desc in cursor.description]
                            else:
                                # No result set (e.g., INSERT, UPDATE, DELETE)
                                columns = []
                            
                            # Convert rows to list of dicts (already dicts due to DictCursor)
                            # Ensure all keys are strings and handle any encoding issues
                            data = []
                            if rows:
                                for row in rows:
                                    # DictCursor already returns dict, but ensure keys are strings
                                    clean_row = {}
                                    for key, value in row.items():
                                        # Handle both string and bytes keys
                                        clean_key = key.decode('utf-8') if isinstance(key, bytes) else str(key)
                                        # Handle datetime and other special types
                                        clean_row[clean_key] = value
                                    data.append(clean_row)
                            
                            return {
                                "columns": columns,
                                "data": data,
                                "row_count": int(len(data)),
                            }
                    except pymysql.Error as e:
                        error_msg = str(e)
                        logger.error(f"MySQL query execution failed: {error_msg}")
                        raise DatabaseException(f"MySQL query failed: {error_msg}") from e
                    except Exception as e:
                        error_msg = str(e)
                        logger.error(f"Unexpected error during MySQL query execution: {error_msg}")
                        raise DatabaseException(f"Query execution failed: {error_msg}") from e
                    finally:
                        if conn:
                            try:
                                conn.close()
                            except Exception:
                                pass  # Ignore errors during close
                
                async def list_tables(self):
                    """List all tables in the database."""
                    try:
                        result = await self.execute_query(
                            "SELECT TABLE_NAME FROM information_schema.tables WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'"
                        )
                        # Extract table names - handle both uppercase and lowercase column names
                        tables = []
                        for row in result.get('data', []):
                            # Try uppercase first (MySQL information_schema standard), then lowercase
                            table_name = row.get('TABLE_NAME') or row.get('table_name') or row.get('Table_name')
                            if table_name:
                                tables.append(str(table_name))
                        return tables
                    except Exception as e:
                        logger.error(f"Failed to list MySQL tables: {str(e)}")
                        raise DatabaseException(f"Failed to list tables: {str(e)}") from e
                
                async def get_table_schema(self, table_name: str):
                    """Get table schema."""
                    try:
                        result = await self.execute_query(
                            """
                            SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT
                            FROM information_schema.columns 
                            WHERE table_schema = DATABASE() AND table_name = %s
                            ORDER BY ordinal_position
                            """,
                            (table_name,)
                        )
                        schema_lines = [f"Table: {table_name}"]
                        for row in result.get('data', []):
                            # Handle both uppercase and lowercase column names
                            col_name = row.get('COLUMN_NAME') or row.get('column_name') or ''
                            data_type = row.get('DATA_TYPE') or row.get('data_type') or ''
                            col_type = row.get('COLUMN_TYPE') or row.get('column_type') or ''
                            is_nullable = row.get('IS_NULLABLE') or row.get('is_nullable') or ''
                            col_default = row.get('COLUMN_DEFAULT') or row.get('column_default')
                            
                            # Build column description
                            col_desc = f"  - {col_name}: {data_type}"
                            if col_type and col_type != data_type:
                                col_desc += f" ({col_type})"
                            if is_nullable:
                                col_desc += f" [{'NULL' if is_nullable == 'YES' else 'NOT NULL'}]"
                            if col_default is not None:
                                col_desc += f" DEFAULT {col_default}"
                            schema_lines.append(col_desc)
                        return "\n".join(schema_lines)
                    except Exception as e:
                        logger.error(f"Failed to get MySQL table schema for {table_name}: {str(e)}")
                        raise DatabaseException(f"Failed to get table schema: {str(e)}") from e
                
                def close(self):
                    """Close MySQL connection. Note: Connections are created per-query, so this is a no-op."""
                    # MySQL connections are created per-query and closed immediately after
                    # This method exists for interface compatibility
                    pass
            
            return MySQLClient(host, port, database, username, password, timeout)
        except DatabaseException:
            raise
        except Exception as e:
            raise DatabaseException(f"Failed to create MySQL client: {str(e)}") from e
    
    def _create_clickhouse_client(self):
        """Create ClickHouse client optimized for large-scale financial data."""
        try:
            from clickhouse_connect import get_client
            from .clickhouse import get_clickhouse_executor
            import asyncio
            
            # Clean and extract connection parameters
            host = self.config.get('host', 'localhost').strip()
            # Remove protocol prefixes
            if host.startswith("http://"):
                host = host[7:].strip()
            elif host.startswith("https://"):
                host = host[8:].strip()
            # Remove trailing slashes
            host = host.rstrip('/')
            
            port = self.config.get('port', 8123)
            database = self.config.get('database_name', 'default')
            username = self.config.get('username', 'default')
            password = self.config.get('password', '')
            
            # Import settings to get query timeout
            from config.settings import settings
            
            # Use query_timeout_seconds from settings (default 600 seconds = 10 minutes)
            # This allows large queries to complete without timing out
            timeout = getattr(settings, 'query_timeout_seconds', 600)
            
            # Create ClickHouse client directly with cleaned parameters
            # Enable compression for large data transfers
            client = get_client(
                host=host,
                port=port,
                database=database,
                username=username,
                password=password if password else None,
                interface="http",
                connect_timeout=timeout,
                send_receive_timeout=timeout,
                compress=True,  # Enable compression for large data transfers
            )
            
            # Wrap in a compatible class with non-blocking async support
            class ClickHouseClientWrapper:
                """ClickHouse client wrapper with non-blocking async execution.
                
                For large financial datasets (3M+ rows per query), this wrapper:
                - Runs queries in a thread pool to avoid blocking the event loop
                - Enables parallel query execution
                - Supports compression for faster data transfer
                """
                
                def __init__(self, ch_client, executor_getter):
                    self.client = ch_client
                    self._get_executor = executor_getter
                
                def _execute_sync(self, sql: str, parameters=None):
                    """Synchronous query execution (runs in thread pool)."""
                    result = self.client.query(sql, parameters=parameters)
                    row_count = len(result.result_rows)
                    
                    # Log for large result sets
                    if row_count > 100_000:
                        logger.info(f"ClickHouse query returned {row_count:,} rows")
                    
                    return {
                        "columns": result.column_names,
                        "data": result.result_rows,
                        "row_count": int(row_count),
                    }
                
                async def execute_query(self, sql: str, parameters=None):
                    """Execute query asynchronously (non-blocking).
                    
                    Runs the synchronous ClickHouse query in a thread pool
                    to prevent blocking the event loop during large data fetches.
                    """
                    loop = asyncio.get_event_loop()
                    executor = self._get_executor()
                    
                    result = await loop.run_in_executor(
                        executor,
                        self._execute_sync,
                        sql,
                        parameters
                    )
                    return result
                
                async def list_tables(self):
                    result = await self.execute_query("SHOW TABLES")
                    return [row[0] for row in result["data"]] if result["data"] else []
                
                async def get_table_schema(self, table_name: str):
                    result = await self.execute_query(f"DESCRIBE TABLE {table_name}")
                    schema_lines = [f"Table: {table_name}"]
                    for row in result["data"]:
                        if len(row) >= 2:
                            schema_lines.append(f"  - {row[0]}: {row[1]}")
                    return "\n".join(schema_lines)
                
                async def get_sample_data(self, table_name: str, limit: int = 3):
                    result = await self.execute_query(f"SELECT * FROM {table_name} LIMIT {limit}")
                    if not result["data"]:
                        return []
                    return [dict(zip(result["columns"], row)) for row in result["data"]]
                
                def query(self, sql: str):
                    """Synchronous query for compatibility (blocking)."""
                    return self.client.query(sql)
                
                def close(self):
                    try:
                        self.client.close()
                    except Exception:
                        pass
            
            return ClickHouseClientWrapper(client, get_clickhouse_executor)
        except Exception as e:
            error_msg = str(e)
            # Clean up error message
            if "405" in error_msg or "Not Allowed" in error_msg:
                error_msg = f"HTTP 405 Not Allowed - Check if ClickHouse HTTP interface is enabled and accessible at {host}:{port}"
            elif "Connection refused" in error_msg:
                error_msg = f"Connection refused to {host}:{port}. Check if ClickHouse server is running."
            raise DatabaseException(f"Failed to create ClickHouse client: {error_msg}") from e
    
    async def execute_sql(self, sql: str, parameters: Optional[Dict[str, Any]] = None, cached_dataframes: Optional[Dict[str, pd.DataFrame]] = None) -> Dict[str, Any]:
        """
        Execute SQL query and return results.
        
        Args:
            sql: SQL query string
            parameters: Optional query parameters
            cached_dataframes: Optional dict of pre-normalized DataFrames (for Excel/CSV sources)
            
        Returns:
            Dictionary with 'columns', 'data', and 'row_count'
        """
        try:
            # IMPORTANT: We do NOT convert CSV -> Excel. CSV sources are queried directly via DuckDB.
            if self.type == "excel":
                return await self._execute_excel_query(sql, cached_dataframes=cached_dataframes)
            elif self.type == "csv":
                return await self._execute_csv_query(sql, cached_dataframes=cached_dataframes)
            elif self.type in ["postgres", "mysql", "clickhouse"]:
                if hasattr(self.client, 'execute_query'):
                    # Check if the method is async or sync
                    import inspect
                    is_async = inspect.iscoroutinefunction(self.client.execute_query)
                    
                    if is_async:
                        result = await self.client.execute_query(sql, parameters)
                    else:
                        # For sync methods (like PostgreSQL), call without await
                        result = self.client.execute_query(sql, parameters)
                    
                    # Convert PostgreSQL result format to expected format if needed
                    if self.type == "postgres":
                        # PostgreSQL returns List[Dict[str, Any]], convert to expected format
                        if isinstance(result, list) and len(result) > 0:
                            # Extract columns from first row
                            columns = list(result[0].keys())
                            # Data is already in the right format (list of dicts)
                            data = result
                            return {
                                "columns": columns,
                                "data": data,
                                "row_count": int(len(data)),
                            }
                        else:
                            # Empty result
                            return {
                                "columns": [],
                                "data": [],
                                "row_count": int(0),
                            }
                    
                    # For other types (ClickHouse, MySQL), return as-is
                    return result
                else:
                    raise DatabaseException(f"Client for {self.type} does not support execute_query")
            else:
                raise DatabaseException(f"Unsupported data source type: {self.type}")
        except Exception as e:
            logger.error(f"SQL execution failed for {self.type}: {str(e)}")
            raise DatabaseException(f"Query execution failed: {str(e)}") from e
    
    async def _execute_excel_query(self, sql: str, cached_dataframes: Optional[Dict[str, pd.DataFrame]] = None) -> Dict[str, Any]:
        """
        Execute SQL query on Excel file using DuckDB.
        Supports multiple sheets - each sheet is loaded as a separate table.
        PERFORMANCE OPTIMIZED: Uses cached DataFrames if provided, otherwise caches loaded sheets.
        
        Args:
            sql: SQL query string (can reference sheet names as table names)
            cached_dataframes: Optional dict of pre-normalized DataFrames (table_name -> DataFrame)
            
        Returns:
            Dictionary with 'columns', 'data', and 'row_count'
        """
        try:
            # If cached DataFrames are provided, use them directly (no reloading)
            if cached_dataframes:
                logger.info(f"[DataSourceGateway] ✅ Using cached DataFrames for Excel query execution ({len(cached_dataframes)} tables)")
                
                # Log cached DataFrame details
                total_cached_rows = 0
                for table_name, df in cached_dataframes.items():
                    row_count = len(df)
                    col_count = len(df.columns)
                    total_cached_rows += row_count
                    logger.info(
                        f"[DataSourceGateway]   📋 Cached table '{table_name}': {row_count:,} rows, {col_count} columns"
                    )
                logger.info(f"[DataSourceGateway]   📊 Total cached data: {total_cached_rows:,} rows")
                
                # Initialize DuckDB connection if needed
                if not hasattr(self, '_duckdb_conn') or self._duckdb_conn is None:
                    logger.debug("[DataSourceGateway]   🔌 Initializing DuckDB connection...")
                    self._duckdb_conn = duckdb.connect()
                    self._duckdb_table_date_columns = {}
                
                # Register cached DataFrames with DuckDB
                logger.info("[DataSourceGateway]   📝 Registering cached DataFrames with DuckDB...")
                for table_name, df in cached_dataframes.items():
                    # Clean table name
                    clean_table_name = table_name.replace(' ', '_').replace('-', '_')
                    clean_table_name = re.sub(r'[^a-zA-Z0-9_]', '_', clean_table_name)
                    if clean_table_name and not clean_table_name[0].isalpha() and clean_table_name[0] != '_':
                        clean_table_name = '_' + clean_table_name
                    if not clean_table_name:
                        clean_table_name = f"sheet_{list(cached_dataframes.keys()).index(table_name) + 1}"
                    
                    # Normalize DataFrame to prevent DuckDB type casting errors (e.g., mixed numeric/string columns)
                    df_normalized = _normalize_dataframe_for_duckdb(df)
                    
                    # Register DataFrame in DuckDB
                    self._duckdb_conn.register(clean_table_name, df_normalized)
                    
                    # Cache date columns for schema-aware SQL validation
                    date_cols = [
                        c for c in df.columns
                        if pd.api.types.is_datetime64_any_dtype(df[c])
                    ]
                    self._duckdb_table_date_columns[clean_table_name] = set(date_cols)
                    logger.info(
                        f"[DataSourceGateway]   ✅ Registered '{table_name}' → DuckDB table '{clean_table_name}' "
                        f"({len(df):,} rows, {len(date_cols)} date column(s))"
                    )
                    if date_cols:
                        logger.debug(f"[DataSourceGateway]      Date columns: {', '.join(date_cols)}")
                
                # Map table names for SQL rewriting
                registered_tables = {name: re.sub(r'[^a-zA-Z0-9_]', '_', name.replace(' ', '_').replace('-', '_')) 
                                    for name in cached_dataframes.keys()}
                
                # Execute SQL with table name mapping
                logger.debug(f"[DataSourceGateway]   🔍 Original SQL: {sql[:200]}{'...' if len(sql) > 200 else ''}")
                modified_sql = sql
                for original_name, clean_name in registered_tables.items():
                    if original_name != clean_name:
                        logger.debug(
                            f"[DataSourceGateway]   🔄 Mapping table name: '{original_name}' → '{clean_name}'"
                        )
                    modified_sql = re.sub(
                        rf'\bFROM\s+{re.escape(original_name)}\b',
                        f'FROM {clean_name}',
                        modified_sql,
                        flags=re.IGNORECASE
                    )
                    modified_sql = re.sub(
                        rf'\bJOIN\s+{re.escape(original_name)}\b',
                        f'JOIN {clean_name}',
                        modified_sql,
                        flags=re.IGNORECASE
                    )
                
                # Schema-aware validation/rewrite for date comparisons
                date_cols_union: List[str] = []
                for cols in self._duckdb_table_date_columns.values():
                    if cols:
                        date_cols_union.extend(list(cols))
                
                if date_cols_union:
                    logger.debug(
                        f"[DataSourceGateway]   📅 Applying date comparison safety rules for columns: "
                        f"{', '.join(date_cols_union[:5])}{'...' if len(date_cols_union) > 5 else ''}"
                    )
                modified_sql = _enforce_safe_duckdb_date_comparisons(modified_sql, date_cols_union)
                
                if modified_sql != sql:
                    logger.debug(f"[DataSourceGateway]   ✏️  Modified SQL: {modified_sql[:200]}{'...' if len(modified_sql) > 200 else ''}")
                
                # Execute SQL query
                logger.info("[DataSourceGateway]   🚀 Executing SQL query on cached DataFrames via DuckDB...")
                try:
                    result = self._duckdb_conn.execute(modified_sql).fetchdf()
                    result_rows = len(result)
                    result_cols = len(result.columns)
                    logger.info(
                        f"[DataSourceGateway]   ✅ Query executed successfully: {result_rows:,} rows, {result_cols} columns"
                    )
                    
                    # Log filtering effectiveness
                    if total_cached_rows > 0 and result_rows < total_cached_rows:
                        reduction_pct = ((total_cached_rows - result_rows) / total_cached_rows) * 100
                        logger.info(
                            f"[DataSourceGateway]   📉 SQL filters applied: {reduction_pct:.1f}% of rows filtered out "
                            f"({total_cached_rows:,} → {result_rows:,} rows)"
                        )
                    elif result_rows == total_cached_rows:
                        logger.warning(
                            f"[DataSourceGateway]   ⚠️  No filtering applied - all {result_rows:,} rows returned "
                            f"(SQL query may not have WHERE clause)"
                        )
                except Exception as e:
                    logger.error(f"[DataSourceGateway]   ❌ Excel query execution failed: {str(e)}")
                    logger.debug(f"[DataSourceGateway]   Failed SQL: {modified_sql}")
                    raise DatabaseException(f"Excel query execution failed: {str(e)}") from e
                
                # Convert to list of dictionaries
                data = result.to_dict('records')
                columns = list(result.columns)
                
                logger.debug(f"[DataSourceGateway]   ✅ Converted result to dict format ({len(data)} records)")
                
                return {
                    "columns": columns,
                    "data": data,
                    "row_count": int(len(data)),
                }
            
            # Fallback to original behavior (load from file)
            file_path = getattr(self, '_excel_file_path', None) or self.config.get('file_path')
            if not file_path or not Path(file_path).exists():
                raise DatabaseException(f"Excel file not found: {file_path}")
            
            # Check if we have a cached connection with loaded sheets
            if not hasattr(self, '_duckdb_conn') or self._duckdb_conn is None:
                self._duckdb_conn = duckdb.connect()
                self._excel_sheets_cached = {}
                self._duckdb_table_date_columns = {}
            
            # Load sheets if not already cached
            if not self._excel_sheets_cached:
                logger.info(f"Loading Excel file into cache: {file_path}")
                
                # Get appropriate engine for the file
                engine = get_excel_file_engine(file_path)
                try:
                    xl_file = pd.ExcelFile(file_path, engine=engine)
                except Exception as e:
                    # Try alternative engine if first fails
                    alt_engine = 'xlrd' if engine == 'openpyxl' else 'openpyxl'
                    logger.debug(f"Failed with {engine} engine, trying {alt_engine}: {str(e)}")
                    try:
                        xl_file = pd.ExcelFile(file_path, engine=alt_engine)
                        engine = alt_engine
                    except Exception as e2:
                        raise DatabaseException(
                            f"Failed to open Excel file with both engines. "
                            f"openpyxl error: {str(e)}, {alt_engine} error: {str(e2)}"
                        ) from e2
                
                sheet_names = xl_file.sheet_names
                
                if not sheet_names:
                    raise DatabaseException("Excel file contains no sheets")
                
                # Also create a table name based on the file name (for CSV-converted files)
                file_based_table_name = Path(file_path).stem
                clean_file_table_name = file_based_table_name.replace(' ', '_').replace('-', '_')
                clean_file_table_name = re.sub(r'[^a-zA-Z0-9_]', '_', clean_file_table_name)
                if clean_file_table_name and not clean_file_table_name[0].isalpha() and clean_file_table_name[0] != '_':
                    clean_file_table_name = '_' + clean_file_table_name
                if not clean_file_table_name:
                    clean_file_table_name = 'excel_table'
                
                # Load all sheets as separate tables in DuckDB
                for sheet_name in sheet_names:
                    try:
                        df = read_excel_with_engine(file_path, sheet_name=sheet_name, engine=engine)
                        
                        # Skip empty sheets
                        if df.empty:
                            logger.warning(f"Sheet '{sheet_name}' is empty, skipping")
                            continue
                        
                        # Ensure column names are strings and properly encoded
                        # DuckDB can have issues with certain column name formats
                        df.columns = [str(col).strip() for col in df.columns]
                        
                        # Clean table name from sheet name
                        clean_table_name = sheet_name.replace(' ', '_').replace('-', '_')
                        clean_table_name = re.sub(r'[^a-zA-Z0-9_]', '_', clean_table_name)
                        if clean_table_name and not clean_table_name[0].isalpha() and clean_table_name[0] != '_':
                            clean_table_name = '_' + clean_table_name
                        if not clean_table_name:
                            clean_table_name = f"sheet_{sheet_names.index(sheet_name) + 1}"
                        
                        # Register DataFrame in DuckDB with sheet-based name
                        self._duckdb_conn.register(clean_table_name, df)
                        self._excel_sheets_cached[sheet_name] = clean_table_name
                        # Cache date columns for schema-aware SQL validation
                        date_cols = [
                            c for c in df.columns
                            if pd.api.types.is_datetime64_any_dtype(df[c])
                        ]
                        self._duckdb_table_date_columns[clean_table_name] = set(date_cols)
                        logger.debug(f"Cached Excel sheet '{sheet_name}' as table '{clean_table_name}' with {len(df.columns)} columns")
                        
                        # Also register with file-based name (for CSV-converted files)
                        # This allows queries using the original CSV file name to work
                        if len(sheet_names) == 1:  # Only for single-sheet files (typical for CSV conversions)
                            # Register the same DataFrame with file-based name
                            self._duckdb_conn.register(clean_file_table_name, df)
                            self._excel_sheets_cached[clean_file_table_name] = clean_file_table_name
                            self._duckdb_table_date_columns[clean_file_table_name] = set(date_cols)
                            logger.debug(f"Also registered file-based table name '{clean_file_table_name}' (same as '{clean_table_name}')")
                    except Exception as e:
                        logger.warning(f"Failed to load sheet '{sheet_name}': {str(e)}")
                        continue
                
                if not self._excel_sheets_cached:
                    raise DatabaseException("Failed to load any sheets from Excel file")
                logger.info(f"Excel file cached: {len(self._excel_sheets_cached)} sheets loaded")
            
            # Use cached sheet mappings
            registered_tables = self._excel_sheets_cached
            logger.debug(f"Registered Excel tables: {list(registered_tables.keys())}")
            logger.debug(f"Registered Excel table values: {list(registered_tables.values())}")
            
            # Execute SQL query - map sheet names and file-based names to table names
            modified_sql = sql
            logger.debug(f"Original SQL query: {sql[:200]}")
            
            # First, try to map file-based table names (for CSV-converted files)
            file_based_table_name = Path(file_path).stem
            clean_file_table_name = file_based_table_name.replace(' ', '_').replace('-', '_')
            clean_file_table_name = re.sub(r'[^a-zA-Z0-9_]', '_', clean_file_table_name)
            if clean_file_table_name and not clean_file_table_name[0].isalpha() and clean_file_table_name[0] != '_':
                clean_file_table_name = '_' + clean_file_table_name
            logger.debug(f"File-based table name: {clean_file_table_name}")
            
            # Check if file-based table name exists in registered tables (for CSV-converted files)
            if clean_file_table_name in registered_tables:
                # File-based name is already registered, use it directly
                actual_table_name = registered_tables[clean_file_table_name]
                # Replace file-based name references (handle quoted and unquoted, with various separators)
                # Pattern: FROM "table_name" or FROM table_name or FROM `table_name`
                patterns = [
                    (rf'\bFROM\s+["\'`]?{re.escape(clean_file_table_name)}["\'`]?\b', f'FROM {actual_table_name}'),
                    (rf'\bJOIN\s+["\'`]?{re.escape(clean_file_table_name)}["\'`]?\b', f'JOIN {actual_table_name}'),
                ]
                for pattern, replacement in patterns:
                    modified_sql = re.sub(pattern, replacement, modified_sql, flags=re.IGNORECASE)
            
            # Map sheet names to table names
            for original_name, clean_name in registered_tables.items():
                if original_name == clean_file_table_name:
                    continue  # Already handled above
                modified_sql = re.sub(
                    rf'\bFROM\s+{re.escape(original_name)}\b',
                    f'FROM {clean_name}',
                    modified_sql,
                    flags=re.IGNORECASE
                )
                modified_sql = re.sub(
                    rf'\bJOIN\s+{re.escape(original_name)}\b',
                    f'JOIN {clean_name}',
                    modified_sql,
                    flags=re.IGNORECASE
                )
            
            # If SQL doesn't reference any table, use the first sheet
            if 'FROM' not in modified_sql.upper() and 'JOIN' not in modified_sql.upper():
                first_table = list(registered_tables.values())[0]
                modified_sql = f"SELECT * FROM {first_table}"
            
            # Fallback: If the SQL still references a table that doesn't exist, try to find a match
            # This handles cases where table name in query doesn't exactly match registered names
            if 'FROM' in modified_sql.upper():
                # Extract table name from FROM clause
                from_match = re.search(r'\bFROM\s+["\'`]?([a-zA-Z0-9_]+)["\'`]?', modified_sql, re.IGNORECASE)
                if from_match:
                    table_in_query = from_match.group(1)
                    # Check if this table exists in registered tables
                    table_exists = False
                    for reg_name, reg_table in registered_tables.items():
                        if reg_table.lower() == table_in_query.lower() or reg_name.lower() == table_in_query.lower():
                            table_exists = True
                            break
                    
                    # If table doesn't exist, replace with first available table
                    if not table_exists and registered_tables:
                        first_table = list(registered_tables.values())[0]
                        modified_sql = re.sub(
                            rf'\bFROM\s+["\'`]?{re.escape(table_in_query)}["\'`]?',
                            f'FROM {first_table}',
                            modified_sql,
                            flags=re.IGNORECASE
                        )
                        logger.debug(f"Table '{table_in_query}' not found, using '{first_table}' instead")
            
            # Schema-aware validation/rewrite for date comparisons (DuckDB safety)
            date_cols_union: List[str] = []
            try:
                for cols in getattr(self, "_duckdb_table_date_columns", {}).values():
                    if cols:
                        date_cols_union.extend(list(cols))
            except Exception:
                date_cols_union = []
            modified_sql = _enforce_safe_duckdb_date_comparisons(modified_sql, date_cols_union)
            
            # Execute SQL query using cached connection
            try:
                result = self._duckdb_conn.execute(modified_sql).fetchdf()
            except Exception as e:
                logger.error(f"Excel query execution failed after date rewrite. SQL: {modified_sql[:500]}... Error: {str(e)}")
                raise DatabaseException(f"Excel query execution failed: {str(e)}") from e
            
            # Convert to list of dictionaries
            data = result.to_dict('records')
            columns = list(result.columns)
            
            return {
                "columns": columns,
                "data": data,
                "row_count": int(len(data)),
            }
        except Exception as e:
            logger.error(f"Excel query execution failed: {str(e)}")
            raise DatabaseException(f"Excel query execution failed: {str(e)}") from e
    
    async def _execute_csv_query(self, sql: str, cached_dataframes: Optional[Dict[str, pd.DataFrame]] = None) -> Dict[str, Any]:
        """
        Execute SQL query on CSV file using DuckDB.
        PERFORMANCE OPTIMIZED: Uses cached DataFrames if provided, otherwise caches loaded CSV.
        
        Args:
            sql: SQL query string (can reference the CSV file as a table)
            cached_dataframes: Optional dict of pre-normalized DataFrames (table_name -> DataFrame)
            
        Returns:
            Dictionary with 'columns', 'data', and 'row_count'
        """
        try:
            # If cached DataFrames are provided, use them directly (no reloading)
            if cached_dataframes:
                logger.info(f"[DataSourceGateway] ✅ Using cached DataFrames for CSV query execution ({len(cached_dataframes)} tables)")
                
                # Get first (and typically only) DataFrame
                table_name = list(cached_dataframes.keys())[0]
                df = cached_dataframes[table_name]
                cached_row_count = len(df)
                cached_col_count = len(df.columns)
                
                logger.info(
                    f"[DataSourceGateway]   📋 Cached table '{table_name}': {cached_row_count:,} rows, {cached_col_count} columns"
                )
                
                # Initialize DuckDB connection if needed
                if not hasattr(self, '_duckdb_conn') or self._duckdb_conn is None:
                    logger.debug("[DataSourceGateway]   🔌 Initializing DuckDB connection...")
                    self._duckdb_conn = duckdb.connect()
                    self._duckdb_table_date_columns = {}
                
                # Clean table name
                clean_table_name = table_name.replace(' ', '_').replace('-', '_')
                clean_table_name = re.sub(r'[^a-zA-Z0-9_]', '_', clean_table_name)
                if clean_table_name and not clean_table_name[0].isalpha() and clean_table_name[0] != '_':
                    clean_table_name = '_' + clean_table_name
                if not clean_table_name:
                    clean_table_name = 'csv_table'
                
                # Register DataFrame in DuckDB
                logger.info(f"[DataSourceGateway]   📝 Registering cached DataFrame with DuckDB...")
                self._duckdb_conn.register(clean_table_name, df)
                
                # Cache date columns for schema-aware SQL validation
                date_cols = [
                    c for c in df.columns
                    if pd.api.types.is_datetime64_any_dtype(df[c])
                ]
                self._duckdb_table_date_columns[clean_table_name] = set(date_cols)
                logger.info(
                    f"[DataSourceGateway]   ✅ Registered '{table_name}' → DuckDB table '{clean_table_name}' "
                    f"({cached_row_count:,} rows, {len(date_cols)} date column(s))"
                )
                if date_cols:
                    logger.debug(f"[DataSourceGateway]      Date columns: {', '.join(date_cols)}")
                
                # Execute SQL with table name mapping
                logger.debug(f"[DataSourceGateway]   🔍 Original SQL: {sql[:200]}{'...' if len(sql) > 200 else ''}")
                modified_sql = sql
                if 'FROM' not in modified_sql.upper() and 'JOIN' not in modified_sql.upper():
                    logger.debug(f"[DataSourceGateway]   ⚠️  No FROM clause found, adding: SELECT * FROM {clean_table_name}")
                    modified_sql = f"SELECT * FROM {clean_table_name}"
                else:
                    # Replace table references
                    if table_name != clean_table_name:
                        logger.debug(
                            f"[DataSourceGateway]   🔄 Mapping table name: '{table_name}' → '{clean_table_name}'"
                        )
                    # Fix: Properly handle quoted table names - match opening AND closing quotes together
                    # Pattern handles: FROM table, FROM "table", FROM 'table', FROM `table`
                    modified_sql = re.sub(
                        rf'\bFROM\s+([\'"`]){re.escape(table_name)}\1',
                        f'FROM {clean_table_name}',
                        modified_sql,
                        flags=re.IGNORECASE
                    )
                    # Also handle unquoted table names
                    modified_sql = re.sub(
                        rf'\bFROM\s+{re.escape(table_name)}\b(?![\'"`])',
                        f'FROM {clean_table_name}',
                        modified_sql,
                        flags=re.IGNORECASE
                    )
                
                # Schema-aware validation/rewrite for date comparisons
                date_cols_union: List[str] = []
                for cols in self._duckdb_table_date_columns.values():
                    if cols:
                        date_cols_union.extend(list(cols))
                
                if date_cols_union:
                    logger.debug(
                        f"[DataSourceGateway]   📅 Applying date comparison safety rules for columns: "
                        f"{', '.join(date_cols_union[:5])}{'...' if len(date_cols_union) > 5 else ''}"
                    )
                modified_sql = _enforce_safe_duckdb_date_comparisons(modified_sql, date_cols_union)
                
                if modified_sql != sql:
                    logger.debug(f"[DataSourceGateway]   ✏️  Modified SQL: {modified_sql[:200]}{'...' if len(modified_sql) > 200 else ''}")
                
                # Execute SQL query
                logger.info("[DataSourceGateway]   🚀 Executing SQL query on cached DataFrame via DuckDB...")
                logger.debug(f"[DataSourceGateway]   📝 Full SQL to execute: {modified_sql}")
                try:
                    result = self._duckdb_conn.execute(modified_sql).fetchdf()
                    result_rows = len(result)
                    result_cols = len(result.columns)
                    logger.info(
                        f"[DataSourceGateway]   ✅ Query executed successfully: {result_rows:,} rows, {result_cols} columns"
                    )
                    
                    # Log filtering effectiveness
                    if cached_row_count > 0 and result_rows < cached_row_count:
                        reduction_pct = ((cached_row_count - result_rows) / cached_row_count) * 100
                        logger.info(
                            f"[DataSourceGateway]   📉 SQL filters applied: {reduction_pct:.1f}% of rows filtered out "
                            f"({cached_row_count:,} → {result_rows:,} rows)"
                        )
                    elif result_rows == cached_row_count:
                        logger.warning(
                            f"[DataSourceGateway]   ⚠️  No filtering applied - all {result_rows:,} rows returned "
                            f"(SQL query may not have WHERE clause)"
                        )
                except Exception as e:
                    logger.error(f"[DataSourceGateway]   ❌ CSV query execution failed: {str(e)}")
                    logger.error(f"[DataSourceGateway]   📝 Full SQL that failed: {modified_sql}")
                    # Log column names from DataFrame for debugging
                    logger.debug(f"[DataSourceGateway]   📋 DataFrame columns: {list(df.columns)[:10]}{'...' if len(df.columns) > 10 else ''}")
                    raise DatabaseException(f"CSV query execution failed: {str(e)}") from e
                
                # Convert to list of dictionaries
                data = result.to_dict('records')
                columns = list(result.columns)
                
                logger.debug(f"[DataSourceGateway]   ✅ Converted result to dict format ({len(data)} records)")
                
                return {
                    "columns": columns,
                    "data": data,
                    "row_count": int(len(data)),
                }
            
            # Fallback to original behavior (load from file)
            file_path = getattr(self, '_csv_file_path', None) or self.config.get('file_path')
            if not file_path or not Path(file_path).exists():
                raise DatabaseException(f"CSV file not found: {file_path}")
            
            # Check if we have a cached connection
            if not hasattr(self, '_duckdb_conn') or self._duckdb_conn is None:
                self._duckdb_conn = duckdb.connect()
                self._csv_cached = False
                self._csv_table_name = None
                self._duckdb_table_date_columns = {}
            
            # Load CSV if not already cached
            if not getattr(self, '_csv_cached', False):
                logger.info(f"Loading CSV file into cache: {file_path}")
                try:
                    df = read_csv_with_encoding(file_path)
                    logger.info(f"Loaded CSV file with {len(df)} rows and {len(df.columns)} columns")
                    
                    # Log date-like columns for debugging
                    date_like_cols = [col for col in df.columns if any(
                        kw in col.lower() for kw in ['date', 'created', 'time', 'dt', 'on']
                    )]
                    for col in date_like_cols:
                        sample_vals = df[col].dropna().head(3).tolist()
                        col_dtype = df[col].dtype
                        logger.info(f"Date-like column '{col}': dtype={col_dtype}, samples={sample_vals}")
                        
                except Exception as e:
                    raise DatabaseException(f"Failed to read CSV file: {str(e)}")
                
                if df.empty:
                    raise DatabaseException("CSV file is empty")
                
                # Ensure column names are strings and properly encoded
                # DuckDB can have issues with certain column name formats
                df.columns = [str(col).strip() for col in df.columns]
                
                # Clean table name from file name
                table_name = Path(file_path).stem
                clean_table_name = table_name.replace(' ', '_').replace('-', '_')
                clean_table_name = re.sub(r'[^a-zA-Z0-9_]', '_', clean_table_name)
                if clean_table_name and not clean_table_name[0].isalpha() and clean_table_name[0] != '_':
                    clean_table_name = '_' + clean_table_name
                if not clean_table_name:
                    clean_table_name = 'csv_table'
                
                # Register DataFrame in DuckDB
                self._duckdb_conn.register(clean_table_name, df)
                self._csv_table_name = clean_table_name
                self._csv_cached = True
                date_cols = [
                    c for c in df.columns
                    if pd.api.types.is_datetime64_any_dtype(df[c])
                ]
                self._duckdb_table_date_columns[clean_table_name] = set(date_cols)
                logger.info(f"CSV file cached as table '{clean_table_name}'")
            
            clean_table_name = self._csv_table_name
            
            # Execute SQL query - map table references
            modified_sql = sql
            if 'FROM' not in modified_sql.upper() and 'JOIN' not in modified_sql.upper():
                modified_sql = f"SELECT * FROM {clean_table_name}"
            else:
                # Replace any table references with the cached table name
                table_name = Path(file_path).stem
                # Fix: Properly handle quoted table names - match opening AND closing quotes together
                modified_sql = re.sub(
                    rf'\bFROM\s+([\'"`]){re.escape(table_name)}\1',
                    f'FROM {clean_table_name}',
                    modified_sql,
                    flags=re.IGNORECASE
                )
                # Also handle unquoted table names
                modified_sql = re.sub(
                    rf'\bFROM\s+{re.escape(table_name)}\b(?![\'"`])',
                    f'FROM {clean_table_name}',
                    modified_sql,
                    flags=re.IGNORECASE
                )
            
            # Schema-aware validation/rewrite for date comparisons (DuckDB safety)
            date_cols_union: List[str] = []
            try:
                for cols in getattr(self, "_duckdb_table_date_columns", {}).values():
                    if cols:
                        date_cols_union.extend(list(cols))
            except Exception:
                date_cols_union = []
            modified_sql = _enforce_safe_duckdb_date_comparisons(modified_sql, date_cols_union)
            
            # Execute SQL query using cached connection
            try:
                result = self._duckdb_conn.execute(modified_sql).fetchdf()
            except Exception as e:
                logger.error(f"CSV query execution failed after date rewrite. SQL: {modified_sql[:500]}... Error: {str(e)}")
                raise DatabaseException(f"CSV query execution failed: {str(e)}") from e
            
            # Convert to list of dictionaries
            data = result.to_dict('records')
            columns = list(result.columns)
            
            logger.debug(f"CSV query executed successfully, returned {len(data)} rows")
            
            return {
                "columns": columns,
                "data": data,
                "row_count": int(len(data)),
            }
        except Exception as e:
            logger.error(f"CSV query execution failed: {str(e)}")
            raise DatabaseException(f"CSV query execution failed: {str(e)}") from e
    
    async def list_tables(self) -> List[str]:
        """
        List all tables in the data source.
        
        Returns:
            List of table names
        """
        try:
            if self.type == "excel":
                # For Excel, return sheet names or a default table name
                file_path = self.config.get('file_path')
                if file_path and Path(file_path).exists():
                    engine = get_excel_file_engine(file_path)
                    try:
                        xl_file = pd.ExcelFile(file_path, engine=engine)
                    except Exception:
                        # Try alternative engine
                        alt_engine = 'xlrd' if engine == 'openpyxl' else 'openpyxl'
                        xl_file = pd.ExcelFile(file_path, engine=alt_engine)
                    return xl_file.sheet_names
                return []
            elif self.type == "csv":
                # For CSV, return table name (file name without extension)
                file_path = self.config.get('file_path')
                if file_path and Path(file_path).exists():
                    table_name = Path(file_path).stem
                    # Clean table name
                    clean_table_name = table_name.replace(' ', '_').replace('-', '_')
                    clean_table_name = re.sub(r'[^a-zA-Z0-9_]', '_', clean_table_name)
                    if clean_table_name and not clean_table_name[0].isalpha() and clean_table_name[0] != '_':
                        clean_table_name = '_' + clean_table_name
                    if not clean_table_name:
                        clean_table_name = 'csv_table'
                    return [clean_table_name]
                return []
            elif self.type == "postgres":
                result = self.client.execute_query(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
                )
                return [row['table_name'] for row in result]
            elif self.type == "mysql":
                return await self.client.list_tables()
            elif self.type == "clickhouse":
                return await self.client.list_tables()
            elif self.type in ("sap", "sap_datasphere"):
                # SAP Datasphere uses API calls, not database connections
                # Use DatasphereService.list_catalog_assets() to get available views
                if get_datasphere_service is None:
                    raise DatabaseException(
                        "Failed to import DatasphereService. Please ensure all dependencies are installed."
                    )
                
                # Get user_id from config (required for SAP Datasphere)
                user_id = self.config.get("user_id")
                if not user_id:
                    raise DatabaseException(
                        "user_id is required in config for SAP Datasphere to list catalog assets."
                    )
                
                # Call DatasphereService to get catalog assets
                datasphere_service = get_datasphere_service()
                result = await datasphere_service.list_catalog_assets(user_id)
                
                # Return list of view names
                return result.view_names
            else:
                raise DatabaseException(f"Unsupported data source type: {self.type}")
        except Exception as e:
            logger.error(f"Failed to list tables for {self.type}: {str(e)}")
            raise DatabaseException(f"Failed to list tables: {str(e)}") from e
    
    async def get_table_schema(self, table_name: str) -> str:
        """
        Get formatted schema description for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Formatted schema string
        """
        try:
            if self.type == "excel":
                # For Excel, table_name is the sheet name
                # Read a sample to get accurate types after type detection
                file_path = self.config.get('file_path')
                if file_path and Path(file_path).exists():
                    engine = get_excel_file_engine(file_path)
                    try:
                        xl_file = pd.ExcelFile(file_path, engine=engine)
                    except Exception:
                        # Try alternative engine
                        alt_engine = 'xlrd' if engine == 'openpyxl' else 'openpyxl'
                        xl_file = pd.ExcelFile(file_path, engine=alt_engine)
                        engine = alt_engine
                    
                    def _build_excel_schema(df, table_display_name):
                        """Build schema lines with accurate type detection for date columns."""
                        schema_lines = [f"Table: {table_display_name} (Excel Sheet)"]
                        for col in df.columns:
                            dtype = str(df[col].dtype)
                            if pd.api.types.is_datetime64_any_dtype(df[col]):
                                dtype = "datetime"
                            elif pd.api.types.is_object_dtype(df[col]) or dtype == 'object':
                                # Ingested Excel/CSV is normalized; remaining object columns are treated as text.
                                pass
                            schema_lines.append(f"  - {col}: {dtype}")
                        return "\n".join(schema_lines)
                    
                    # Check if table_name matches a sheet name (exact or cleaned)
                    if table_name in xl_file.sheet_names:
                        df = read_excel_with_engine(file_path, sheet_name=table_name, engine=engine, nrows=100)
                        return _build_excel_schema(df, table_name)
                    else:
                        # Try to find by cleaned name
                        for sheet_name in xl_file.sheet_names:
                            clean_name = sheet_name.replace(' ', '_').replace('-', '_')
                            clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', clean_name)
                            if clean_name == table_name or sheet_name == table_name:
                                df = read_excel_with_engine(file_path, sheet_name=sheet_name, engine=engine, nrows=100)
                                return _build_excel_schema(df, sheet_name)
                return f"Table: {table_name}\n  (Sheet not found or file not accessible)"
            elif self.type == "csv":
                # For CSV, return column info with accurate types
                # Read a sample to trigger type detection (not just headers)
                file_path = self.config.get('file_path')
                if file_path and Path(file_path).exists():
                    # Read a small sample to get accurate types after type detection
                    df = read_csv_with_encoding(file_path, nrows=100)
                    schema_lines = [f"Table: {Path(file_path).stem} (CSV File)"]
                    for col in df.columns:
                        dtype = str(df[col].dtype)
                        if pd.api.types.is_datetime64_any_dtype(df[col]):
                            dtype = "datetime"
                        elif pd.api.types.is_object_dtype(df[col]) or dtype == 'object':
                            # Ingested Excel/CSV is normalized; remaining object columns are treated as text.
                            pass
                        schema_lines.append(f"  - {col}: {dtype}")
                    return "\n".join(schema_lines)
                return f"Table: {table_name}\n  (CSV file not found or not accessible)"
            elif self.type == "clickhouse":
                return await self.client.get_table_schema(table_name)
            elif self.type == "mysql":
                return await self.client.get_table_schema(table_name)
            elif self.type == "postgres":
                result = self.client.execute_query(
                    """
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = %s
                    """,
                    (table_name,)
                )
                schema_lines = [f"Table: {table_name}"]
                for row in result:
                    schema_lines.append(f"  - {row['column_name']}: {row['data_type']}")
                return "\n".join(schema_lines)
            else:
                raise DatabaseException(f"Unsupported data source type: {self.type}")
        except Exception as e:
            logger.error(f"Failed to get schema for {self.type}: {str(e)}")
            raise DatabaseException(f"Failed to get table schema: {str(e)}") from e
    
    async def test_connection(self) -> bool:
        """
        Test the data source connection.
        
        Returns:
            True if connection is successful
        """
        try:
            if self.type == "excel":
                file_path = self.config.get('file_path')
                return file_path and Path(file_path).exists()
            elif self.type == "csv":
                file_path = self.config.get('file_path')
                if file_path and Path(file_path).exists():
                    # Try to read the CSV to validate it
                    try:
                        read_csv_with_encoding(file_path, nrows=0)
                        return True
                    except Exception as e:
                        logger.warning(f"CSV file exists but cannot be read: {str(e)}")
                        return False
                return False
            elif self.type == "postgres":
                self.client.execute_query("SELECT 1")
                return True
            elif self.type == "mysql":
                await self.client.execute_query("SELECT 1")
                return True
            elif self.type == "clickhouse":
                if hasattr(self.client, 'execute_query'):
                    await self.client.execute_query("SELECT 1")
                    return True
                else:
                    # Fallback for synchronous clients
                    self.client.query("SELECT 1")
                    return True
            elif self.type in ("sap", "sap_datasphere"):
                # For SAP Datasphere, test connection by listing catalog assets
                # This verifies the base URL and authentication are working
                if get_datasphere_service is None:
                    raise DatabaseException(
                        "Failed to import DatasphereService. Please ensure all dependencies are installed."
                    )
                datasphere_service = get_datasphere_service()
                
                # Get user_id from config (should be set when gateway is created)
                user_id = self.config.get("user_id")
                if not user_id:
                    logger.warning("SAP Datasphere connection test: user_id not found in config")
                    raise DatabaseException("user_id is required for SAP Datasphere connection test")
                
                # Try to list catalog assets - if this succeeds (200), connection is good
                try:
                    result = await datasphere_service.list_catalog_assets(user_id)
                    # If we get here without exception, connection is successful (200 response)
                    logger.info(f"SAP Datasphere connection test successful - found {len(result.view_names)} assets")
                    return True
                except Exception as e:
                    logger.error(f"SAP Datasphere connection test failed: {str(e)}")
                    raise DatabaseException(f"SAP Datasphere connection test failed: {str(e)}") from e
            else:
                return False
        except DatabaseException as e:
            # Preserve actionable error messages for API callers (e.g., /datasource/test)
            logger.error(f"Connection test failed for {self.type}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Connection test failed for {self.type}: {str(e)}")
            raise DatabaseException(f"Connection test failed for {self.type}: {str(e)}") from e
    
    def close(self):
        """Close the data source connection."""
        if self.client and hasattr(self.client, 'close'):
            try:
                self.client.close()
            except Exception:
                pass
        
        # Close cached DuckDB connection for Excel/CSV sources
        if hasattr(self, '_duckdb_conn') and self._duckdb_conn:
            try:
                self._duckdb_conn.close()
                self._duckdb_conn = None
            except Exception:
                pass
        
        # Clear caches
        if hasattr(self, '_excel_sheets_cached'):
            self._excel_sheets_cached = {}
        if hasattr(self, '_csv_cached'):
            self._csv_cached = False
            self._csv_table_name = None

