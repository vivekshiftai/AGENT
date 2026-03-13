"""PostgreSQL database client for application data."""
from typing import Optional, Dict, Any, List
import logging
import time
import asyncio
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
from psycopg2 import OperationalError, InterfaceError
from config.settings import settings
from shared.exceptions import DatabaseException

logger = logging.getLogger(__name__)


class PostgreSQLClient:
    """PostgreSQL database client for application data storage."""
    
    def __init__(self, connection_string: Optional[str] = None, ensure_tables: bool = True):
        """
        Initialize PostgreSQL client.
        
        Args:
            connection_string: Optional PostgreSQL connection string.
                               If not provided, uses settings.
            ensure_tables: Whether to ensure tables exist on initialization.
                          Set to False if tables are already ensured (for performance).
        """
        self.connection_string = connection_string or self._build_connection_string()
        self.pool: Optional[SimpleConnectionPool] = None
        self._tables_ensured = False
        self._ensure_database_exists()
        self._initialize_pool()
        if ensure_tables:
            self._ensure_tables()
    
    def _ensure_database_exists(self) -> None:
        """Create the target database if it does not already exist.

        Connects to the default 'postgres' maintenance database to check
        and optionally CREATE DATABASE for the configured database name.
        """
        import os
        db_name = getattr(settings, 'postgres_database', None) or os.getenv('POSTGRES_DATABASE', 'insightforge')

        def _clean_host(host_str: str) -> str:
            if not host_str:
                return 'localhost'
            h = host_str.strip()
            if h.startswith('http://'):
                h = h[7:]
            elif h.startswith('https://'):
                h = h[8:]
            return h.rstrip('/')

        host = _clean_host(getattr(settings, 'postgres_host', None) or os.getenv('POSTGRES_HOST', 'localhost'))
        port = getattr(settings, 'postgres_port', 5432) or os.getenv('POSTGRES_PORT', '5432')
        user = getattr(settings, 'postgres_user', 'postgres') or os.getenv('POSTGRES_USER', 'postgres')
        password = getattr(settings, 'postgres_password', '') or os.getenv('POSTGRES_PASSWORD', '')

        maint_dsn = f"host={host} port={port} dbname=postgres user={user} password={password} connect_timeout=10"
        try:
            conn = psycopg2.connect(maint_dsn)
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
            exists = cur.fetchone()
            if not exists:
                cur.execute(f'CREATE DATABASE "{db_name}"')
                logger.info(f"Created PostgreSQL database '{db_name}'")
            else:
                logger.info(f"PostgreSQL database '{db_name}' already exists")
            cur.close()
            conn.close()
        except Exception as e:
            logger.warning(f"Could not auto-create database '{db_name}': {e} — assuming it already exists")

    def _build_connection_string(self) -> str:
        """Build PostgreSQL connection string from settings."""
        def clean_host(host_str: str) -> str:
            """Remove protocol prefixes and trailing slashes from host."""
            if not host_str:
                return 'localhost'
            host = host_str.strip()
            # Remove protocol prefixes
            if host.startswith('http://'):
                host = host[7:]
            elif host.startswith('https://'):
                host = host[8:]
            # Remove trailing slashes
            host = host.rstrip('/')
            return host
        
        # Check if we have PostgreSQL settings
        if hasattr(settings, 'postgres_host') and settings.postgres_host:
            host = clean_host(settings.postgres_host)
            port = getattr(settings, 'postgres_port', 5432)
            database = getattr(settings, 'postgres_database', 'insightforge')
            user = getattr(settings, 'postgres_user', 'postgres')
            password = getattr(settings, 'postgres_password', '')
            
            # Get query timeout from settings (already imported at module level)
            timeout = getattr(settings, 'query_timeout_seconds', 600)
            
            # Add connection timeout and keepalive settings to prevent connection drops
            return (f"host={host} port={port} dbname={database} user={user} password={password} "
                   f"connect_timeout={timeout} keepalives=1 keepalives_idle=30 keepalives_interval=10 keepalives_count=5")
        else:
            # Default to localhost with environment variables
            import os
            host = clean_host(os.getenv('POSTGRES_HOST', 'localhost'))
            port = os.getenv('POSTGRES_PORT', '5432')
            database = os.getenv('POSTGRES_DATABASE', 'insightforge')
            user = os.getenv('POSTGRES_USER', 'postgres')
            password = os.getenv('POSTGRES_PASSWORD', '')
            
            # Get query timeout from settings (already imported at module level)
            timeout = getattr(settings, 'query_timeout_seconds', 600)
            
            # Add connection timeout and keepalive settings to prevent connection drops
            return (f"host={host} port={port} dbname={database} user={user} password={password} "
                   f"connect_timeout={timeout} keepalives=1 keepalives_idle=30 keepalives_interval=10 keepalives_count=5")
    
    def _initialize_pool(self):
        """Initialize connection pool with increased size for concurrent requests."""
        try:
            # Get query timeout from settings (already imported at module level)
            timeout = getattr(settings, 'query_timeout_seconds', 600)
            
            # Increased pool size to handle multiple concurrent requests
            # minconn=5 ensures we always have some connections ready
            # maxconn=50 allows up to 50 concurrent database operations
            self.pool = SimpleConnectionPool(
                minconn=5,
                maxconn=50,
                dsn=self.connection_string,
                connect_timeout=timeout  # Use query_timeout_seconds from settings (default 600 seconds = 10 minutes)
            )
            logger.info(f"PostgreSQL connection pool initialized: minconn=5, maxconn=50 (supports up to 50 concurrent requests)")
        except Exception as e:
            logger.error(f"PostgreSQL pool init failed: {str(e)[:100]}")
            raise DatabaseException(f"Failed to connect to PostgreSQL: {str(e)}") from e
    
    def _is_connection_valid(self, conn, quick_check: bool = False) -> bool:
        """Check if a connection is still valid.
        
        Args:
            conn: Connection to check
            quick_check: If True, only check closed status without querying
        """
        try:
            if conn.closed:
                return False
            
            # Quick check: just verify connection isn't closed
            if quick_check:
                return True
            
            # Full check: try a simple query (only if quick_check is False)
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.close()
            return True
        except (OperationalError, InterfaceError, AttributeError):
            return False
        except Exception:
            return False
    
    def _get_valid_connection(self, retries: int = 2, validate: bool = False):
        """Get a valid connection from the pool, retrying if needed.
        
        Args:
            retries: Number of retry attempts
            validate: If True, validate connection with a query (slower but safer)
                     If False, only check closed status (faster)
        """
        for attempt in range(retries):
            try:
                conn = self.get_connection()
                # Use quick check by default for better performance
                if self._is_connection_valid(conn, quick_check=not validate):
                    return conn
                else:
                    # Connection is stale, close it and try again
                    try:
                        self.pool.putconn(conn, close=True)
                    except Exception:
                        pass
                    if attempt < retries - 1:
                        time.sleep(0.1)  # Shorter delay
            except Exception as e:
                if attempt < retries - 1:
                    time.sleep(0.1)
        
        # If all retries failed, try to recreate the pool
        logger.warning("All connection attempts failed, recreating pool...")
        try:
            # Force close the existing pool completely
            if self.pool:
                try:
                    self.pool.closeall()
                except Exception:
                    pass  # Ignore errors when closing already closed pool
                self.pool = None

            # Create fresh pool
            self._initialize_pool()
            conn = self.get_connection()
            if self._is_connection_valid(conn, quick_check=True):
                return conn
        except Exception as e:
            logger.error(f"Failed to recreate connection pool: {str(e)}")
            # Reset pool reference on failure
            self.pool = None
        
        raise DatabaseException("Failed to get a valid database connection after retries")
    
    def _ensure_tables(self):
        """Ensure required tables exist. Only runs once per instance."""
        if self._tables_ensured:
            return
        
        try:
            conn = self._get_valid_connection()
            cur = None
            try:
                cur = conn.cursor()
                
                # Create data_source_config table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS data_source_config (
                        id SERIAL PRIMARY KEY,
                        user_id VARCHAR(255) NOT NULL,
                        name VARCHAR(255) NOT NULL,
                        type VARCHAR(50) NOT NULL,
                        host VARCHAR(255),
                        port INTEGER,
                        username VARCHAR(255),
                        password VARCHAR(255),
                        database_name VARCHAR(255),
                        file_path TEXT,
                        is_active BOOLEAN DEFAULT FALSE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(user_id, name)
                    )
                """)
                
                # Create index on user_id and is_active for faster lookups
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_data_source_user_active 
                    ON data_source_config(user_id, is_active)
                """)
                
                # Create partial index for active data sources (more efficient for common query)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_data_source_user_active_partial 
                    ON data_source_config(user_id) 
                    WHERE is_active = TRUE
                """)
                
                # Create llm_usage table if it doesn't exist
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS llm_usage (
                        id SERIAL PRIMARY KEY,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        query_id VARCHAR(255),
                        query_text TEXT,
                        node_name VARCHAR(255),
                        provider VARCHAR(100),
                        model VARCHAR(255),
                        input_tokens INTEGER DEFAULT 0,
                        output_tokens INTEGER DEFAULT 0,
                        total_tokens INTEGER DEFAULT 0,
                        config JSONB
                    )
                """)
                
                # Add query_text column if it doesn't exist (migration)
                cur.execute("""
                    DO $$ 
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.columns 
                            WHERE table_name='llm_usage' AND column_name='query_text'
                        ) THEN
                            ALTER TABLE llm_usage ADD COLUMN query_text TEXT;
                        END IF;
                    END $$;
                """)
                
                # Create indexes for llm_usage table
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_llm_usage_created_at 
                    ON llm_usage(created_at DESC)
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_llm_usage_query_id 
                    ON llm_usage(query_id)
                """)
                
                # Create node_timing table if it doesn't exist
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS node_timing (
                        id SERIAL PRIMARY KEY,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        query_id VARCHAR(255),
                        query_text TEXT,
                        node_name VARCHAR(255),
                        duration_seconds NUMERIC(10, 3) DEFAULT 0,
                        pipeline VARCHAR(50),
                        status VARCHAR(50) DEFAULT 'completed',
                        metadata JSONB
                    )
                """)
                
                # Add query_text column if it doesn't exist (migration)
                cur.execute("""
                    DO $$ 
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.columns 
                            WHERE table_name='node_timing' AND column_name='query_text'
                        ) THEN
                            ALTER TABLE node_timing ADD COLUMN query_text TEXT;
                        END IF;
                    END $$;
                """)
                
                # Create indexes for node_timing table
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_node_timing_created_at 
                    ON node_timing(created_at DESC)
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_node_timing_query_id 
                    ON node_timing(query_id)
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_node_timing_node_name 
                    ON node_timing(node_name)
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_node_timing_pipeline 
                    ON node_timing(pipeline)
                """)
                
                # Create data_source_analysis table for tracking analysis status
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS data_source_analysis (
                        id SERIAL PRIMARY KEY,
                        data_source_id INTEGER NOT NULL,
                        user_id VARCHAR(255) NOT NULL,
                        description TEXT,
                        status VARCHAR(50) DEFAULT 'pending',
                        progress_percent INTEGER DEFAULT 0,
                        current_table VARCHAR(255),
                        total_tables INTEGER DEFAULT 0,
                        processed_tables INTEGER DEFAULT 0,
                        error_message TEXT,
                        started_at TIMESTAMP,
                        completed_at TIMESTAMP,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (data_source_id) REFERENCES data_source_config(id) ON DELETE CASCADE
                    )
                """)
                
                # Create indexes for data_source_analysis table
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_data_source_analysis_data_source_id 
                    ON data_source_analysis(data_source_id)
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_data_source_analysis_status 
                    ON data_source_analysis(status)
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_data_source_analysis_user_id 
                    ON data_source_analysis(user_id)
                """)
                
                # Create column_descriptions table for storing LLM-generated column descriptions
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS column_descriptions (
                        id SERIAL PRIMARY KEY,
                        data_source_id INTEGER NOT NULL,
                        analysis_id INTEGER NOT NULL,
                        table_name VARCHAR(255) NOT NULL,
                        column_name VARCHAR(255) NOT NULL,
                        data_type VARCHAR(100),
                        unique_values JSONB,
                        description TEXT,
                        usage_suggestions TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (data_source_id) REFERENCES data_source_config(id) ON DELETE CASCADE,
                        FOREIGN KEY (analysis_id) REFERENCES data_source_analysis(id) ON DELETE CASCADE,
                        UNIQUE(analysis_id, table_name, column_name)
                    )
                """)
                
                # Create indexes for column_descriptions table
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_column_descriptions_analysis_id 
                    ON column_descriptions(analysis_id)
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_column_descriptions_data_source_id 
                    ON column_descriptions(data_source_id)
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_column_descriptions_table_name 
                    ON column_descriptions(table_name)
                """)
                
                conn.commit()
                self._tables_ensured = True
                logger.debug("PostgreSQL tables ensured")
            finally:
                if cur:
                    cur.close()
                self.put_connection(conn)
        except Exception as e:
            logger.error(f"Failed to ensure tables: {str(e)}")
            raise DatabaseException(f"Failed to create tables: {str(e)}") from e
    
    def get_connection(self):
        """Get a connection from the pool."""
        if not self.pool:
            raise DatabaseException("Connection pool not initialized")
        return self.pool.getconn()
    
    def put_connection(self, conn):
        """Return a connection to the pool."""
        if self.pool:
            self.pool.putconn(conn)
    
    def execute_query(self, sql: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """
        Execute a SELECT query and return results.
        
        Args:
            sql: SQL query string
            params: Optional query parameters
            
        Returns:
            List of dictionaries representing rows
        """
        conn = None
        cur = None
        try:
            retries = 1  # Reduced retries for better performance
            for attempt in range(retries):
                try:
                    # Use quick check for better performance (no SELECT 1 query)
                    conn = self._get_valid_connection(validate=False)
                    cur = conn.cursor(cursor_factory=RealDictCursor)
                    cur.execute(sql, params)
                    results = cur.fetchall()
                    return [dict(row) for row in results]
                except (OperationalError, InterfaceError) as e:
                    # On connection error, retry with validation
                    if conn:
                        try:
                            if cur:
                                cur.close()
                            self.pool.putconn(conn, close=True)
                        except Exception:
                            pass
                        conn = None
                    
                    if attempt < retries:
                        # Retry with validation on connection error
                        try:
                            conn = self._get_valid_connection(validate=True)
                            cur = conn.cursor(cursor_factory=RealDictCursor)
                            cur.execute(sql, params)
                            results = cur.fetchall()
                            return [dict(row) for row in results]
                        except Exception as retry_e:
                            logger.error(f"Query execution failed after retry: {str(retry_e)}")
                            if conn:
                                try:
                                    if cur:
                                        cur.close()
                                    self.pool.putconn(conn, close=True)
                                except Exception:
                                    pass
                            raise DatabaseException(f"Query execution failed: {str(retry_e)}") from retry_e
                    else:
                        logger.error(f"Query execution failed: {str(e)}")
                        raise DatabaseException(f"Query execution failed: {str(e)}") from e
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            if conn:
                try:
                    if cur:
                        cur.close()
                    self.put_connection(conn)
                except Exception:
                    pass
            raise DatabaseException(f"Query execution failed: {str(e)}") from e
        finally:
            if conn and cur:
                try:
                    cur.close()
                    self.put_connection(conn)
                except Exception:
                    # If we can't return the connection, close it
                    try:
                        conn.close()
                    except Exception:
                        pass
        
        raise DatabaseException("Query execution failed: Unable to get valid connection")
    
    def execute_update(self, sql: str, params: Optional[tuple] = None) -> int:
        """
        Execute an INSERT/UPDATE/DELETE query.
        
        Args:
            sql: SQL query string
            params: Optional query parameters
            
        Returns:
            Number of affected rows
            
        Raises:
            DatabaseException: For general database errors
            psycopg2.errors.IntegrityError: For constraint violations (preserved for specific handling)
        """
        conn = None
        cur = None
        retries = 1  # Reduced retries for better performance
        for attempt in range(retries):
            try:
                # Use quick check for better performance
                conn = self._get_valid_connection(validate=False)
                cur = conn.cursor()
                cur.execute(sql, params)
                conn.commit()
                return cur.rowcount
            except psycopg2.errors.IntegrityError as e:
                # Preserve integrity errors (unique constraints, foreign keys, etc.) for specific handling
                if conn:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                if cur:
                    try:
                        cur.close()
                    except Exception:
                        pass
                if conn:
                    try:
                        self.put_connection(conn)
                    except Exception:
                        pass
                logger.error(f"Integrity constraint violation: {str(e)}")
                raise  # Re-raise the original exception
            except (OperationalError, InterfaceError) as e:
                # On connection error, retry with validation
                if conn:
                    try:
                        conn.rollback()
                        if cur:
                            cur.close()
                        self.pool.putconn(conn, close=True)
                    except Exception:
                        pass
                    conn = None
                
                if attempt < retries:
                    # Retry with validation on connection error
                    try:
                        conn = self._get_valid_connection(validate=True)
                        cur = conn.cursor()
                        cur.execute(sql, params)
                        conn.commit()
                        return cur.rowcount
                    except Exception as retry_e:
                        logger.error(f"Update execution failed after retry: {str(retry_e)}")
                        if conn:
                            try:
                                conn.rollback()
                                if cur:
                                    cur.close()
                                self.pool.putconn(conn, close=True)
                            except Exception:
                                pass
                        raise DatabaseException(f"Update execution failed: {str(retry_e)}") from retry_e
                else:
                    logger.error(f"Update execution failed: {str(e)}")
                    raise DatabaseException(f"Update execution failed: {str(e)}") from e
            except Exception as e:
                if conn:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                if cur:
                    try:
                        cur.close()
                    except Exception:
                        pass
                if conn:
                    try:
                        self.put_connection(conn)
                    except Exception:
                        pass
                logger.error(f"Update execution failed: {str(e)}")
                raise DatabaseException(f"Update execution failed: {str(e)}") from e
        
        raise DatabaseException("Update execution failed: Unable to get valid connection")
    
    async def execute_query_async(self, sql: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """
        Execute a SELECT query asynchronously (non-blocking).
        
        This method wraps the synchronous execute_query in asyncio.to_thread()
        to prevent blocking the event loop, allowing multiple concurrent requests.
        
        Args:
            sql: SQL query string
            params: Optional query parameters
            
        Returns:
            List of dictionaries representing rows
        """
        logger.debug(f"[DB] Executing async query (non-blocking): {sql[:100]}{'...' if len(sql) > 100 else ''}")
        try:
            # Run the synchronous execute_query in a thread pool to avoid blocking
            result = await asyncio.to_thread(self.execute_query, sql, params)
            logger.debug(f"[DB] Async query completed successfully")
            return result
        except Exception as e:
            logger.error(f"[DB] Async query execution failed: {str(e)}")
            raise
    
    async def execute_update_async(self, sql: str, params: Optional[tuple] = None) -> int:
        """
        Execute an INSERT/UPDATE/DELETE query asynchronously (non-blocking).
        
        This method wraps the synchronous execute_update in asyncio.to_thread()
        to prevent blocking the event loop, allowing multiple concurrent requests.
        
        Args:
            sql: SQL query string
            params: Optional query parameters
            
        Returns:
            Number of affected rows
        """
        logger.debug(f"[DB] Executing async update (non-blocking): {sql[:100]}{'...' if len(sql) > 100 else ''}")
        try:
            # Run the synchronous execute_update in a thread pool to avoid blocking
            result = await asyncio.to_thread(self.execute_update, sql, params)
            logger.debug(f"[DB] Async update completed successfully, affected rows: {result}")
            return result
        except Exception as e:
            logger.error(f"[DB] Async update execution failed: {str(e)}")
            raise
    
    def close(self):
        """Close the connection pool."""
        if self.pool:
            self.pool.closeall()
            logger.debug("PostgreSQL connection pool closed")

