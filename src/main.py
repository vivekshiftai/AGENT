"""FastAPI application entry point."""
import sys
import signal
import logging
from pathlib import Path

# Add parent directory to path to allow imports when running directly or via uvicorn
src_dir = Path(__file__).parent
parent_dir = src_dir.parent

# When running directly (python main.py), we need to add parent_dir to sys.path
# so that imports like "from src.infrastructure..." work
# When running as module (python -m src.main), Python handles this automatically
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

# Also add src_dir to sys.path for direct imports
if str(src_dir) not in sys.path:
    sys.path.insert(0, str(src_dir))

# When running directly, we need to make src a package by adding __init__.py handling
# This allows relative imports in other files to work
if __name__ == "__main__":
    # Mark src as a package for relative imports
    import importlib.util
    import os
    
    # Create a fake package structure for relative imports
    # This allows files to use "from ....infrastructure" when running directly
    src_init_path = src_dir / "__init__.py"
    if not src_init_path.exists():
        # Create empty __init__.py if it doesn't exist (it should exist)
        pass
    
    # Ensure Python recognizes src as a package
    if "src" not in sys.modules:
        import types
        src_module = types.ModuleType("src")
        src_module.__path__ = [str(src_dir)]
        sys.modules["src"] = src_module

# Global exception handler to prevent worker crashes
def handle_exception(exc_type, exc_value, exc_traceback):
    """Global exception handler to log unhandled exceptions."""
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    
    logger = logging.getLogger(__name__)
    logger.critical(
        f"Unhandled exception in worker process",
        exc_info=(exc_type, exc_value, exc_traceback)
    )

# Set global exception handler
sys.excepthook = handle_exception

# Handle SIGTERM gracefully (PM2 sends this on restart)
def signal_handler(signum, frame):
    """Handle termination signals gracefully."""
    logger = logging.getLogger(__name__)
    logger.info(f"Received signal {signum}, shutting down gracefully...")
    sys.exit(0)

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging
import asyncio

# Use absolute imports (works both when run directly and as module)
# Path setup above ensures parent directory is in path for absolute imports
try:
    # Try relative imports first (works when run as module via uvicorn: src.main:app)
    from .config.settings import settings
    from .shared.logger import configure_logging, get_logger
    from .api.v1.routes import query, health, datasource, llm_usage, node_timing, data_source_analysis, export, datasphere
    from .api.middleware.logging import LoggingMiddleware
except ImportError:
    # Fall back to absolute imports (works when run directly: python main.py)
    from config.settings import settings
    from shared.logger import configure_logging, get_logger
    from api.v1.routes import query, health, datasource, llm_usage, node_timing, data_source_analysis, export, datasphere
    from api.middleware.logging import LoggingMiddleware

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    # Startup
    try:
        configure_logging(settings.log_level)
        logger.info("=" * 50)
        logger.info("🚀 InsightForge Analytics API Starting...")
        
        # Test PostgreSQL connection (required for data source config storage)
        try:
            try:
                from .infrastructure.database.postgres_client_singleton import get_shared_postgres_client
            except ImportError:
                from infrastructure.database.postgres_client_singleton import get_shared_postgres_client
            postgres_client = get_shared_postgres_client(ensure_tables=True)
            
            # Test connection with a simple query
            test_result = postgres_client.execute_query("SELECT 1 as test")
            if test_result:
                logger.info("✅ PostgreSQL connected")
            
        except Exception as e:
            error_msg = str(e)[:100] + "..." if len(str(e)) > 100 else str(e)
            logger.error(f"❌ PostgreSQL failed: {error_msg}")
        
        logger.info("✅ API Ready | Port: 2345")
        logger.info("=" * 50)
        
    except Exception as e:
        logger.error(f"Startup error: {str(e)}")
    
    try:
        yield
    except asyncio.CancelledError:
        # Handle cancellation during hot-reload gracefully - don't log as error
        pass
    finally:
        # Shutdown - minimal logging and cleanup
        try:
            logger.info("🛑 InsightForge API shutting down...")

            # Clean up database connections for this process
            try:
                try:
                    from .infrastructure.database.postgres_client_singleton import cleanup_all_postgres_clients
                except ImportError:
                    from infrastructure.database.postgres_client_singleton import cleanup_all_postgres_clients
                cleanup_all_postgres_clients()
                logger.info("Database connections cleaned up")
            except Exception as e:
                logger.warning(f"Error during database cleanup: {str(e)}")

        except Exception:
            pass

# Create FastAPI app
app = FastAPI(
    title=settings.api_title,
    version=settings.api_version,
    lifespan=lifespan,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify actual origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Logging middleware: sets request/query ID in contextvars (from X-Request-Id/X-Query-Id or generated once)
# so every log line includes it for tracing; clears ID after each request
app.add_middleware(LoggingMiddleware)

# Include routers
app.include_router(query.router, prefix=settings.api_prefix)
app.include_router(health.router, prefix=settings.api_prefix)
app.include_router(datasource.router, prefix=settings.api_prefix)
app.include_router(llm_usage.router, prefix=settings.api_prefix)
app.include_router(node_timing.router, prefix=settings.api_prefix)
app.include_router(data_source_analysis.router, prefix=settings.api_prefix)
app.include_router(export.router, prefix=settings.api_prefix)
app.include_router(datasphere.router, prefix=settings.api_prefix)


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "name": settings.api_title,
        "version": settings.api_version,
        "status": "running",
    }

if __name__ == "__main__":
    # Note: This file uses relative imports and is designed to be run via uvicorn
    # Recommended: uvicorn src.main:app --reload
    # Alternative: python -m src.main (from analytics-backend directory)
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=settings.api_port,
        reload=settings.debug,
        timeout_keep_alive=0,  # Disable keep-alive timeout (0 = no timeout)
        timeout_graceful_shutdown=30,  # Graceful shutdown timeout
        ws_ping_interval=25,  # Send ping every 25 seconds (aligned with application heartbeat)
        ws_ping_timeout=3600,  # Wait 60 minutes for pong response (supports 60+ min queries)
    )
