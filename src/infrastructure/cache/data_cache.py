"""Query cache for large dataset exports.

Stores the SQL queries used to fetch data, so we can re-run them
when users download full data. This is much more efficient than
storing the actual data (gigabytes of Parquet files).

Flow:
1. fetch_data.py executes SQL queries → saves queries to cache (tiny - just text)
2. export.py retrieves the query from cache → re-runs it for download
3. No data storage overhead - just re-execute the same query

This approach:
- Uses minimal storage (a few KB per query vs GB for data)
- Ensures fresh data on download (re-queries database)
- Works with any data source (ClickHouse, PostgreSQL, etc.)
"""
import os
import logging
import json
import hashlib
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from threading import Lock

import pandas as pd

from config.settings import settings

logger = logging.getLogger(__name__)

# Thread lock for cache operations
_cache_lock = Lock()


class QueryCache:
    """
    Cache for SQL queries used to fetch data.
    
    Instead of caching actual data (which uses GB of storage),
    we cache the SQL queries and re-run them when user downloads.
    
    Features:
    - Minimal storage (a few KB per query)
    - Re-runs query for fresh data on download
    - TTL-based expiration
    - Thread-safe operations
    """
    
    def __init__(self):
        """Initialize the query cache."""
        self.enabled = getattr(settings, 'data_cache_enabled', True)
        self.cache_dir = Path(getattr(settings, 'data_cache_dir', 'query_cache'))
        self.ttl_minutes = getattr(settings, 'data_cache_ttl_minutes', 60)
        
        if self.enabled:
            self._ensure_cache_dir()
            logger.info(f"QueryCache initialized: {self.cache_dir} (TTL: {self.ttl_minutes}min)")
    
    def _ensure_cache_dir(self):
        """Ensure cache directory exists."""
        try:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            logger.warning(f"Failed to create cache directory: {e}")
            self.enabled = False
    
    def _generate_cache_key(self, query_id: str) -> str:
        """Generate a unique cache key for a query_id."""
        # Use hash to handle long query_ids
        key_hash = hashlib.md5(query_id.encode()).hexdigest()[:16]
        return f"query_{key_hash}"
    
    def _get_cache_path(self, cache_key: str) -> Path:
        """Get the file path for a cache key."""
        return self.cache_dir / f"{cache_key}.json"
    
    def save_queries(
        self, 
        query_id: str,
        table_queries: Dict[str, str],
        data_source_config: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Save SQL queries for a query session.
        
        Args:
            query_id: Unique query identifier
            table_queries: Dict of table_name -> SQL query
            data_source_config: Data source configuration (type, host, etc.)
            metadata: Optional metadata (selected_tables, etc.)
            
        Returns:
            True if saved successfully
        """
        if not self.enabled or not table_queries:
            return False
        
        cache_key = self._generate_cache_key(query_id)
        cache_path = self._get_cache_path(cache_key)
        
        try:
            with _cache_lock:
                cache_data = {
                    "query_id": query_id,
                    "table_queries": table_queries,
                    "data_source_config": {
                        # Store only non-sensitive config for reconnection
                        "type": data_source_config.get("type"),
                        "host": data_source_config.get("host"),
                        "port": data_source_config.get("port"),
                        "database_name": data_source_config.get("database_name"),
                        "file_path": data_source_config.get("file_path"),
                        # Note: username/password retrieved from active data source on export
                    },
                    "created_at": datetime.now().isoformat(),
                    "expires_at": (datetime.now() + timedelta(minutes=self.ttl_minutes)).isoformat(),
                    **(metadata or {})
                }
                
                with open(cache_path, 'w') as f:
                    json.dump(cache_data, f, indent=2)
                
                logger.info(f"📦 [QueryCache] Saved {len(table_queries)} queries for query_id={query_id[:8]}...")
                return True
                
        except Exception as e:
            logger.warning(f"📦 [QueryCache] Failed to save queries: {e}")
            return False
    
    def get_query(self, query_id: str, table_name: str) -> Optional[str]:
        """
        Get the SQL query for a specific table.
        
        Args:
            query_id: Unique query identifier
            table_name: Name of the table
            
        Returns:
            SQL query string if found and valid, None otherwise
        """
        cache_data = self.load(query_id)
        if not cache_data:
            return None
        
        table_queries = cache_data.get("table_queries", {})
        return table_queries.get(table_name)

    def save_sap_api_urls(
        self,
        query_id: str,
        sap_api_urls: Dict[str, str],
        data_source_config: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Save SAP OData API URLs per table for later export (re-fetch and stream).
        Merges into existing cache entry if present so SQL and SAP can coexist.
        
        Args:
            query_id: Unique query identifier
            sap_api_urls: Dict of table_name (dataset_key) -> full OData API URL
            data_source_config: Optional data source config for the cache entry
            
        Returns:
            True if saved successfully
        """
        if not self.enabled or not sap_api_urls:
            return False
        cache_key = self._generate_cache_key(query_id)
        cache_path = self._get_cache_path(cache_key)
        try:
            with _cache_lock:
                cache_data = self.load(query_id) or {
                    "query_id": query_id,
                    "table_queries": {},
                    "created_at": datetime.now().isoformat(),
                    "expires_at": (datetime.now() + timedelta(minutes=self.ttl_minutes)).isoformat(),
                }
                cache_data["sap_api_urls"] = sap_api_urls
                if data_source_config:
                    cache_data["data_source_config"] = {
                        "type": data_source_config.get("type"),
                        "host": data_source_config.get("host"),
                        "port": data_source_config.get("port"),
                        "database_name": data_source_config.get("database_name"),
                        "file_path": data_source_config.get("file_path"),
                    }
                cache_data["expires_at"] = (datetime.now() + timedelta(minutes=self.ttl_minutes)).isoformat()
                with open(cache_path, "w") as f:
                    json.dump(cache_data, f, indent=2)
                logger.info(f"📦 [QueryCache] Saved {len(sap_api_urls)} SAP API URL(s) for query_id={query_id[:8]}...")
                return True
        except Exception as e:
            logger.warning(f"📦 [QueryCache] Failed to save SAP API URLs: {e}")
            return False

    def get_sap_api_url(self, query_id: str, table_name: str) -> Optional[str]:
        """
        Get the stored SAP OData API URL for a table (for export re-fetch).
        Tries table_name as-is, then base view name if table_name contains __by__.
        """
        cache_data = self.load(query_id)
        if not cache_data:
            return None
        urls = cache_data.get("sap_api_urls", {})
        if table_name in urls:
            return urls[table_name]
        if "__by__" in table_name:
            base = table_name.split("__by__")[0]
            if base in urls:
                return urls[base]
        return None

    def load(self, query_id: str) -> Optional[Dict[str, Any]]:
        """
        Load cached query data.
        
        Args:
            query_id: Unique query identifier
            
        Returns:
            Cache data dict if found and valid, None otherwise
        """
        if not self.enabled:
            return None
        
        cache_key = self._generate_cache_key(query_id)
        cache_path = self._get_cache_path(cache_key)
        
        try:
            if not cache_path.exists():
                return None
            
            with open(cache_path, 'r') as f:
                cache_data = json.load(f)
            
            # Check expiration
            expires_at = datetime.fromisoformat(cache_data.get("expires_at", "2000-01-01"))
            if datetime.now() > expires_at:
                logger.debug(f"📦 [QueryCache] Expired: {query_id[:8]}...")
                self._delete_cache_file(cache_key)
                return None
            
            return cache_data
            
        except Exception as e:
            logger.warning(f"📦 [QueryCache] Failed to load: {e}")
            return None
    
    def exists(self, query_id: str) -> bool:
        """Check if a valid cache entry exists."""
        return self.load(query_id) is not None
    
    def _delete_cache_file(self, cache_key: str):
        """Delete cache file for a key."""
        try:
            cache_path = self._get_cache_path(cache_key)
            if cache_path.exists():
                cache_path.unlink()
        except Exception as e:
            logger.warning(f"📦 [QueryCache] Failed to delete {cache_key}: {e}")
    
    def clear(self):
        """Clear all cached queries."""
        try:
            if self.cache_dir.exists():
                import shutil
                shutil.rmtree(self.cache_dir)
                self._ensure_cache_dir()
                logger.info("📦 [QueryCache] Cleared all cached queries")
        except Exception as e:
            logger.warning(f"📦 [QueryCache] Failed to clear cache: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        try:
            cache_files = list(self.cache_dir.glob("*.json"))
            total_size = sum(f.stat().st_size for f in cache_files)
            
            return {
                "enabled": self.enabled,
                "cache_dir": str(self.cache_dir),
                "file_count": len(cache_files),
                "total_size_kb": total_size / 1024,
                "ttl_minutes": self.ttl_minutes,
            }
        except Exception as e:
            return {"enabled": self.enabled, "error": str(e)}


# Singleton instance
_query_cache: Optional[QueryCache] = None


def get_query_cache() -> QueryCache:
    """Get the shared QueryCache instance."""
    global _query_cache
    if _query_cache is None:
        _query_cache = QueryCache()
    return _query_cache


# Backwards compatibility alias
DataCache = QueryCache
get_data_cache = get_query_cache


class CleanedDataCache:
    """
    Persistent storage for cleaned/normalized Excel/CSV files.
    
    Creates cleaned versions of files with predictable names (e.g., filename_cleaned.xlsx).
    After first cleaning, all subsequent queries use the cleaned file instead of the original.
    
    Features:
    - Creates cleaned files in the same directory as original files
    - Uses predictable naming: original_name_cleaned.extension
    - Thread-safe operations
    - Automatically uses cleaned file if it exists
    """
    
    def __init__(self):
        """Initialize the cleaned data storage."""
        self.enabled = getattr(settings, 'cleaned_data_cache_enabled', True)
        
        if self.enabled:
            logger.info("CleanedDataCache initialized - will create cleaned files with _cleaned suffix")
    
    def _get_cleaned_file_path(self, original_file_path: str) -> Path:
        """
        Get the path for the cleaned version of a file.
        
        Args:
            original_file_path: Path to the original file
            
        Returns:
            Path to the cleaned file (e.g., original_file_cleaned.xlsx)
        """
        original_path = Path(original_file_path)
        # Create cleaned filename: original_name_cleaned.extension
        cleaned_name = f"{original_path.stem}_cleaned{original_path.suffix}"
        return original_path.parent / cleaned_name
    
    def get_cleaned_file_path(self, original_file_path: str) -> Optional[str]:
        """
        Get the path to cleaned file if it exists.
        
        Args:
            original_file_path: Path to the original file
            
        Returns:
            Path to cleaned file if it exists, None otherwise
        """
        if not self.enabled:
            return None
        
        cleaned_path = self._get_cleaned_file_path(original_file_path)
        
        if cleaned_path.exists():
            logger.info(
                f"💾 [CleanedDataCache] ✅ Found cleaned file: {cleaned_path.name} "
                f"(original: {Path(original_file_path).name})"
            )
            return str(cleaned_path)
        
        return None
    
    def save_cleaned_excel(
        self,
        original_file_path: str,
        dataframes: Dict[str, pd.DataFrame],
        engine: str = 'openpyxl'
    ) -> Optional[str]:
        """
        Save cleaned DataFrames as a new Excel file with _cleaned suffix.
        
        Args:
            original_file_path: Path to the original Excel file
            dataframes: Dictionary of sheet_name -> cleaned DataFrame
            engine: Excel engine to use (default: openpyxl)
            
        Returns:
            Path to the saved cleaned file if successful, None otherwise
        """
        if not self.enabled or not dataframes:
            return None
        
        cleaned_path = self._get_cleaned_file_path(original_file_path)
        
        try:
            with _cache_lock:
                # Save all sheets to cleaned Excel file
                with pd.ExcelWriter(cleaned_path, engine=engine) as writer:
                    for sheet_name, df in dataframes.items():
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                
                logger.info(
                    f"💾 [CleanedDataCache] ✅ Saved cleaned Excel file: {cleaned_path.name} "
                    f"({len(dataframes)} sheet(s), original: {Path(original_file_path).name})"
                )
                return str(cleaned_path)
                
        except Exception as e:
            logger.warning(f"💾 [CleanedDataCache] Failed to save cleaned Excel file: {e}")
            return None
    
    def save_cleaned_csv(
        self,
        original_file_path: str,
        df: pd.DataFrame
    ) -> Optional[str]:
        """
        Save cleaned DataFrame as a new CSV file with _cleaned suffix.
        
        Args:
            original_file_path: Path to the original CSV file
            df: Cleaned DataFrame
            
        Returns:
            Path to the saved cleaned file if successful, None otherwise
        """
        if not self.enabled or df.empty:
            return None
        
        cleaned_path = self._get_cleaned_file_path(original_file_path)
        
        try:
            with _cache_lock:
                # Save DataFrame to cleaned CSV file
                df.to_csv(cleaned_path, index=False, encoding='utf-8')
                
                logger.info(
                    f"💾 [CleanedDataCache] ✅ Saved cleaned CSV file: {cleaned_path.name} "
                    f"({len(df):,} rows, {len(df.columns)} columns, original: {Path(original_file_path).name})"
                )
                return str(cleaned_path)
                
        except Exception as e:
            logger.warning(f"💾 [CleanedDataCache] Failed to save cleaned CSV file: {e}")
            return None
    
    def delete_cleaned_file(self, original_file_path: str) -> bool:
        """
        Delete the cleaned file for a given original file.
        
        Args:
            original_file_path: Path to the original file
            
        Returns:
            True if deleted successfully, False otherwise
        """
        if not self.enabled:
            return False
        
        cleaned_path = self._get_cleaned_file_path(original_file_path)
        
        try:
            if cleaned_path.exists():
                with _cache_lock:
                    cleaned_path.unlink()
                    logger.info(f"💾 [CleanedDataCache] Deleted cleaned file: {cleaned_path.name}")
                    return True
        except Exception as e:
            logger.warning(f"💾 [CleanedDataCache] Failed to delete cleaned file: {e}")
        
        return False
    
    def clear_all(self) -> bool:
        """Clear all cleaned files (finds all files with _cleaned suffix)."""
        try:
            # This would need to know where to look - for now, return False
            # Users can manually delete _cleaned files or we can add directory scanning
            logger.warning("💾 [CleanedDataCache] clear_all() not implemented - cleaned files are stored alongside originals")
            return False
        except Exception as e:
            logger.warning(f"💾 [CleanedDataCache] Failed to clear all: {e}")
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """Get storage statistics."""
        return {
            "enabled": self.enabled,
            "note": "Cleaned files are stored alongside original files with _cleaned suffix"
        }


# Singleton instance
_cleaned_data_cache: Optional[CleanedDataCache] = None


def get_cleaned_data_cache() -> CleanedDataCache:
    """Get the shared CleanedDataCache instance."""
    global _cleaned_data_cache
    if _cleaned_data_cache is None:
        _cleaned_data_cache = CleanedDataCache()
    return _cleaned_data_cache

