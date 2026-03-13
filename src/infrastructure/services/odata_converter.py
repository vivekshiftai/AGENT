"""SQL to OData parameter converter for SAP Datasphere Consumption API."""
import logging
import re
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class ODataParams:
    """Container for OData query parameters."""
    select: Optional[str] = None
    filter: Optional[str] = None
    top: Optional[int] = None
    skip: Optional[int] = None
    orderby: Optional[str] = None
    count: bool = False
    format: Optional[str] = None  # $format parameter (e.g., "json")
    apply: Optional[str] = None  # $apply parameter for aggregation (e.g., "groupby((DateCol), aggregate(Amount with sum as TotalAmount))")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for URL query parameters.
        
        IMPORTANT: Preserves all parameters including filters - does NOT remove empty filters.
        The LLM provides filters, and we pass them through as-is.
        """
        params = {}
        # Always include $format if specified
        if self.format:
            params["$format"] = self.format
        if self.select:
            params["$select"] = self.select
        # CRITICAL: Include filter if provided (even if empty string - let LLM decide)
        # Do NOT filter out filters - the LLM generates them and we must preserve them
        if self.filter is not None and self.filter != "":
            params["$filter"] = self.filter
        if self.top is not None:
            params["$top"] = self.top
        if self.skip is not None:
            params["$skip"] = self.skip
        if self.orderby:
            params["$orderby"] = self.orderby
        if self.count:
            params["$count"] = "true"
        if self.apply:
            params["$apply"] = self.apply
        return params
    
    def to_query_string(self) -> str:
        """Convert to URL query string."""
        params = self.to_dict()
        if not params:
            return ""
        return "&".join(f"{k}={v}" for k, v in params.items())


class SQLToODataConverter:
    """
    Converts SQL query clauses to OData parameters.
    
    Supported conversions:
    - SELECT columns → $select
    - WHERE conditions → $filter
    - LIMIT n → $top
    - OFFSET n → $skip
    - ORDER BY → $orderby
    
    Note: This converter handles basic SQL patterns. Complex queries with
    subqueries, JOINs, or advanced functions may require manual adjustment.
    
    IMPORTANT: For SAP Datasphere, the LLM generates OData filters directly
    with proper type-aware quoting. This converter is used as a fallback
    for basic SQL-to-OData conversion.
    """
    
    # SQL to OData operator mapping
    OPERATOR_MAP = {
        "=": "eq",
        "!=": "ne",
        "<>": "ne",
        ">": "gt",
        ">=": "ge",
        "<": "lt",
        "<=": "le",
        "LIKE": "contains",  # Simplified - actual LIKE patterns need special handling
    }
    
    # SQL logical operators to OData
    LOGICAL_MAP = {
        "AND": "and",
        "OR": "or",
        "NOT": "not",
    }
    
    def __init__(self):
        """Initialize the converter."""
        pass
    
    def convert(self, sql_query: str) -> Tuple[str, ODataParams]:
        """
        Convert a SQL query to OData parameters.
        
        Args:
            sql_query: SQL query string (SELECT statement)
            
        Returns:
            Tuple of (table_name, ODataParams)
            
        Raises:
            ValueError: If the SQL query cannot be parsed
        """
        logger.info(f"Converting SQL to OData: {sql_query[:100]}...")
        
        # Normalize the query
        sql_query = self._normalize_sql(sql_query)
        
        # Extract table name
        table_name = self._extract_table_name(sql_query)
        if not table_name:
            raise ValueError("Could not extract table name from SQL query")
        
        # Build OData parameters
        params = ODataParams()
        
        # Extract SELECT columns
        params.select = self._extract_select(sql_query)
        
        # Extract WHERE clause
        params.filter = self._extract_where(sql_query)
        
        # Extract LIMIT
        params.top = self._extract_limit(sql_query)
        
        # Extract OFFSET
        params.skip = self._extract_offset(sql_query)
        
        # Extract ORDER BY
        params.orderby = self._extract_orderby(sql_query)
        
        logger.info(f"Converted to OData - Table: {table_name}, Params: {params.to_dict()}")
        return table_name, params
    
    def _normalize_sql(self, sql: str) -> str:
        """Normalize SQL query for parsing."""
        # Remove extra whitespace
        sql = " ".join(sql.split())
        # Ensure consistent casing for keywords
        return sql.strip()
    
    def _extract_table_name(self, sql: str) -> Optional[str]:
        """Extract table name from FROM clause."""
        # Pattern: FROM table_name or FROM schema.table_name
        pattern = r'\bFROM\s+(["\']?[\w.]+["\']?)'
        match = re.search(pattern, sql, re.IGNORECASE)
        if match:
            table_name = match.group(1)
            # Remove quotes if present
            table_name = table_name.strip('"\'')
            return table_name
        return None
    
    def _extract_select(self, sql: str) -> Optional[str]:
        """Extract SELECT columns and convert to $select."""
        # Pattern: SELECT columns FROM
        pattern = r'\bSELECT\s+(.*?)\s+FROM\b'
        match = re.search(pattern, sql, re.IGNORECASE)
        if match:
            columns = match.group(1).strip()
            # Handle SELECT *
            if columns == "*":
                return None  # No $select needed for all columns
            
            # Parse column list
            columns = self._parse_column_list(columns)
            if columns:
                return ",".join(columns)
        return None
    
    def _parse_column_list(self, columns_str: str) -> List[str]:
        """Parse comma-separated column list."""
        columns = []
        # Split by comma, but handle quoted identifiers
        parts = re.split(r',\s*(?=(?:[^"]*"[^"]*")*[^"]*$)', columns_str)
        
        for part in parts:
            part = part.strip()
            if not part:
                continue
            
            # Handle column aliases (col AS alias)
            alias_match = re.match(r'(.+?)\s+AS\s+(\w+)', part, re.IGNORECASE)
            if alias_match:
                # Use the original column name for OData
                part = alias_match.group(1).strip()
            
            # Remove quotes and clean up
            part = part.strip('"\'`')
            
            # Skip aggregate functions for now (they need special handling in OData)
            if re.match(r'^\w+\s*\(', part):
                logger.warning(f"Aggregate function '{part}' may not be directly supported in OData $select")
            
            columns.append(part)
        
        return columns
    
    def _extract_where(self, sql: str) -> Optional[str]:
        """Extract WHERE clause and convert to $filter."""
        # Pattern: WHERE conditions (until ORDER BY, LIMIT, OFFSET, GROUP BY, or end)
        pattern = r'\bWHERE\s+(.*?)(?=\s+(?:ORDER\s+BY|LIMIT|OFFSET|GROUP\s+BY)|$)'
        match = re.search(pattern, sql, re.IGNORECASE)
        if match:
            where_clause = match.group(1).strip()
            return self._convert_where_to_filter(where_clause)
        return None
    
    def _convert_where_to_filter(self, where_clause: str) -> str:
        """Convert SQL WHERE clause to OData $filter syntax."""
        filter_str = where_clause
        
        # Convert comparison operators
        # Handle = (but not ==)
        filter_str = re.sub(r"(\w+)\s*=\s*'([^']*)'", r"\1 eq '\2'", filter_str)
        filter_str = re.sub(r"(\w+)\s*=\s*(\d+(?:\.\d+)?)", r"\1 eq \2", filter_str)
        
        # Handle != and <>
        filter_str = re.sub(r"(\w+)\s*(?:!=|<>)\s*'([^']*)'", r"\1 ne '\2'", filter_str)
        filter_str = re.sub(r"(\w+)\s*(?:!=|<>)\s*(\d+(?:\.\d+)?)", r"\1 ne \2", filter_str)
        
        # Handle >, >=, <, <=
        filter_str = re.sub(r"(\w+)\s*>=\s*'([^']*)'", r"\1 ge '\2'", filter_str)
        filter_str = re.sub(r"(\w+)\s*>=\s*(\d+(?:\.\d+)?)", r"\1 ge \2", filter_str)
        filter_str = re.sub(r"(\w+)\s*<=\s*'([^']*)'", r"\1 le '\2'", filter_str)
        filter_str = re.sub(r"(\w+)\s*<=\s*(\d+(?:\.\d+)?)", r"\1 le \2", filter_str)
        filter_str = re.sub(r"(\w+)\s*>\s*'([^']*)'", r"\1 gt '\2'", filter_str)
        filter_str = re.sub(r"(\w+)\s*>\s*(\d+(?:\.\d+)?)", r"\1 gt \2", filter_str)
        filter_str = re.sub(r"(\w+)\s*<\s*'([^']*)'", r"\1 lt '\2'", filter_str)
        filter_str = re.sub(r"(\w+)\s*<\s*(\d+(?:\.\d+)?)", r"\1 lt \2", filter_str)
        
        # Convert LIKE to contains/startswith/endswith
        filter_str = self._convert_like_to_odata(filter_str)
        
        # Convert IN clause to multiple OR conditions
        filter_str = self._convert_in_to_odata(filter_str)
        
        # Convert BETWEEN to range
        filter_str = self._convert_between_to_odata(filter_str)
        
        # Convert IS NULL / IS NOT NULL
        filter_str = re.sub(r"(\w+)\s+IS\s+NULL", r"\1 eq null", filter_str, flags=re.IGNORECASE)
        filter_str = re.sub(r"(\w+)\s+IS\s+NOT\s+NULL", r"\1 ne null", filter_str, flags=re.IGNORECASE)
        
        # Convert logical operators (AND, OR, NOT)
        filter_str = re.sub(r'\bAND\b', 'and', filter_str, flags=re.IGNORECASE)
        filter_str = re.sub(r'\bOR\b', 'or', filter_str, flags=re.IGNORECASE)
        filter_str = re.sub(r'\bNOT\b', 'not', filter_str, flags=re.IGNORECASE)
        
        return filter_str.strip()
    
    def _convert_like_to_odata(self, filter_str: str) -> str:
        """Convert SQL LIKE patterns to OData string functions."""
        # Pattern: column LIKE 'pattern'
        like_pattern = r"(\w+)\s+LIKE\s+'([^']*)'"
        
        def replace_like(match):
            column = match.group(1)
            pattern = match.group(2)
            
            # Handle different LIKE patterns
            if pattern.startswith('%') and pattern.endswith('%'):
                # %value% -> contains
                value = pattern[1:-1]
                return f"contains({column},'{value}')"
            elif pattern.startswith('%'):
                # %value -> endswith
                value = pattern[1:]
                return f"endswith({column},'{value}')"
            elif pattern.endswith('%'):
                # value% -> startswith
                value = pattern[:-1]
                return f"startswith({column},'{value}')"
            else:
                # Exact match
                return f"{column} eq '{pattern}'"
        
        return re.sub(like_pattern, replace_like, filter_str, flags=re.IGNORECASE)
    
    def _convert_in_to_odata(self, filter_str: str) -> str:
        """Convert SQL IN clause to OData OR conditions."""
        # Pattern: column IN (value1, value2, ...)
        in_pattern = r"(\w+)\s+IN\s*\(([^)]+)\)"
        
        def replace_in(match):
            column = match.group(1)
            values_str = match.group(2)
            
            # Parse values
            values = [v.strip().strip("'\"") for v in values_str.split(',')]
            
            # Build OR conditions
            conditions = []
            for value in values:
                # Check if numeric
                if re.match(r'^-?\d+(?:\.\d+)?$', value):
                    conditions.append(f"{column} eq {value}")
                else:
                    conditions.append(f"{column} eq '{value}'")
            
            return "(" + " or ".join(conditions) + ")"
        
        return re.sub(in_pattern, replace_in, filter_str, flags=re.IGNORECASE)
    
    def _convert_between_to_odata(self, filter_str: str) -> str:
        """Convert SQL BETWEEN to OData range conditions."""
        # Pattern: column BETWEEN value1 AND value2
        between_pattern = r"(\w+)\s+BETWEEN\s+('?[\w\-:]+\'?)\s+AND\s+('?[\w\-:]+\'?)"
        
        def replace_between(match):
            column = match.group(1)
            value1 = match.group(2).strip("'")
            value2 = match.group(3).strip("'")
            
            # Check if numeric
            if re.match(r'^-?\d+(?:\.\d+)?$', value1):
                return f"({column} ge {value1} and {column} le {value2})"
            else:
                return f"({column} ge '{value1}' and {column} le '{value2}')"
        
        return re.sub(between_pattern, replace_between, filter_str, flags=re.IGNORECASE)
    
    def _extract_limit(self, sql: str) -> Optional[int]:
        """Extract LIMIT clause and convert to $top."""
        pattern = r'\bLIMIT\s+(\d+)'
        match = re.search(pattern, sql, re.IGNORECASE)
        if match:
            return int(match.group(1))
        return None
    
    def _extract_offset(self, sql: str) -> Optional[int]:
        """Extract OFFSET clause and convert to $skip."""
        pattern = r'\bOFFSET\s+(\d+)'
        match = re.search(pattern, sql, re.IGNORECASE)
        if match:
            return int(match.group(1))
        return None
    
    def _extract_orderby(self, sql: str) -> Optional[str]:
        """Extract ORDER BY clause and convert to $orderby."""
        # Pattern: ORDER BY column [ASC|DESC], ...
        pattern = r'\bORDER\s+BY\s+(.*?)(?=\s+(?:LIMIT|OFFSET)|$)'
        match = re.search(pattern, sql, re.IGNORECASE)
        if match:
            orderby_clause = match.group(1).strip()
            return self._convert_orderby(orderby_clause)
        return None
    
    def _convert_orderby(self, orderby_clause: str) -> str:
        """Convert SQL ORDER BY to OData $orderby syntax."""
        parts = []
        # Split by comma
        columns = re.split(r',\s*', orderby_clause)
        
        for col in columns:
            col = col.strip()
            # Check for ASC/DESC
            if re.search(r'\bDESC\b', col, re.IGNORECASE):
                col_name = re.sub(r'\s+DESC\b', '', col, flags=re.IGNORECASE).strip()
                parts.append(f"{col_name} desc")
            elif re.search(r'\bASC\b', col, re.IGNORECASE):
                col_name = re.sub(r'\s+ASC\b', '', col, flags=re.IGNORECASE).strip()
                parts.append(f"{col_name} asc")
            else:
                # Default is ascending
                parts.append(col)
        
        return ",".join(parts)


# Singleton instance
_converter: Optional[SQLToODataConverter] = None


def get_sql_to_odata_converter() -> SQLToODataConverter:
    """Get the singleton SQLToODataConverter instance."""
    global _converter
    if _converter is None:
        _converter = SQLToODataConverter()
    return _converter
