"""SQL to OData parameter converter for SAP Datasphere Consumption API."""
import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

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
    format: Optional[str] = None
    apply: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        params = {}
        if self.format:
            params["$format"] = self.format
        if self.select:
            params["$select"] = self.select
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
        params = self.to_dict()
        return "&".join(f"{k}={v}" for k, v in params.items()) if params else ""


class SQLToODataConverter:
    """Converts SQL query clauses to OData parameters for SAP Datasphere."""

    def convert(self, sql_query: str) -> Tuple[str, ODataParams]:
        sql_query = " ".join(sql_query.split()).strip()
        table_name = self._extract_table_name(sql_query)
        if not table_name:
            raise ValueError("Could not extract table name from SQL query")
        params = ODataParams()
        params.select = self._extract_select(sql_query)
        params.filter = self._extract_where(sql_query)
        params.top = self._extract_limit(sql_query)
        params.skip = self._extract_offset(sql_query)
        params.orderby = self._extract_orderby(sql_query)
        return table_name, params

    def _extract_table_name(self, sql: str) -> Optional[str]:
        match = re.search(r'\bFROM\s+(["\']?[\w.]+["\']?)', sql, re.IGNORECASE)
        return match.group(1).strip('"\'') if match else None

    def _extract_select(self, sql: str) -> Optional[str]:
        match = re.search(r'\bSELECT\s+(.*?)\s+FROM\b', sql, re.IGNORECASE)
        if match:
            cols = match.group(1).strip()
            if cols == "*":
                return None
            parts = [
                p.strip().strip('"\'`')
                for p in re.split(r',\s*(?=(?:[^"]*"[^"]*")*[^"]*$)', cols)
                if p.strip()
            ]
            return ",".join(parts) if parts else None
        return None

    def _extract_where(self, sql: str) -> Optional[str]:
        match = re.search(
            r'\bWHERE\s+(.*?)(?=\s+(?:ORDER\s+BY|LIMIT|OFFSET|GROUP\s+BY)|$)',
            sql,
            re.IGNORECASE,
        )
        if match:
            return self._convert_where_to_filter(match.group(1).strip())
        return None

    def _convert_where_to_filter(self, where: str) -> str:
        for pattern, repl in [
            (r"(\w+)\s*=\s*'([^']*)'", r"\1 eq '\2'"),
            (r"(\w+)\s*=\s*(\d+(?:\.\d+)?)", r"\1 eq \2"),
            (r"(\w+)\s*(?:!=|<>)\s*'([^']*)'", r"\1 ne '\2'"),
            (r"(\w+)\s*>=\s*'([^']*)'", r"\1 ge '\2'"),
            (r"(\w+)\s*<=\s*'([^']*)'", r"\1 le '\2'"),
        ]:
            where = re.sub(pattern, repl, where, flags=re.IGNORECASE)
        where = re.sub(r"\bAND\b", "and", where, flags=re.IGNORECASE)
        where = re.sub(r"\bOR\b", "or", where, flags=re.IGNORECASE)
        return where.strip()

    def _extract_limit(self, sql: str) -> Optional[int]:
        match = re.search(r"\bLIMIT\s+(\d+)", sql, re.IGNORECASE)
        return int(match.group(1)) if match else None

    def _extract_offset(self, sql: str) -> Optional[int]:
        match = re.search(r"\bOFFSET\s+(\d+)", sql, re.IGNORECASE)
        return int(match.group(1)) if match else None

    def _extract_orderby(self, sql: str) -> Optional[str]:
        match = re.search(
            r"\bORDER\s+BY\s+(.*?)(?=\s+(?:LIMIT|OFFSET)|$)", sql, re.IGNORECASE
        )
        return match.group(1).strip() if match else None


_converter: Optional[SQLToODataConverter] = None


def get_sql_to_odata_converter() -> SQLToODataConverter:
    global _converter
    if _converter is None:
        _converter = SQLToODataConverter()
    return _converter
