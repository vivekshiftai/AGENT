"""Datasource metadata entity for platform storage."""
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional


@dataclass
class DatasourceEntity:
    """
    Datasource metadata stored in psdb.datasources.

    id, name, type, host, port, database, username, password, extra_config, created_at
    """

    id: Optional[int] = None
    name: str = ""
    type: str = ""  # sap | clickhouse | postgres | mysql | excel | csv
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    extra_config: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None

    def to_connection_config(self) -> Dict[str, Any]:
        """Config dict for ConnectorFactory.get_connector()."""
        c = {
            "type": self.type.lower(),
            "name": self.name,
        }
        if self.host is not None:
            c["host"] = self.host
        if self.port is not None:
            c["port"] = self.port
        if self.database is not None:
            c["database"] = self.database
            c["database_name"] = self.database
        if self.username is not None:
            c["username"] = self.username
        if self.password is not None:
            c["password"] = self.password
        if self.extra_config:
            c.update(self.extra_config)
        return c
