"""Data source request DTOs."""
from pydantic import BaseModel, Field, ConfigDict
from typing import Optional
from datetime import datetime


class DataSourceCreateRequest(BaseModel):
    """Request to create a new data source."""
    user_id: str = Field(..., description="User ID")
    name: str = Field(..., description="Data source name")
    type: str = Field(..., description="Data source type: postgres, clickhouse, sqlserver, sap, excel")
    host: Optional[str] = Field(None, description="Database host")
    port: Optional[int] = Field(None, description="Database port")
    username: Optional[str] = Field(None, description="Database username")
    password: Optional[str] = Field(None, description="Database password")
    database_name: Optional[str] = Field(None, description="Database name")
    file_path: Optional[str] = Field(None, description="File path for Excel files")


class DataSourceActivateRequest(BaseModel):
    """Request to activate a data source."""
    user_id: str = Field(..., description="User ID")
    data_source_id: int = Field(..., description="Data source ID to activate")


class DataSourceTestRequest(BaseModel):
    """Request to test a data source connection."""
    user_id: str = Field(..., description="User ID")
    data_source_id: Optional[int] = Field(None, description="Data source ID to test")
    config: Optional[dict] = Field(None, description="Config to test (if not using existing data source)")


class DataSourceResponse(BaseModel):
    """Data source response."""
    id: int
    user_id: str
    name: str
    type: str
    host: Optional[str] = None
    port: Optional[int] = None
    username: Optional[str] = None
    password: Optional[str] = None  # Note: In production, never return passwords
    database_name: Optional[str] = None
    file_path: Optional[str] = None
    is_active: bool
    created_at: datetime
    
    model_config = ConfigDict(
        json_encoders={
            datetime: lambda v: v.isoformat() if v else None
        },
        populate_by_name=True
    )

