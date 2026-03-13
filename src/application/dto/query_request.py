"""Query request DTO."""
from pydantic import BaseModel, Field, ConfigDict
from typing import Optional


class QueryRequest(BaseModel):
    """Request DTO for analytics query."""
    query: str = Field(..., description="Natural language query")
    user_id: Optional[str] = Field(None, description="Optional user ID")
    analysis_mode: Optional[str] = Field("normal", description="Analysis mode: 'normal' or 'deep_research'")
    user_context: Optional[str] = Field(None, description="User context information to help tune the response")
    feedback_summary: Optional[str] = Field(None, description="Summary of user feedback to guide response tuning")
    org_context: Optional[str] = Field(None, description="Organization-level context (e.g., fiscal dates, org-specific settings) for SQL queries and analysis")
    
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "query": "Show me total sales by category for the last month",
                "user_id": "user_123",
                "analysis_mode": "normal",
                "user_context": "User prefers detailed breakdowns and wants to see monthly trends",
                "feedback_summary": "Previous responses were too high-level, user wants more granular analysis",
                "org_context": "Fiscal year starts in April, fiscal quarters: Q1 (Apr-Jun), Q2 (Jul-Sep), Q3 (Oct-Dec), Q4 (Jan-Mar). Currency: USD. Timezone: EST.",
            }
        }
    )

