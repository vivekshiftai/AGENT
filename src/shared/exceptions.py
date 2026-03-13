"""Shared exception classes."""
from typing import Optional


class AnalyticsException(Exception):
    """Base exception for analytics operations."""
    pass


class QueryParsingException(AnalyticsException):
    """Raised when query parsing fails."""
    pass


class SQLGenerationException(AnalyticsException):
    """Raised when SQL generation fails."""
    pass


class DatabaseException(AnalyticsException):
    """Raised when database operations fail."""
    pass


class DataAnalysisException(AnalyticsException):
    """Raised when data analysis fails."""
    pass


class VisualizationException(AnalyticsException):
    """Raised when visualization generation fails."""
    pass


class LLMException(AnalyticsException):
    """Raised when LLM operations fail."""
    pass


class DatasphereException(AnalyticsException):
    """Base exception for SAP Datasphere operations."""
    pass


class TokenRetrievalException(DatasphereException):
    """Raised when token retrieval from Key Vault fails."""
    pass
