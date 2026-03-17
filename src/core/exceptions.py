"""Shared exception classes."""

from typing import Optional


class DataAccessException(Exception):
    """Base exception for data access operations."""

    pass


class DatabaseException(DataAccessException):
    """Raised when database operations fail."""

    pass


class ConnectionException(DataAccessException):
    """Raised when connection to data source fails."""

    pass


class DatasphereException(DataAccessException):
    """Base exception for SAP Datasphere operations."""

    pass


class TokenNotFoundError(DatasphereException):
    """Raised when user's access token is not found."""

    pass


class TokenExpiredError(DatasphereException):
    """Raised when the access token has expired."""

    pass


class LLMException(Exception):
    """Raised when an LLM API call fails."""

    pass
