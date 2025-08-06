"""Custom exceptions for the Cassandra MCP server.

This module defines custom exception classes for better error handling
and more specific error reporting throughout the application.
"""


class CassandraMCPError(Exception):
    """Base exception for all Cassandra MCP errors."""

    pass


class CassandraConnectionError(CassandraMCPError):
    """Raised when connection to Cassandra cluster fails."""

    pass


class CassandraQueryError(CassandraMCPError):
    """Raised when a CQL query execution fails."""

    pass


class CassandraMetadataError(CassandraMCPError):
    """Raised when accessing or parsing metadata fails."""

    pass


class CassandraVersionError(CassandraMCPError):
    """Raised when Cassandra version cannot be determined or is incompatible."""

    pass


class MCPToolError(CassandraMCPError):
    """Raised when an MCP tool operation fails."""

    pass
