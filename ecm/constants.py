"""Constants used throughout the Cassandra MCP server.

This module centralizes all magic numbers, strings, and configuration
constants for better maintainability and consistency.
"""

from .cassandra_version import CassandraVersion

# Connection defaults
DEFAULT_PORT = 9042
DEFAULT_DATACENTER = "datacenter1"
DEFAULT_PROTOCOL_VERSION = 5
CONNECTION_TIMEOUT = 30  # seconds
QUERY_TIMEOUT = 10  # seconds
MAX_QUERY_LOG_LENGTH = 100  # characters

# System keyspaces
SYSTEM_KEYSPACE = "system"
SYSTEM_VIEWS_KEYSPACE = "system_views"
SYSTEM_SCHEMA_KEYSPACE = "system_schema"
VALID_SYSTEM_KEYSPACES = [SYSTEM_KEYSPACE, SYSTEM_VIEWS_KEYSPACE]

# Query limits
MAX_DISPLAY_ROWS = 10
MAX_CONCURRENT_QUERIES = 10
BATCH_SIZE = 100

# Logging
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# Cache settings
VERSION_CACHE_TTL = 3600  # 1 hour in seconds

# Compaction strategies
STCS_CLASS = "SizeTieredCompactionStrategy"
UCS_CLASS = "UnifiedCompactionStrategy"
UCS_MIN_VERSION = CassandraVersion(5, 0, 0)

# MCP Server
MCP_SERVER_NAME = "Cassandra MCP Server"
