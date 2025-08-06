from typing import List, Optional

from pydantic import ConfigDict
from pydantic_settings import BaseSettings

from constants import (DEFAULT_DATACENTER, DEFAULT_PORT,
                       DEFAULT_PROTOCOL_VERSION)


class CassandraConfig(BaseSettings):
    """Configuration for Cassandra MCP server."""
    
    model_config = ConfigDict(
        env_prefix="CASSANDRA_",
        env_file=".env",
        extra="ignore"
    )

    # Connection settings
    contact_points: List[str] = ["localhost"]
    port: int = DEFAULT_PORT
    datacenter: str = DEFAULT_DATACENTER
    username: Optional[str] = None
    password: Optional[str] = None
    protocol_version: int = DEFAULT_PROTOCOL_VERSION

    # Test-specific settings
    test_keyspace: str = "mcp_test"
    test_contact_points: Optional[List[str]] = None
    test_datacenter: Optional[str] = None
