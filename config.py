from pydantic_settings import BaseSettings
from typing import List, Optional


class CassandraConfig(BaseSettings):
    """Configuration for Cassandra MCP server."""
    
    # Connection settings
    contact_points: List[str] = ["localhost"]
    port: int = 9042
    datacenter: str = "datacenter1"
    username: Optional[str] = None
    password: Optional[str] = None
    protocol_version: int = 5
    
    # Test-specific settings
    test_keyspace: str = "mcp_test"
    test_contact_points: Optional[List[str]] = None
    test_datacenter: Optional[str] = None
    
    class Config:
        env_prefix = "CASSANDRA_"
        env_file = ".env"
        extra = "ignore"