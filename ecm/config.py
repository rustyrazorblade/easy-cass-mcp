import os
from typing import List, Optional, Union

from pydantic import ConfigDict, field_validator
from pydantic_settings import BaseSettings

from .constants import (DEFAULT_DATACENTER, DEFAULT_PORT,
                        DEFAULT_PROTOCOL_VERSION)


class CassandraConfig(BaseSettings):
    """Configuration for Cassandra MCP server."""

    model_config = ConfigDict(env_prefix="CASSANDRA_", env_file=".env", extra="ignore")

    # Connection settings - support both single host and list
    contact_points: Union[str, List[str]] = ["localhost"]
    host: Optional[str] = None  # Alternative to contact_points for Docker
    port: int = DEFAULT_PORT
    datacenter: str = DEFAULT_DATACENTER
    username: Optional[str] = None
    password: Optional[str] = None
    protocol_version: int = DEFAULT_PROTOCOL_VERSION

    # Test-specific settings
    test_keyspace: str = "mcp_test"
    test_contact_points: Optional[List[str]] = None
    test_datacenter: Optional[str] = None
    
    @field_validator('contact_points', mode='before')
    @classmethod
    def parse_contact_points(cls, v):
        """Parse contact points from string or list format."""
        if isinstance(v, str):
            # Handle comma-separated string
            return [point.strip() for point in v.split(',')]
        return v
    
    def __init__(self, **data):
        """Initialize config with Docker-friendly environment variable support."""
        # Support CASSANDRA_HOST as an alternative to CASSANDRA_CONTACT_POINTS
        if 'host' not in data and os.getenv('CASSANDRA_HOST'):
            data['contact_points'] = [os.getenv('CASSANDRA_HOST')]
        super().__init__(**data)
        
        # Normalize contact_points to always be a list
        if isinstance(self.contact_points, str):
            self.contact_points = [self.contact_points]
        elif self.host and not any(cp != 'localhost' for cp in self.contact_points):
            # Use host if contact_points is still default
            self.contact_points = [self.host]
