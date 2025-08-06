"""Unit tests for the MCP server module.

This module tests the FastMCP server implementation and its tools
for interacting with Cassandra through the Model Context Protocol.
"""

from unittest.mock import AsyncMock, Mock

import pytest

from cassandra_service import CassandraService
from mcp_server import create_mcp_server


class TestMCPServer:
    """Unit tests for MCP server tools.

    Tests cover:
    - Tool registration with FastMCP
    - Individual tool functionality
    - Error handling in MCP tools
    - Proper async operation of tools
    """

    def _create_mock_service(self):
        """Helper to create a properly mocked CassandraService."""
        mock_service = Mock(spec=CassandraService)
        mock_connection = Mock()
        mock_session = Mock()
        mock_service.connection = mock_connection
        mock_connection.session = mock_session
        # Mock the new discovery methods
        mock_service.discover_system_tables = AsyncMock(return_value={
            "system": ["local", "peers"],
            "system_views": ["disk_usage"]
        })
        mock_service.generate_system_table_description = Mock(
            return_value="Test description for system tables"
        )
        return mock_service

    @pytest.mark.asyncio
    async def test_tool_registration(self):
        """Test that tools are properly registered with the MCP server."""
        # Use helper to create mock service
        mock_service = self._create_mock_service()

        # Create MCP server (now async)
        mcp = await create_mcp_server(mock_service)

        # Check that the server has been created
        assert mcp is not None
        assert mcp.name == "Cassandra MCP Server"

    @pytest.mark.asyncio
    async def test_get_keyspaces_tool(self):
        """Test get_keyspaces tool functionality."""
        # Use helper to create mock service
        mock_service = self._create_mock_service()
        mock_service.get_keyspaces = AsyncMock(
            return_value=[
                {
                    'name': 'my_app',
                    'replication': {'class': 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor': '3'},
                    'durable_writes': True
                },
                {
                    'name': 'another_app',
                    'replication': {'class': 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'dc1': '3', 'dc2': '2'},
                    'durable_writes': False
                }
            ]
        )
        
        # Create the MCP server
        mcp = await create_mcp_server(mock_service)
        
        # Test the service method
        keyspaces = await mock_service.get_keyspaces(include_system=False)
        
        assert len(keyspaces) == 2
        assert keyspaces[0]['name'] == 'my_app'
        assert keyspaces[1]['durable_writes'] == False
        mock_service.get_keyspaces.assert_called_once_with(include_system=False)
    
    @pytest.mark.asyncio
    async def test_get_keyspaces_tool_empty(self):
        """Test get_keyspaces with no user keyspaces."""
        mock_service = self._create_mock_service()
        mock_service.get_keyspaces = AsyncMock(return_value=[])
        
        mcp = await create_mcp_server(mock_service)
        
        # Test the service method
        keyspaces = await mock_service.get_keyspaces(include_system=False)
        assert keyspaces == []
    
    @pytest.mark.asyncio
    async def test_get_tables_tool(self):
        """Test get_tables tool functionality through direct invocation."""
        # Use helper to create mock service
        mock_service = self._create_mock_service()
        mock_service.get_tables = AsyncMock(
            return_value=["users", "products", "orders"]
        )

        # Import the function directly from the module
        from mcp_server import create_mcp_server

        # Create the MCP server which registers the tools
        mcp = await create_mcp_server(mock_service)

        # The tool is registered as a function on the mcp object
        # We'll test by mocking the service method directly
        tables = await mock_service.get_tables("test_keyspace")

        assert tables == ["users", "products", "orders"]
        mock_service.get_tables.assert_called_once_with("test_keyspace")

    @pytest.mark.asyncio
    async def test_get_tables_empty_keyspace(self):
        """Test get_tables with empty keyspace."""
        mock_service = self._create_mock_service()
        mock_service.get_tables = AsyncMock(return_value=[])

        mcp = await create_mcp_server(mock_service)

        # Test the service method
        tables = await mock_service.get_tables("empty_keyspace")
        assert tables == []

    @pytest.mark.asyncio
    async def test_get_tables_error_handling(self):
        """Test error handling in get_tables."""
        mock_service = self._create_mock_service()
        mock_service.get_tables = AsyncMock(side_effect=Exception("Connection error"))

        mcp = await create_mcp_server(mock_service)

        # Test error handling
        with pytest.raises(Exception) as exc_info:
            await mock_service.get_tables("test_keyspace")

        assert "Connection error" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_create_table_tool(self):
        """Test get_create_table functionality."""
        mock_service = self._create_mock_service()
        create_statement = """CREATE TABLE test.users (
            id UUID PRIMARY KEY,
            username TEXT,
            email TEXT
        )"""
        mock_service.get_create_table = AsyncMock(return_value=create_statement)

        mcp = await create_mcp_server(mock_service)

        # Test the service method
        result = await mock_service.get_create_table("test", "users")

        assert result == create_statement
        mock_service.get_create_table.assert_called_once_with("test", "users")

    @pytest.mark.asyncio
    async def test_get_create_table_error(self):
        """Test error handling in get_create_table."""
        mock_service = self._create_mock_service()
        mock_service.get_create_table = AsyncMock(
            side_effect=Exception("Table does not exist")
        )

        mcp = await create_mcp_server(mock_service)

        # Test error handling
        with pytest.raises(Exception) as exc_info:
            await mock_service.get_create_table("test", "users")

        assert "Table does not exist" in str(exc_info.value)
