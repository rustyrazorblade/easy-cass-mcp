"""Unit tests for the CassandraService module.

This module contains unit tests for the CassandraService class,
focusing on async operations and proper mocking of database connections.
"""

from unittest.mock import AsyncMock, Mock

import pytest

from ecm.cassandra_connection import CassandraConnection
from ecm.cassandra_service import CassandraService


class TestCassandraServiceUnit:
    """Unit tests for async CassandraService.

    Tests cover:
    - Table retrieval operations
    - Query execution with and without parameters
    - Error handling for various scenarios
    - Proper async/await patterns
    """

    @pytest.mark.asyncio
    async def test_get_keyspaces_with_system(self):
        """Test get_keyspaces including system keyspaces."""
        # Create mock connection
        mock_connection = Mock(spec=CassandraConnection)
        mock_connection.prepared_statements = {"select_keyspaces": Mock()}
        
        # Mock keyspaces result
        mock_result = [
            Mock(keyspace_name="system", replication={'class': 'LocalStrategy'}, durable_writes=True),
            Mock(keyspace_name="system_schema", replication={'class': 'LocalStrategy'}, durable_writes=True),
            Mock(keyspace_name="my_app", replication={'class': 'SimpleStrategy', 'replication_factor': '3'}, durable_writes=True),
        ]
        mock_connection.execute_async = AsyncMock(return_value=mock_result)
        
        # Create service and test
        service = CassandraService(mock_connection)
        keyspaces = await service.get_keyspaces(include_system=True)
        
        # Verify results
        assert len(keyspaces) == 3
        assert any(ks['name'] == 'system' for ks in keyspaces)
        assert any(ks['name'] == 'my_app' for ks in keyspaces)
        
        # Check my_app keyspace has correct replication
        my_app = next(ks for ks in keyspaces if ks['name'] == 'my_app')
        assert my_app['replication']['replication_factor'] == '3'
    
    @pytest.mark.asyncio
    async def test_get_keyspaces_without_system(self):
        """Test get_keyspaces excluding system keyspaces."""
        # Create mock connection
        mock_connection = Mock(spec=CassandraConnection)
        mock_connection.prepared_statements = {"select_keyspaces": Mock()}
        
        # Mock keyspaces result
        mock_result = [
            Mock(keyspace_name="system", replication={'class': 'LocalStrategy'}, durable_writes=True),
            Mock(keyspace_name="system_schema", replication={'class': 'LocalStrategy'}, durable_writes=True),
            Mock(keyspace_name="my_app", replication={'class': 'SimpleStrategy', 'replication_factor': '3'}, durable_writes=True),
            Mock(keyspace_name="another_app", replication={'class': 'NetworkTopologyStrategy', 'dc1': '3', 'dc2': '2'}, durable_writes=False),
        ]
        mock_connection.execute_async = AsyncMock(return_value=mock_result)
        
        # Create service and test
        service = CassandraService(mock_connection)
        keyspaces = await service.get_keyspaces(include_system=False)
        
        # Verify only user keyspaces returned
        assert len(keyspaces) == 2
        assert all(not ks['name'].startswith('system') for ks in keyspaces)
        assert any(ks['name'] == 'my_app' for ks in keyspaces)
        assert any(ks['name'] == 'another_app' for ks in keyspaces)
        
        # Check another_app has durable_writes=False
        another_app = next(ks for ks in keyspaces if ks['name'] == 'another_app')
        assert another_app['durable_writes'] == False
    
    @pytest.mark.asyncio
    async def test_get_keyspaces_empty(self):
        """Test get_keyspaces with no user keyspaces."""
        # Create mock connection
        mock_connection = Mock(spec=CassandraConnection)
        mock_connection.prepared_statements = {"select_keyspaces": Mock()}
        
        # Mock only system keyspaces
        mock_result = [
            Mock(keyspace_name="system", replication={'class': 'LocalStrategy'}, durable_writes=True),
            Mock(keyspace_name="system_schema", replication={'class': 'LocalStrategy'}, durable_writes=True),
        ]
        mock_connection.execute_async = AsyncMock(return_value=mock_result)
        
        # Create service and test
        service = CassandraService(mock_connection)
        keyspaces = await service.get_keyspaces(include_system=False)
        
        # Verify no user keyspaces returned
        assert len(keyspaces) == 0
    
    @pytest.mark.asyncio
    async def test_get_tables_with_mock(self):
        """Test get_tables with mocked connection."""
        # Create mock connection
        mock_connection = Mock(spec=CassandraConnection)
        mock_connection.prepared_statements = {"select_tables": Mock()}

        # Mock the execute_async result
        mock_result = [
            Mock(table_name="table1"),
            Mock(table_name="table2"),
            Mock(table_name="table3"),
        ]
        mock_connection.execute_async = AsyncMock(return_value=mock_result)

        # Create service and test
        service = CassandraService(mock_connection)
        tables = await service.get_tables("test_keyspace")

        # Verify results
        assert len(tables) == 3
        assert "table1" in tables
        assert "table2" in tables
        assert "table3" in tables

        # Verify the prepared statement was used
        mock_connection.execute_async.assert_called_once_with(
            mock_connection.prepared_statements["select_tables"], ["test_keyspace"]
        )

    @pytest.mark.asyncio
    async def test_get_tables_empty_keyspace(self):
        """Test get_tables returns empty list for empty keyspace."""
        # Create mock connection
        mock_connection = Mock(spec=CassandraConnection)
        mock_connection.prepared_statements = {"select_tables": Mock()}

        # Mock empty result
        mock_connection.execute_async = AsyncMock(return_value=[])

        # Create service and test
        service = CassandraService(mock_connection)
        tables = await service.get_tables("empty_keyspace")

        assert tables == []

    @pytest.mark.asyncio
    async def test_execute_query_with_parameters(self):
        """Test execute_query passes parameters correctly."""
        # Create mock connection
        mock_connection = Mock(spec=CassandraConnection)
        mock_connection.execute_async = AsyncMock()

        # Create service and test
        service = CassandraService(mock_connection)
        query = "INSERT INTO test (id, name) VALUES (%s, %s)"
        params = (123, "test_name")

        await service.execute_query(query, params)

        # Verify call
        mock_connection.execute_async.assert_called_once_with(query, params)

    @pytest.mark.asyncio
    async def test_execute_query_without_parameters(self):
        """Test execute_query without parameters."""
        # Create mock connection
        mock_connection = Mock(spec=CassandraConnection)
        mock_connection.execute_async = AsyncMock()

        # Create service and test
        service = CassandraService(mock_connection)
        query = "SELECT * FROM test"

        await service.execute_query(query)

        # Verify call
        mock_connection.execute_async.assert_called_once_with(query, None)
    
    def test_format_node_results_empty(self):
        """Test formatting empty node results."""
        service = CassandraService(Mock())
        result = service.format_node_results({})
        assert result == "No results returned"
    
    def test_format_node_results_with_data(self):
        """Test formatting node results with data."""
        service = CassandraService(Mock())
        results = {
            "192.168.1.1": [{"col1": "val1", "col2": "val2"}],
            "192.168.1.2": []
        }
        
        formatted = service.format_node_results(results)
        assert "=== Node: 192.168.1.1 ===" in formatted
        assert "=== Node: 192.168.1.2 ===" in formatted
        assert "{'col1': 'val1', 'col2': 'val2'}" in formatted
        assert "No results" in formatted
    
    def test_format_node_results_with_query(self):
        """Test formatting node results with query header."""
        service = CassandraService(Mock())
        results = {"192.168.1.1": []}
        
        formatted = service.format_node_results(results, "SELECT * FROM test")
        assert "=== Query: SELECT * FROM test ===" in formatted
        assert "=== Node: 192.168.1.1 ===" in formatted
    
    def test_format_node_results_with_error(self):
        """Test formatting node results with errors."""
        service = CassandraService(Mock())
        results = {
            "192.168.1.1": {"error": "Connection timeout"},
            "192.168.1.2": [{"data": "ok"}]
        }
        
        formatted = service.format_node_results(results)
        assert "Error: Connection timeout" in formatted
        assert "{'data': 'ok'}" in formatted
    
    def test_format_system_table_results(self):
        """Test formatting system table results with row limiting."""
        service = CassandraService(Mock())
        
        # Create results with more than MAX_DISPLAY_ROWS
        many_rows = [{"id": i} for i in range(15)]
        results = {
            "192.168.1.1": many_rows,
            "192.168.1.2": []
        }
        
        formatted = service.format_system_table_results(results, "system", "local")
        assert "=== Query: SELECT * FROM system.local ===" in formatted
        assert "--- Node: 192.168.1.1 ---" in formatted
        assert "Returned 15 rows" in formatted
        assert "... and 5 more rows" in formatted  # MAX_DISPLAY_ROWS is 10
        assert "--- Node: 192.168.1.2 ---" in formatted
    
    def test_format_single_node_results(self):
        """Test formatting single node results."""
        service = CassandraService(Mock())
        
        # Test with results
        result = [{"col1": "val1"}, {"col1": "val2"}]
        formatted = service.format_single_node_results(result, "192.168.1.1")
        assert "=== Results from node 192.168.1.1 ===" in formatted
        assert "{'col1': 'val1'}" in formatted
        assert "{'col1': 'val2'}" in formatted
        
        # Test with no results
        formatted = service.format_single_node_results(None, "192.168.1.1")
        assert "No results from node 192.168.1.1" in formatted
        
        # Test with empty list
        formatted = service.format_single_node_results([], "192.168.1.1")
        assert "=== Results from node 192.168.1.1 ===" in formatted
        assert "No results" in formatted
    
    @pytest.mark.asyncio
    async def test_get_cassandra_version(self):
        """Test getting Cassandra version."""
        service = CassandraService(Mock())
        
        # Mock execute_async to return version
        mock_row = Mock()
        mock_row.release_version = "4.0.11"
        mock_result = [mock_row]
        service.connection.execute_async = AsyncMock(return_value=mock_result)
        
        version = await service.get_cassandra_version()
        assert version == (4, 0, 11)
        
        # Test caching - should not query again
        service.connection.execute_async.reset_mock()
        version2 = await service.get_cassandra_version()
        assert version2 == (4, 0, 11)
        service.connection.execute_async.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_get_cassandra_version_with_snapshot(self):
        """Test parsing version with SNAPSHOT suffix."""
        service = CassandraService(Mock())
        
        mock_row = Mock()
        mock_row.release_version = "5.0.0-SNAPSHOT"
        service.connection.execute_async = AsyncMock(return_value=[mock_row])
        
        version = await service.get_cassandra_version()
        assert version == (5, 0, 0)
    
    @pytest.mark.asyncio
    async def test_discover_system_tables_cassandra_4(self):
        """Test discovering system tables for Cassandra 4.x."""
        service = CassandraService(Mock())
        
        # Mock version check
        service.get_cassandra_version = AsyncMock(return_value=(4, 0, 11))
        
        # Mock get_tables calls
        service.get_tables = AsyncMock(side_effect=[
            ["local", "peers", "compaction_history"],  # system tables
            ["disk_usage", "local_read_latency", "thread_pools"]  # system_views tables
        ])
        
        discovered = await service.discover_system_tables()
        
        assert "system" in discovered
        assert "system_views" in discovered
        assert "local" in discovered["system"]
        assert "disk_usage" in discovered["system_views"]
        
        # Verify get_tables was called for both keyspaces
        assert service.get_tables.call_count == 2
        service.get_tables.assert_any_call("system")
        service.get_tables.assert_any_call("system_views")
    
    @pytest.mark.asyncio
    async def test_discover_system_tables_cassandra_3(self):
        """Test discovering system tables for Cassandra 3.x (no system_views)."""
        service = CassandraService(Mock())
        
        # Mock version check for Cassandra 3.11
        service.get_cassandra_version = AsyncMock(return_value=(3, 11, 0))
        
        # Mock get_tables for system only
        service.get_tables = AsyncMock(return_value=["local", "peers", "compaction_history"])
        
        discovered = await service.discover_system_tables()
        
        assert "system" in discovered
        assert "system_views" not in discovered
        assert "local" in discovered["system"]
        
        # Verify get_tables was only called for system keyspace
        service.get_tables.assert_called_once_with("system")
    
    def test_generate_system_table_description(self):
        """Test generating dynamic description for system tables."""
        service = CassandraService(Mock())
        
        discovered_tables = {
            "system": ["local", "peers", "unknown_table"],
            "system_views": ["disk_usage", "thread_pools"]
        }
        
        description = service.generate_system_table_description(discovered_tables)
        
        # Check that known tables have descriptions
        assert "local: Current node information" in description
        assert "peers: Information about other nodes" in description
        assert "disk_usage: Disk space usage" in description
        assert "thread_pools: Thread pool statistics" in description
        
        # Check sections are present
        assert "AVAILABLE SYSTEM TABLES" in description
        assert "AVAILABLE SYSTEM_VIEWS TABLES" in description
        
        # Check that unknown_table is included (but not schema tables)
        assert "unknown_table" in description
    
    def test_generate_system_table_description_empty(self):
        """Test generating description with no discovered tables."""
        service = CassandraService(Mock())
        
        description = service.generate_system_table_description({})
        
        assert "Query database internal statistics" in description
        assert "AVAILABLE SYSTEM TABLES" not in description
        assert "AVAILABLE SYSTEM_VIEWS TABLES" not in description
