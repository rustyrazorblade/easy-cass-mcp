"""Unit tests for the CassandraService module.

This module contains unit tests for the CassandraService class,
focusing on async operations and proper mocking of database connections.
"""

from unittest.mock import AsyncMock, Mock

import pytest

from cassandra_connection import CassandraConnection
from cassandra_service import CassandraService


class TestCassandraServiceUnit:
    """Unit tests for async CassandraService.

    Tests cover:
    - Table retrieval operations
    - Query execution with and without parameters
    - Error handling for various scenarios
    - Proper async/await patterns
    """

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
