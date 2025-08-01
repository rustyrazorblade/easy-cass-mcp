from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from cassandra_connection import CassandraConnection
from cassandra_service import CassandraService


class TestCassandraServiceUnit:
    """Unit tests for async CassandraService."""

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
