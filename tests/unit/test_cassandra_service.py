from unittest.mock import Mock, MagicMock, patch
import pytest
from cassandra_service import CassandraService
from cassandra_connection import CassandraConnection


class TestCassandraServiceUnit:
    """Unit tests for CassandraService."""
    
    def test_get_tables_with_mock(self):
        """Test get_tables with mocked connection."""
        # Create mock connection
        mock_connection = Mock(spec=CassandraConnection)
        mock_session = MagicMock()
        mock_connection.session = mock_session
        mock_connection.prepared_statements = {'select_tables': Mock()}
        
        # Mock the execute result
        mock_result = [
            Mock(table_name="table1"),
            Mock(table_name="table2"),
            Mock(table_name="table3")
        ]
        mock_session.execute.return_value = mock_result
        
        # Create service and test
        service = CassandraService(mock_connection)
        tables = service.get_tables("test_keyspace")
        
        # Verify results
        assert len(tables) == 3
        assert "table1" in tables
        assert "table2" in tables
        assert "table3" in tables
        
        # Verify the prepared statement was used
        mock_session.execute.assert_called_once_with(
            mock_connection.prepared_statements['select_tables'],
            ["test_keyspace"]
        )
    
    def test_get_tables_empty_keyspace(self):
        """Test get_tables returns empty list for empty keyspace."""
        # Create mock connection
        mock_connection = Mock(spec=CassandraConnection)
        mock_session = MagicMock()
        mock_connection.session = mock_session
        mock_connection.prepared_statements = {'select_tables': Mock()}
        
        # Mock empty result
        mock_session.execute.return_value = []
        
        # Create service and test
        service = CassandraService(mock_connection)
        tables = service.get_tables("empty_keyspace")
        
        assert tables == []
    
    def test_get_create_table_success(self):
        """Test get_create_table with successful response."""
        # Create mock connection
        mock_connection = Mock(spec=CassandraConnection)
        mock_session = MagicMock()
        mock_connection.session = mock_session
        
        # Mock the execute result
        mock_result = Mock()
        mock_result.one.return_value = Mock(create_statement="CREATE TABLE test.table1 (...)")
        mock_session.execute.return_value = mock_result
        
        # Create service and test
        service = CassandraService(mock_connection)
        create_stmt = service.get_create_table("test", "table1")
        
        assert create_stmt == "CREATE TABLE test.table1 (...)"
        mock_session.execute.assert_called_once_with("DESCRIBE TABLE test.table1")
    
    def test_get_create_table_error(self):
        """Test get_create_table with error."""
        # Create mock connection
        mock_connection = Mock(spec=CassandraConnection)
        mock_session = MagicMock()
        mock_connection.session = mock_session
        
        # Mock exception
        mock_session.execute.side_effect = Exception("Table not found")
        
        # Create service and test
        service = CassandraService(mock_connection)
        
        with pytest.raises(Exception) as exc_info:
            service.get_create_table("test", "non_existent")
        
        assert "Table not found" in str(exc_info.value)
    
    def test_execute_query_with_parameters(self):
        """Test execute_query passes parameters correctly."""
        # Create mock connection
        mock_connection = Mock(spec=CassandraConnection)
        mock_session = MagicMock()
        mock_connection.session = mock_session
        
        # Create service and test
        service = CassandraService(mock_connection)
        query = "INSERT INTO test (id, name) VALUES (?, ?)"
        params = (123, "test_name")
        
        service.execute_query(query, params)
        
        # Verify call
        mock_session.execute.assert_called_once_with(query, params)
    
    def test_execute_query_without_parameters(self):
        """Test execute_query without parameters."""
        # Create mock connection
        mock_connection = Mock(spec=CassandraConnection)
        mock_session = MagicMock()
        mock_connection.session = mock_session
        
        # Create service and test
        service = CassandraService(mock_connection)
        query = "SELECT * FROM test"
        
        service.execute_query(query)
        
        # Verify call
        mock_session.execute.assert_called_once_with(query, None)