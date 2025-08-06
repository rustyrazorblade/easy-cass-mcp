"""Unit tests for CassandraConnection module.

This module tests connection management, async context manager,
timeout handling, and query execution functionality.
"""

import asyncio
from unittest.mock import AsyncMock, Mock, patch

import pytest

from cassandra_connection import CassandraConnection
from exceptions import CassandraConnectionError, CassandraQueryError


class TestCassandraConnection:
    """Tests for CassandraConnection class."""

    @pytest.fixture
    def connection(self):
        """Create a CassandraConnection instance for testing."""
        return CassandraConnection(
            contact_points=["localhost"],
            port=9042,
            datacenter="datacenter1",
        )

    @pytest.mark.asyncio
    async def test_context_manager_success(self, connection):
        """Test async context manager with successful connection."""
        with patch.object(connection, "connect", new_callable=AsyncMock) as mock_connect:
            with patch.object(connection, "disconnect") as mock_disconnect:
                async with connection as conn:
                    assert conn is connection
                    mock_connect.assert_called_once()
                mock_disconnect.assert_called_once()

    @pytest.mark.asyncio
    async def test_context_manager_with_error(self, connection):
        """Test async context manager handles errors properly."""
        with patch.object(connection, "connect", new_callable=AsyncMock) as mock_connect:
            with patch.object(connection, "disconnect") as mock_disconnect:
                mock_connect.side_effect = CassandraConnectionError("Connection failed")
                
                with pytest.raises(CassandraConnectionError):
                    async with connection:
                        pass
                
                mock_connect.assert_called_once()
                # Disconnect should NOT be called if connect fails (context not entered)
                mock_disconnect.assert_not_called()

    @pytest.mark.asyncio
    async def test_connect_already_connected(self, connection):
        """Test connect when already connected."""
        connection._is_connected = True
        
        with patch("cassandra_connection.asyncio.get_event_loop") as mock_get_loop:
            await connection.connect()
            # Should not attempt to connect again
            mock_get_loop.assert_not_called()

    @pytest.mark.asyncio
    async def test_connect_timeout(self, connection):
        """Test connection timeout handling."""
        with patch("cassandra_connection.asyncio.get_event_loop") as mock_get_loop:
            mock_loop = Mock()
            mock_get_loop.return_value = mock_loop
            
            with patch("cassandra_connection.asyncio.wait_for") as mock_wait_for:
                mock_wait_for.side_effect = asyncio.TimeoutError()
                
                with pytest.raises(CassandraConnectionError) as exc_info:
                    await connection.connect()
                
                assert "Connection timeout" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_execute_async_not_connected(self, connection):
        """Test execute_async when not connected."""
        connection._is_connected = False
        
        with pytest.raises(CassandraQueryError) as exc_info:
            await connection.execute_async("SELECT * FROM test")
        
        assert "Not connected to Cassandra" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_execute_async_timeout(self, connection):
        """Test query execution timeout."""
        connection._is_connected = True
        connection.session = Mock()
        
        # Create a mock ResponseFuture that doesn't trigger callbacks
        mock_response_future = Mock()
        mock_response_future.add_callback = Mock()
        mock_response_future.add_errback = Mock()
        connection.session.execute_async.return_value = mock_response_future
        
        with patch("cassandra_connection.asyncio.wait_for") as mock_wait_for:
            mock_wait_for.side_effect = asyncio.TimeoutError()
            
            with pytest.raises(CassandraQueryError) as exc_info:
                await connection.execute_async("SELECT * FROM test")
            
            assert "Query timeout" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_prepare_statements_error_handling(self, connection):
        """Test prepared statement error handling."""
        connection.session = Mock()
        mock_prepare = Mock(side_effect=Exception("Prepare failed"))
        
        with patch("cassandra_connection.asyncio.get_event_loop") as mock_get_loop:
            mock_loop = Mock()
            mock_get_loop.return_value = mock_loop
            mock_loop.run_in_executor = AsyncMock(side_effect=Exception("Prepare failed"))
            
            # Should not raise, just log warning
            await connection._prepare_statements()
            
            # Prepared statements dict should be empty due to failures
            assert len(connection.prepared_statements) == 0

    def test_get_all_hosts_not_connected(self, connection):
        """Test get_all_hosts when not connected."""
        connection._is_connected = False
        connection.cluster = None
        
        hosts = connection.get_all_hosts()
        assert hosts == []

    def test_get_all_hosts_connected(self, connection):
        """Test get_all_hosts when connected."""
        connection._is_connected = True
        connection.cluster = Mock()
        
        mock_host1 = Mock(address="192.168.1.1")
        mock_host2 = Mock(address="192.168.1.2")
        connection.cluster.metadata.all_hosts.return_value = [mock_host1, mock_host2]
        
        hosts = connection.get_all_hosts()
        assert len(hosts) == 2
        assert mock_host1 in hosts
        assert mock_host2 in hosts

    def test_disconnect_not_connected(self, connection):
        """Test disconnect when not connected."""
        connection._is_connected = False
        
        # Should not raise any errors
        connection.disconnect()

    def test_disconnect_with_error(self, connection):
        """Test disconnect error handling."""
        connection._is_connected = True
        connection.session = Mock()
        connection.cluster = Mock()
        
        # Make shutdown raise an exception
        connection.session.shutdown.side_effect = Exception("Shutdown failed")
        
        # Should not raise, just log error
        connection.disconnect()
        
        # Should still mark as disconnected
        assert not connection._is_connected

    @pytest.mark.asyncio
    async def test_execute_on_host_profile_creation(self, connection):
        """Test execution profile creation for specific host."""
        connection._is_connected = True
        connection.cluster = Mock()
        connection.session = Mock()
        connection._execution_profiles = set()
        
        # Mock the profile manager
        connection.cluster.profile_manager.profiles = {}
        
        # Create a mock ResponseFuture that immediately triggers success callback
        mock_response_future = Mock()
        mock_result = Mock()
        
        def mock_add_callback(callback):
            # Immediately call the callback with a result
            callback(mock_result)
        
        mock_response_future.add_callback = mock_add_callback
        mock_response_future.add_errback = Mock()
        connection.session.execute_async.return_value = mock_response_future
        
        # Mock wait_for to return the result immediately
        with patch("cassandra_connection.asyncio.wait_for") as mock_wait_for:
            mock_wait_for.return_value = mock_result
            
            result = await connection.execute_on_host("192.168.1.1", "SELECT * FROM test")
            
            # Should have created a profile
            assert "host_192_168_1_1" in connection._execution_profiles
            connection.cluster.add_execution_profile.assert_called_once()
            assert result == mock_result

    @pytest.mark.asyncio
    async def test_execute_on_host_existing_profile(self, connection):
        """Test execution with existing profile."""
        connection._is_connected = True
        connection.cluster = Mock()
        connection.session = Mock()
        connection._execution_profiles = {"host_192_168_1_1"}
        
        # Create a mock ResponseFuture that immediately triggers success callback
        mock_response_future = Mock()
        mock_result = Mock()
        
        def mock_add_callback(callback):
            # Immediately call the callback with a result
            callback(mock_result)
        
        mock_response_future.add_callback = mock_add_callback
        mock_response_future.add_errback = Mock()
        connection.session.execute_async.return_value = mock_response_future
        
        # Mock wait_for to return the result immediately
        with patch("cassandra_connection.asyncio.wait_for") as mock_wait_for:
            mock_wait_for.return_value = mock_result
            
            result = await connection.execute_on_host("192.168.1.1", "SELECT * FROM test")
            
            # Should not create a new profile
            connection.cluster.add_execution_profile.assert_not_called()
            assert result == mock_result