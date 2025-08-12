"""Unit tests for CassandraUtility module.

Tests version parsing, caching, and table operations.
"""

from unittest.mock import Mock, patch

import pytest

from ecm.cassandra_utility import CassandraUtility
from ecm.exceptions import CassandraVersionError


class TestCassandraUtility:
    """Tests for CassandraUtility class."""

    @pytest.fixture
    def utility(self):
        """Create a CassandraUtility instance for testing."""
        mock_session = Mock()
        return CassandraUtility(mock_session)

    @pytest.mark.asyncio
    async def test_get_version_from_cache(self, utility):
        """Test version retrieval from cache."""
        utility._version_cache = (4, 0, 11)
        
        version = await utility.get_version()
        assert version == (4, 0, 11)
        
        # Should not query database
        utility.session.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_version_from_metadata(self, utility):
        """Test version retrieval from cluster metadata."""
        # Mock cluster metadata
        utility.session.cluster = Mock()
        utility.session.cluster.metadata = Mock()
        
        # Mock control connection host
        mock_control_conn = Mock()
        mock_host = Mock()
        mock_host.release_version = "4.0.11"
        mock_control_conn.get_control_connection_host.return_value = mock_host
        utility.session.cluster.control_connection = mock_control_conn
        
        version = await utility.get_version()
        assert version == (4, 0, 11)
        assert utility._version_cache == (4, 0, 11)

    @pytest.mark.asyncio
    async def test_get_version_from_system_local(self, utility):
        """Test version retrieval from system.local query."""
        # No cluster metadata available
        utility.session.cluster = Mock()
        utility.session.cluster.metadata = Mock()
        utility.session.cluster.control_connection = None
        
        # Mock query result
        mock_result = Mock()
        mock_row = Mock()
        mock_row.release_version = "5.0.0"
        mock_result.one.return_value = mock_row
        utility.session.execute.return_value = mock_result
        
        version = await utility.get_version()
        assert version == (5, 0, 0)
        assert utility._version_cache == (5, 0, 0)
        
        # Should have queried system.local
        utility.session.execute.assert_called_once_with(
            "SELECT release_version FROM system.local"
        )

    @pytest.mark.asyncio
    async def test_get_version_no_version_available(self, utility):
        """Test error when no version information is available."""
        utility.session.cluster = None
        utility.session.execute.return_value = None
        
        with pytest.raises(CassandraVersionError) as exc_info:
            await utility.get_version()
        
        assert "no version information available" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_version_query_error(self, utility):
        """Test error handling when version query fails."""
        utility.session.cluster = None
        utility.session.execute.side_effect = Exception("Query failed")
        
        with pytest.raises(CassandraVersionError) as exc_info:
            await utility.get_version()
        
        assert "Unable to determine Cassandra version" in str(exc_info.value)

    def test_parse_version_standard(self, utility):
        """Test parsing standard version strings."""
        assert utility._parse_version("4.0.11") == (4, 0, 11)
        assert utility._parse_version("5.0.0") == (5, 0, 0)
        assert utility._parse_version("3.11.16") == (3, 11, 16)

    def test_parse_version_snapshot(self, utility):
        """Test parsing snapshot version strings."""
        assert utility._parse_version("5.0.0-SNAPSHOT") == (5, 0, 0)
        assert utility._parse_version("4.1.0-beta1") == (4, 1, 0)

    def test_parse_version_short(self, utility):
        """Test parsing short version strings."""
        assert utility._parse_version("4.0") == (4, 0, 0)
        assert utility._parse_version("5") == (5, 0, 0)

    def test_parse_version_invalid(self, utility):
        """Test error handling for invalid version strings."""
        with pytest.raises(CassandraVersionError):
            utility._parse_version("invalid")
        
        with pytest.raises(CassandraVersionError):
            utility._parse_version("")
        
        with pytest.raises(CassandraVersionError):
            utility._parse_version("a.b.c")

    def test_get_table(self, utility):
        """Test CassandraTable object creation."""
        from ecm.cassandra_table import CassandraTable
        
        table = utility.get_table("test_keyspace", "test_table")
        
        assert isinstance(table, CassandraTable)
        assert table.keyspace == "test_keyspace"
        assert table.table == "test_table"
        assert table.session == utility.session