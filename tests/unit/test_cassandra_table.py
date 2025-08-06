"""Unit tests for CassandraTable module.

Tests table metadata operations and compaction strategy retrieval.
"""

from unittest.mock import Mock, patch

import pytest

from cassandra_table import CassandraTable
from exceptions import CassandraMetadataError


class TestCassandraTable:
    """Tests for CassandraTable class."""

    @pytest.fixture
    def table(self):
        """Create a CassandraTable instance for testing."""
        mock_session = Mock()
        return CassandraTable(mock_session, "test_keyspace", "test_table")

    def test_get_metadata_keyspace_not_found(self, table):
        """Test error when keyspace doesn't exist."""
        table.session.cluster.metadata.keyspaces = {}
        
        with pytest.raises(CassandraMetadataError) as exc_info:
            table._get_metadata()
        
        assert "Keyspace 'test_keyspace' not found" in str(exc_info.value)

    def test_get_metadata_table_not_found(self, table):
        """Test error when table doesn't exist in keyspace."""
        mock_keyspace = Mock()
        mock_keyspace.tables = {}
        table.session.cluster.metadata.keyspaces = {"test_keyspace": mock_keyspace}
        
        with pytest.raises(CassandraMetadataError) as exc_info:
            table._get_metadata()
        
        assert "Table 'test_table' not found" in str(exc_info.value)

    def test_get_metadata_success(self, table):
        """Test successful metadata retrieval."""
        mock_table_metadata = Mock()
        mock_keyspace = Mock()
        mock_keyspace.tables = {"test_table": mock_table_metadata}
        table.session.cluster.metadata.keyspaces = {"test_keyspace": mock_keyspace}
        
        metadata = table._get_metadata()
        
        assert metadata == mock_table_metadata
        assert table._metadata == mock_table_metadata
        
        # Should use cache on second call
        metadata2 = table._get_metadata()
        assert metadata2 == mock_table_metadata

    def test_get_metadata_unexpected_error(self, table):
        """Test handling of unexpected errors during metadata retrieval."""
        table.session.cluster.metadata.keyspaces = None
        
        with pytest.raises(CassandraMetadataError) as exc_info:
            table._get_metadata()
        
        assert "Failed to get metadata" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_compaction_strategy_stcs(self, table):
        """Test getting STCS compaction strategy."""
        mock_metadata = Mock()
        mock_metadata.options = {
            "compaction": {
                "class": "org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy",
                "max_threshold": "32",
                "min_threshold": "4"
            }
        }
        
        with patch.object(table, "_get_metadata", return_value=mock_metadata):
            strategy = await table.get_compaction_strategy()
            
            assert strategy["class"] == "SizeTieredCompactionStrategy"
            assert strategy["options"]["max_threshold"] == "32"
            assert strategy["options"]["min_threshold"] == "4"

    @pytest.mark.asyncio
    async def test_get_compaction_strategy_lcs(self, table):
        """Test getting LCS compaction strategy."""
        mock_metadata = Mock()
        mock_metadata.options = {
            "compaction": {
                "class": "LeveledCompactionStrategy",
                "sstable_size_in_mb": "160"
            }
        }
        
        with patch.object(table, "_get_metadata", return_value=mock_metadata):
            strategy = await table.get_compaction_strategy()
            
            assert strategy["class"] == "LeveledCompactionStrategy"
            assert strategy["options"]["sstable_size_in_mb"] == "160"

    @pytest.mark.asyncio
    async def test_get_compaction_strategy_default(self, table):
        """Test default compaction strategy when not specified."""
        mock_metadata = Mock()
        mock_metadata.options = {}
        
        with patch.object(table, "_get_metadata", return_value=mock_metadata):
            strategy = await table.get_compaction_strategy()
            
            assert strategy["class"] == "SizeTieredCompactionStrategy"
            assert strategy["options"] == {}

    @pytest.mark.asyncio
    async def test_get_create_statement(self, table):
        """Test getting CREATE TABLE statement."""
        mock_metadata = Mock()
        mock_metadata.export_as_string.return_value = (
            "CREATE TABLE test_keyspace.test_table (id int PRIMARY KEY)"
        )
        
        with patch.object(table, "_get_metadata", return_value=mock_metadata):
            create_statement = await table.get_create_statement()
            
            assert create_statement == "CREATE TABLE test_keyspace.test_table (id int PRIMARY KEY)"
            mock_metadata.export_as_string.assert_called_once()


