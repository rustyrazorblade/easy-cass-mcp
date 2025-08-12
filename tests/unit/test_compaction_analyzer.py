"""Unit tests for CompactionAnalyzer module.

Tests compaction strategy analysis and recommendations.
"""

from unittest.mock import AsyncMock, Mock

import pytest

from ecm.compaction_analyzer import CompactionAnalyzer


class TestCompactionAnalyzer:
    """Tests for CompactionAnalyzer class."""

    @pytest.fixture
    def mock_table(self):
        """Create a mock CassandraTable for testing."""
        return Mock()

    @pytest.mark.asyncio
    async def test_analyze_stcs_to_ucs_recommendation(self, mock_table):
        """Test recommendation to switch from STCS to UCS in Cassandra 5+."""
        # Mock Cassandra 5.0
        analyzer = CompactionAnalyzer(mock_table, (5, 0, 1))
        
        # Mock table with STCS
        mock_table.get_compaction_strategy = AsyncMock(
            return_value={
                "class": "SizeTieredCompactionStrategy",
                "options": {}
            }
        )
        
        recommendations = await analyzer.analyze()
        
        assert len(recommendations) == 1
        assert recommendations[0]["type"] == "compaction_strategy"
        assert "SizeTieredCompactionStrategy" in recommendations[0]["current"]
        assert "UnifiedCompactionStrategy" in recommendations[0]["recommendation"]
        assert "T4" in recommendations[0]["recommendation"]

    @pytest.mark.asyncio
    async def test_analyze_no_recommendation_for_ucs(self, mock_table):
        """Test no recommendation when already using UCS."""
        analyzer = CompactionAnalyzer(mock_table, (5, 0, 0))
        
        # Mock table with UCS
        mock_table.get_compaction_strategy = AsyncMock(
            return_value={
                "class": "UnifiedCompactionStrategy",
                "options": {"scaling_parameters": "T4"}
            }
        )
        
        recommendations = await analyzer.analyze()
        
        assert len(recommendations) == 0

    @pytest.mark.asyncio
    async def test_analyze_no_recommendation_for_lcs(self, mock_table):
        """Test no recommendation for LeveledCompactionStrategy."""
        analyzer = CompactionAnalyzer(mock_table, (5, 0, 0))
        
        # Mock table with LCS
        mock_table.get_compaction_strategy = AsyncMock(
            return_value={
                "class": "LeveledCompactionStrategy",
                "options": {"sstable_size_in_mb": "160"}
            }
        )
        
        recommendations = await analyzer.analyze()
        
        assert len(recommendations) == 0

    @pytest.mark.asyncio
    async def test_analyze_no_recommendation_for_cassandra_4(self, mock_table):
        """Test no UCS recommendation for Cassandra 4.x."""
        analyzer = CompactionAnalyzer(mock_table, (4, 0, 11))
        
        # Mock table with STCS
        mock_table.get_compaction_strategy = AsyncMock(
            return_value={
                "class": "SizeTieredCompactionStrategy",
                "options": {}
            }
        )
        
        recommendations = await analyzer.analyze()
        
        # Should not recommend UCS for Cassandra 4.x
        assert len(recommendations) == 0

    def test_should_recommend_ucs_with_stcs_and_cassandra_5(self, mock_table):
        """Test UCS recommendation logic for STCS in Cassandra 5+."""
        analyzer = CompactionAnalyzer(mock_table, (5, 0, 0))
        
        assert analyzer._should_recommend_ucs("SizeTieredCompactionStrategy")
        assert analyzer._should_recommend_ucs(
            "org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy"
        )

    def test_should_not_recommend_ucs_for_other_strategies(self, mock_table):
        """Test no UCS recommendation for non-STCS strategies."""
        analyzer = CompactionAnalyzer(mock_table, (5, 0, 0))
        
        assert not analyzer._should_recommend_ucs("LeveledCompactionStrategy")
        assert not analyzer._should_recommend_ucs("TimeWindowCompactionStrategy")
        assert not analyzer._should_recommend_ucs("UnifiedCompactionStrategy")

    def test_should_not_recommend_ucs_for_old_cassandra(self, mock_table):
        """Test no UCS recommendation for older Cassandra versions."""
        analyzer = CompactionAnalyzer(mock_table, (4, 1, 5))
        
        assert not analyzer._should_recommend_ucs("SizeTieredCompactionStrategy")
        
        analyzer = CompactionAnalyzer(mock_table, (3, 11, 16))
        assert not analyzer._should_recommend_ucs("SizeTieredCompactionStrategy")

    def test_create_ucs_recommendation(self, mock_table):
        """Test UCS recommendation creation."""
        analyzer = CompactionAnalyzer(mock_table, (5, 0, 0))
        
        recommendation = analyzer._create_ucs_recommendation()
        
        assert recommendation["type"] == "compaction_strategy"
        assert "SizeTieredCompactionStrategy" in recommendation["current"]
        assert "UnifiedCompactionStrategy" in recommendation["recommendation"]
        assert "T4" in recommendation["recommendation"]
        assert "performance" in recommendation["reason"]
        assert "https://rustyrazorblade.com" in recommendation["reference"]