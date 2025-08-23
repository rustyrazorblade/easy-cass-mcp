"""Unit tests for the ConfigurationAnalyzer module.

This module tests the configuration recommendation system
for Cassandra clusters.
"""

from unittest.mock import Mock, AsyncMock

import pytest

from ecm.cassandra_settings import CassandraSettings
from ecm.cassandra_version import CassandraVersion
from ecm.configuration_analyzer import ConfigurationAnalyzer
from ecm.thread_pool_stats import ThreadPoolStats
from ecm.recommendation import Recommendation, RecommendationCategory, RecommendationPriority


class TestConfigurationAnalyzer:
    """Tests for ConfigurationAnalyzer class.
    
    Tests cover:
    - Analyzer creation with session and version
    - Empty recommendations handling
    - Version information
    - Framework for future rule testing
    """

    @pytest.mark.asyncio
    async def test_analyzer_creation(self):
        """Test ConfigurationAnalyzer creation with CassandraSettings and ThreadPoolStats."""
        mock_session = Mock()
        version = CassandraVersion(5, 0, 0)
        settings = CassandraSettings(mock_session, version)
        
        mock_thread_pool_stats = Mock(spec=ThreadPoolStats)
        mock_thread_pool_stats.is_loaded = Mock(return_value=True)
        
        analyzer = ConfigurationAnalyzer(settings, mock_thread_pool_stats)
        
        assert analyzer.settings == settings
        assert analyzer.settings.session == mock_session
        assert analyzer.settings.version == version
        assert analyzer.thread_pool_stats == mock_thread_pool_stats
        assert analyzer.thread_pool_analyzer is not None

    @pytest.mark.asyncio
    async def test_analyzer_creation_cassandra4(self):
        """Test ConfigurationAnalyzer with Cassandra 4.x version."""
        mock_session = Mock()
        version = CassandraVersion(4, 1, 3)
        settings = CassandraSettings(mock_session, version)
        
        mock_thread_pool_stats = Mock(spec=ThreadPoolStats)
        mock_thread_pool_stats.is_loaded = Mock(return_value=True)
        
        analyzer = ConfigurationAnalyzer(settings, mock_thread_pool_stats)
        
        assert analyzer.settings.version == version
        assert analyzer.settings.version.major == 4
        assert analyzer.settings.version.minor == 1
        assert analyzer.settings.version.patch == 3

    @pytest.mark.asyncio
    async def test_analyze_empty(self):
        """Test analyze returns empty list when no issues found."""
        mock_session = Mock()
        settings = CassandraSettings(mock_session, CassandraVersion(5, 0, 0))
        
        # Mock thread pool stats with no issues
        mock_thread_pool_stats = Mock(spec=ThreadPoolStats)
        mock_thread_pool_analyzer = Mock()
        mock_thread_pool_analyzer.analyze = AsyncMock(return_value=[])
        
        analyzer = ConfigurationAnalyzer(settings, mock_thread_pool_stats)
        analyzer.thread_pool_analyzer = mock_thread_pool_analyzer
        
        recommendations = await analyzer.analyze()
        
        assert recommendations == []
        assert isinstance(recommendations, list)
        mock_thread_pool_analyzer.analyze.assert_called_once()

    def test_format_version_string(self):
        """Test version string formatting."""
        mock_session = Mock()
        settings = CassandraSettings(mock_session, CassandraVersion(5, 0, 2))
        mock_thread_pool_stats = Mock(spec=ThreadPoolStats)
        
        analyzer = ConfigurationAnalyzer(settings, mock_thread_pool_stats)
        
        version_str = analyzer._format_version_string()
        
        assert version_str == "5.0.2"

    def test_format_version_string_snapshot(self):
        """Test version string formatting for snapshot versions."""
        mock_session = Mock()
        settings = CassandraSettings(mock_session, CassandraVersion(5, 1, 0))
        mock_thread_pool_stats = Mock(spec=ThreadPoolStats)
        
        analyzer = ConfigurationAnalyzer(settings, mock_thread_pool_stats)
        
        version_str = analyzer._format_version_string()
        
        assert version_str == "5.1.0"

    @pytest.mark.asyncio
    async def test_analyze_preserves_settings(self):
        """Test that analyzer preserves settings reference for future queries."""
        mock_session = Mock()
        settings = CassandraSettings(mock_session, CassandraVersion(4, 0, 0))
        mock_thread_pool_stats = Mock(spec=ThreadPoolStats)
        
        # Mock the thread pool analyzer
        mock_thread_pool_analyzer = Mock()
        mock_thread_pool_analyzer.analyze = AsyncMock(return_value=[])
        
        analyzer = ConfigurationAnalyzer(settings, mock_thread_pool_stats)
        analyzer.thread_pool_analyzer = mock_thread_pool_analyzer
        
        # Call analyze
        await analyzer.analyze()
        
        # Settings should still be available for future rule implementations
        assert analyzer.settings == settings

    @pytest.mark.asyncio
    async def test_analyzer_with_different_versions(self):
        """Test analyzer handles different version formats correctly."""
        mock_session = Mock()
        mock_thread_pool_stats = Mock(spec=ThreadPoolStats)
        
        # Mock the thread pool analyzer
        mock_thread_pool_analyzer = Mock()
        mock_thread_pool_analyzer.analyze = AsyncMock(return_value=[])
        
        # Test various version formats
        versions = [
            CassandraVersion(3, 11, 15),
            CassandraVersion(4, 0, 0),
            CassandraVersion(4, 1, 0),
            CassandraVersion(5, 0, 0),
            CassandraVersion(5, 0, 1),
        ]
        
        for version in versions:
            settings = CassandraSettings(mock_session, version)
            analyzer = ConfigurationAnalyzer(settings, mock_thread_pool_stats)
            analyzer.thread_pool_analyzer = mock_thread_pool_analyzer
            assert analyzer.settings.version.major == version.major
            assert analyzer.settings.version.minor == version.minor
            assert analyzer.settings.version.patch == version.patch
            
            # Should not raise any errors
            recommendations = await analyzer.analyze()
    
    @pytest.mark.asyncio
    async def test_analyze_includes_thread_pool_recommendations(self):
        """Test that analyze includes thread pool analyzer recommendations."""
        mock_session = Mock()
        settings = CassandraSettings(mock_session, CassandraVersion(5, 0, 0))
        mock_thread_pool_stats = Mock(spec=ThreadPoolStats)
        
        # Create sample thread pool recommendations as Recommendation objects
        thread_pool_recs = [
            Recommendation(
                recommendation="Increase native_transport_max_threads",
                category=RecommendationCategory.CAPACITY,
                priority=RecommendationPriority.HIGH,
                reason="At 95% capacity",
                current="100",
                suggested="200",
                pool_name="Native-Transport-Requests"
            )
        ]
        
        # Mock the thread pool analyzer
        mock_thread_pool_analyzer = Mock()
        mock_thread_pool_analyzer.analyze = AsyncMock(return_value=thread_pool_recs)
        
        analyzer = ConfigurationAnalyzer(settings, mock_thread_pool_stats)
        analyzer.thread_pool_analyzer = mock_thread_pool_analyzer
        
        recommendations = await analyzer.analyze()
        
        # Should include thread pool recommendations
        assert len(recommendations) == 1
        assert recommendations[0].category == RecommendationCategory.CAPACITY
        assert recommendations[0].pool_name == "Native-Transport-Requests"
        mock_thread_pool_analyzer.analyze.assert_called_once()
        assert isinstance(recommendations, list)
        assert isinstance(recommendations[0], Recommendation)