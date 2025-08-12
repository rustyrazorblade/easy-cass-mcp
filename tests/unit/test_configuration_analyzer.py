"""Unit tests for the ConfigurationAnalyzer module.

This module tests the configuration recommendation system
for Cassandra clusters.
"""

from unittest.mock import Mock

import pytest

from ecm.configuration_analyzer import ConfigurationAnalyzer


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
        """Test ConfigurationAnalyzer creation with session and version."""
        mock_session = Mock()
        version = (5, 0, 0)
        
        analyzer = ConfigurationAnalyzer(mock_session, version)
        
        assert analyzer.session == mock_session
        assert analyzer.cassandra_version == version
        assert analyzer.major_version == 5
        assert analyzer.minor_version == 0
        assert analyzer.patch_version == 0

    @pytest.mark.asyncio
    async def test_analyzer_creation_cassandra4(self):
        """Test ConfigurationAnalyzer with Cassandra 4.x version."""
        mock_session = Mock()
        version = (4, 1, 3)
        
        analyzer = ConfigurationAnalyzer(mock_session, version)
        
        assert analyzer.cassandra_version == version
        assert analyzer.major_version == 4
        assert analyzer.minor_version == 1
        assert analyzer.patch_version == 3

    @pytest.mark.asyncio
    async def test_analyze_empty(self):
        """Test analyze returns empty list initially."""
        mock_session = Mock()
        analyzer = ConfigurationAnalyzer(mock_session, (5, 0, 0))
        
        recommendations = await analyzer.analyze()
        
        assert recommendations == []
        assert isinstance(recommendations, list)

    def test_format_version_string(self):
        """Test version string formatting."""
        mock_session = Mock()
        analyzer = ConfigurationAnalyzer(mock_session, (5, 0, 2))
        
        version_str = analyzer._format_version_string()
        
        assert version_str == "5.0.2"

    def test_format_version_string_snapshot(self):
        """Test version string formatting for snapshot versions."""
        mock_session = Mock()
        analyzer = ConfigurationAnalyzer(mock_session, (5, 1, 0))
        
        version_str = analyzer._format_version_string()
        
        assert version_str == "5.1.0"

    @pytest.mark.asyncio
    async def test_analyze_preserves_session(self):
        """Test that analyzer preserves session reference for future queries."""
        mock_session = Mock()
        analyzer = ConfigurationAnalyzer(mock_session, (4, 0, 0))
        
        # Call analyze
        await analyzer.analyze()
        
        # Session should still be available for future rule implementations
        assert analyzer.session == mock_session

    @pytest.mark.asyncio
    async def test_analyzer_with_different_versions(self):
        """Test analyzer handles different version formats correctly."""
        mock_session = Mock()
        
        # Test various version formats
        versions = [
            (3, 11, 15),
            (4, 0, 0),
            (4, 1, 0),
            (5, 0, 0),
            (5, 0, 1),
        ]
        
        for version in versions:
            analyzer = ConfigurationAnalyzer(mock_session, version)
            assert analyzer.major_version == version[0]
            assert analyzer.minor_version == version[1]
            assert analyzer.patch_version == version[2]
            
            # Should not raise any errors
            recommendations = await analyzer.analyze()
            assert isinstance(recommendations, list)