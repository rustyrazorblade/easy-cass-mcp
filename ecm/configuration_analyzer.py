"""Configuration analyzer for Cassandra clusters.

This module provides recommendations for configuration improvements
based on Cassandra version and current settings.
"""

import logging
from typing import List

from .cassandra_settings import CassandraSettings
from .recommendation import Recommendation
from .thread_pool_analyzer import ThreadPoolAnalyzer
from .thread_pool_stats import ThreadPoolStats

logger = logging.getLogger(__name__)


class ConfigurationAnalyzer:
    """Analyzes cluster configuration and provides recommendations."""

    def __init__(self, settings: CassandraSettings, thread_pool_stats: ThreadPoolStats) -> None:
        """Initialize the configuration analyzer.
        
        Args:
            settings: CassandraSettings instance with normalized cluster settings
            thread_pool_stats: ThreadPoolStats instance with cluster thread pool data
        """
        self.settings = settings
        self.thread_pool_stats = thread_pool_stats
        self.thread_pool_analyzer = ThreadPoolAnalyzer(thread_pool_stats, settings)

    async def analyze(self) -> List[Recommendation]:
        """Analyze configuration and return recommendations.
        
        Returns:
            List of Recommendation objects containing configuration and performance recommendations.
            
        Note:
            Includes thread pool analysis and configuration recommendations.
        """
        recommendations = []
        
        # Analyze thread pool statistics
        thread_pool_recommendations = await self.thread_pool_analyzer.analyze()
        recommendations.extend(thread_pool_recommendations)
        
        # Placeholder for future configuration rules
        # Settings are available through self.settings
        # Version is available through self.settings.version
        # 
        # Example structure for future rules:
        # if self.settings.version.major >= 5:
        #     recommendations.extend(self._check_cassandra5_config())
        # if self.settings.version.major >= 4:
        #     recommendations.extend(self._check_cassandra4_config())
        
        return recommendations

    def _format_version_string(self) -> str:
        """Format the version as a string for display."""
        return str(self.settings.version)