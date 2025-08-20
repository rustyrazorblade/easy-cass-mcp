import logging
from typing import Any, Dict, List

from .cassandra_table import CassandraTable
from .cassandra_version import CassandraVersion
from .constants import STCS_CLASS, UCS_CLASS, UCS_MIN_VERSION

logger = logging.getLogger(__name__)


class CompactionAnalyzer:
    """Analyzes table compaction strategies and provides optimization recommendations."""

    def __init__(
        self, table: CassandraTable, cassandra_version: CassandraVersion
    ) -> None:
        self.table = table
        self.cassandra_version = cassandra_version
        self.major_version = cassandra_version.major

    async def analyze(self) -> List[Dict[str, Any]]:
        """Analyze the table's compaction strategy and return optimization recommendations.

        Returns:
            List of optimization recommendations, each containing:
            - type: Type of optimization (e.g., 'compaction_strategy')
            - current: Current configuration
            - recommendation: Recommended configuration
            - reason: Explanation for the recommendation
            - reference: Optional reference URL
        """
        optimizations = []

        # Get current compaction strategy
        compaction_info = await self.table.get_compaction_strategy()
        compaction_class = compaction_info["class"]

        # Check for STCS in Cassandra 5+
        if self._should_recommend_ucs(compaction_class):
            optimizations.append(self._create_ucs_recommendation())

        return optimizations

    def _should_recommend_ucs(self, compaction_class: str) -> bool:
        """Check if UCS should be recommended based on current strategy and version."""
        return (
            STCS_CLASS in compaction_class and self.cassandra_version >= UCS_MIN_VERSION
        )

    def _create_ucs_recommendation(self) -> Dict[str, Any]:
        """Create a recommendation for switching to UCS."""
        return {
            "type": "compaction_strategy",
            "current": f"{STCS_CLASS} (STCS)",
            "recommendation": (f"{UCS_CLASS} (UCS) with scaling_parameters: T4"),
            "reason": (
                "UCS with T4 scaling parameters provides better "
                "performance and more predictable latencies "
                "compared to STCS in Cassandra 5.0+"
            ),
            "reference": (
                "https://rustyrazorblade.com/post/2025/"
                "07-compaction-strategies-and-performance/"
            ),
        }
