"""Thread pool analyzer for Cassandra clusters.

This module analyzes thread pool statistics and provides recommendations
for tuning thread pool configurations to optimize performance.
"""

import logging
from typing import List

from .cassandra_settings import CassandraSettings
from .recommendation import Recommendation, RecommendationCategory, RecommendationPriority
from .thread_pool_stats import ThreadPoolStats, ThreadPoolStat

logger = logging.getLogger(__name__)


class ThreadPoolAnalyzer:
    """Analyzes thread pool statistics and provides tuning recommendations."""

    # Thread pool thresholds for recommendations
    BLOCKED_THRESHOLD = 0  # Any blocked tasks trigger a recommendation
    PENDING_HIGH_THRESHOLD = 100  # High pending tasks threshold
    PENDING_WARNING_THRESHOLD = 50  # Warning level for pending tasks
    CAPACITY_WARNING_RATIO = 0.8  # Warn when pool is at 80% capacity
    CAPACITY_CRITICAL_RATIO = 0.95  # Critical when pool is at 95% capacity

    def __init__(self, thread_pool_stats: ThreadPoolStats, settings: CassandraSettings) -> None:
        """Initialize the thread pool analyzer.
        
        Args:
            thread_pool_stats: ThreadPoolStats instance with cluster thread pool data
            settings: CassandraSettings instance with cluster configuration
        """
        self.stats = thread_pool_stats
        self.settings = settings

    async def analyze(self) -> List[Recommendation]:
        """Analyze thread pool statistics and return recommendations.
        
        Returns:
            List of recommendation dictionaries, each containing:
            - recommendation: Brief description of the recommendation
            - category: Category (performance, capacity, backpressure, configuration)
            - priority: Priority level (high, medium, low)
            - reason: Detailed explanation
            - current: Current configuration/state
            - suggested: Suggested configuration/action
            - pool_name: Name of the affected thread pool
        """
        recommendations = []
        
        # Ensure stats are loaded
        if not self.stats.is_loaded():
            await self.stats.load_stats()
        
        # Analyze each type of thread pool
        recommendations.extend(self._analyze_native_transport())
        recommendations.extend(self._analyze_read_stage())
        recommendations.extend(self._analyze_compaction())
        recommendations.extend(self._analyze_memtable_flush())
        recommendations.extend(self._analyze_blocked_pools())
        recommendations.extend(self._analyze_pending_backlog())
        
        return recommendations

    def _analyze_native_transport(self) -> List[Recommendation]:
        """Analyze Native-Transport-Requests thread pool.
        
        This pool handles client requests and is critical for request handling performance.
        """
        recommendations = []
        pool = self.stats.native_transport_requests
        
        if not pool:
            return recommendations
        
        # Check if at capacity
        if pool.active > 0:
            utilization = pool.active / pool.active_limit if pool.active_limit > 0 else 0
            
            if utilization >= self.CAPACITY_CRITICAL_RATIO:
                current_threads = self.settings.get_setting('native_transport_max_threads', pool.active_limit)
                recommendations.append(Recommendation(
                    recommendation="Increase native_transport_max_threads",
                    category=RecommendationCategory.CAPACITY,
                    priority=RecommendationPriority.HIGH,
                    reason=f"Native transport thread pool is at {utilization:.0%} capacity, which may cause request queueing and increased latency",
                    current=f"native_transport_max_threads: {current_threads}",
                    suggested=f"native_transport_max_threads: {current_threads * 2}",
                    pool_name="Native-Transport-Requests"
                ))
            elif utilization >= self.CAPACITY_WARNING_RATIO:
                current_threads = self.settings.get_setting('native_transport_max_threads', pool.active_limit)
                recommendations.append(Recommendation(
                    recommendation="Consider increasing native_transport_max_threads",
                    category=RecommendationCategory.CAPACITY,
                    priority=RecommendationPriority.MEDIUM,
                    reason=f"Native transport thread pool is at {utilization:.0%} capacity",
                    current=f"native_transport_max_threads: {current_threads}",
                    suggested=f"native_transport_max_threads: {int(current_threads * 1.5)}",
                    pool_name="Native-Transport-Requests"
                ))
        
        return recommendations

    def _analyze_read_stage(self) -> List[Recommendation]:
        """Analyze ReadStage thread pool.
        
        This pool handles read requests and directly impacts read latency.
        """
        recommendations = []
        pool = self.stats.read_stage
        
        if not pool:
            return recommendations
        
        # Check pending reads
        current_reads = self.settings.get_setting('concurrent_reads', pool.active_limit)
        if pool.pending > self.PENDING_HIGH_THRESHOLD:
            recommendations.append(Recommendation(
                recommendation="Increase concurrent_reads to handle read backlog",
                category=RecommendationCategory.PERFORMANCE,
                priority=RecommendationPriority.HIGH,
                reason=f"ReadStage has {pool.pending} pending tasks, indicating read throughput bottleneck",
                current=f"concurrent_reads: {current_reads}",
                suggested=f"concurrent_reads: {min(current_reads * 2, 128)}",
                pool_name="ReadStage"
            ))
        elif pool.pending > self.PENDING_WARNING_THRESHOLD:
            recommendations.append(Recommendation(
                recommendation="Monitor read performance, consider increasing concurrent_reads",
                category=RecommendationCategory.PERFORMANCE,
                priority=RecommendationPriority.MEDIUM,
                reason=f"ReadStage has {pool.pending} pending tasks",
                current=f"concurrent_reads: {current_reads}",
                suggested=f"concurrent_reads: {min(int(current_reads * 1.5), 96)}",
                pool_name="ReadStage"
            ))
        
        return recommendations

    def _analyze_compaction(self) -> List[Recommendation]:
        """Analyze CompactionExecutor thread pool.
        
        This pool handles compaction tasks which are crucial for read performance.
        """
        recommendations = []
        pool = self.stats.compaction_executor
        
        if not pool:
            return recommendations
        
        # Check for compaction backlog
        current_compactors = self.settings.get_setting('concurrent_compactors', pool.active_limit)
        if pool.pending > 10:
            recommendations.append(Recommendation(
                recommendation="Increase concurrent_compactors to reduce compaction backlog",
                category=RecommendationCategory.PERFORMANCE,
                priority=RecommendationPriority.MEDIUM,
                reason=f"CompactionExecutor has {pool.pending} pending tasks, which may impact read performance",
                current=f"concurrent_compactors: {current_compactors}",
                suggested=f"concurrent_compactors: {min(current_compactors + 1, 4)}",
                pool_name="CompactionExecutor"
            ))
        
        # Check if compaction is disabled (limit = 0)
        if current_compactors == 0:
            recommendations.append(Recommendation(
                recommendation="Compaction appears to be disabled",
                category=RecommendationCategory.CONFIGURATION,
                priority=RecommendationPriority.HIGH,
                reason="CompactionExecutor has no threads allocated, compaction may be disabled",
                current="concurrent_compactors: 0",
                suggested="concurrent_compactors: 2",
                pool_name="CompactionExecutor"
            ))
        
        return recommendations

    def _analyze_memtable_flush(self) -> List[Recommendation]:
        """Analyze MemtableFlushWriter thread pool.
        
        This pool handles memtable flushes which impact write performance.
        """
        recommendations = []
        pool = self.stats.memtable_flush_writer
        
        if not pool:
            return recommendations
        
        # Check for flush backlog
        current_flush_writers = self.settings.get_setting('memtable_flush_writers', pool.active_limit)
        if pool.pending > 5:
            recommendations.append(Recommendation(
                recommendation="Increase memtable_flush_writers to handle write load",
                category=RecommendationCategory.PERFORMANCE,
                priority=RecommendationPriority.HIGH,
                reason=f"MemtableFlushWriter has {pool.pending} pending flushes, which may cause write blocking",
                current=f"memtable_flush_writers: {current_flush_writers}",
                suggested=f"memtable_flush_writers: {min(current_flush_writers + 1, 4)}",
                pool_name="MemtableFlushWriter"
            ))
        
        return recommendations

    def _analyze_blocked_pools(self) -> List[Recommendation]:
        """Analyze all thread pools for blocked tasks.
        
        Blocked tasks indicate serious performance issues that need immediate attention.
        """
        recommendations = []
        blocked_pools = self.stats.get_blocked_pools()
        
        for pool in blocked_pools:
            if pool.blocked > self.BLOCKED_THRESHOLD:
                # Determine the pool name from the stats
                pool_name = self._get_pool_name(pool)
                
                recommendations.append(Recommendation(
                    recommendation=f"Investigate blocked tasks in {pool_name}",
                    category=RecommendationCategory.BACKPRESSURE,
                    priority=RecommendationPriority.HIGH,
                    reason=f"{pool_name} has {pool.blocked} blocked tasks (total blocked all time: {pool.blocked_all_time}), indicating severe resource contention",
                    current=f"Blocked tasks: {pool.blocked}",
                    suggested="Review system resources (CPU, I/O), check for lock contention, consider increasing pool size",
                    pool_name=pool_name
                ))
        
        return recommendations

    def _analyze_pending_backlog(self) -> List[Recommendation]:
        """Analyze all thread pools for high pending task counts.
        
        High pending tasks indicate the system cannot keep up with the workload.
        """
        recommendations = []
        pools_with_pending = self.stats.get_pools_with_pending()
        
        # We've already analyzed specific pools, so look for others
        analyzed_pools = {
            "Native-Transport-Requests",
            "ReadStage",
            "CompactionExecutor",
            "MemtableFlushWriter"
        }
        
        for pool in pools_with_pending:
            pool_name = self._get_pool_name(pool)
            
            # Skip if already analyzed
            if pool_name in analyzed_pools:
                continue
            
            if pool.pending > self.PENDING_HIGH_THRESHOLD:
                priority = RecommendationPriority.HIGH
            elif pool.pending > self.PENDING_WARNING_THRESHOLD:
                priority = RecommendationPriority.MEDIUM
            else:
                continue
            
            recommendations.append(Recommendation(
                recommendation=f"High pending tasks in {pool_name}",
                category=RecommendationCategory.PERFORMANCE,
                priority=priority,
                reason=f"{pool_name} has {pool.pending} pending tasks, indicating processing backlog",
                current=f"Pending tasks: {pool.pending}, Active limit: {pool.active_limit}",
                suggested=f"Consider increasing thread pool size for {pool_name}",
                pool_name=pool_name
            ))
        
        return recommendations

    def _get_pool_name(self, pool: ThreadPoolStat) -> str:
        """Get the name of a thread pool from the stats.
        
        This is a helper method to find the pool name by comparing object references.
        """
        # Check each pool property to find which one matches
        pool_mapping = {
            "CacheCleanupExecutor": self.stats.cache_cleanup_executor,
            "CompactionExecutor": self.stats.compaction_executor,
            "GossipStage": self.stats.gossip_stage,
            "HintsDispatcher": self.stats.hints_dispatcher,
            "MemtableFlushWriter": self.stats.memtable_flush_writer,
            "MemtablePostFlush": self.stats.memtable_post_flush,
            "MemtableReclaimMemory": self.stats.memtable_reclaim_memory,
            "MigrationStage": self.stats.migration_stage,
            "Native-Transport-Auth-Requests": self.stats.native_transport_auth_requests,
            "Native-Transport-Requests": self.stats.native_transport_requests,
            "PendingRangeCalculator": self.stats.pending_range_calculator,
            "PerDiskMemtableFlushWriter_0": self.stats.per_disk_memtable_flush_writer_0,
            "ReadStage": self.stats.read_stage,
            "Sampler": self.stats.sampler,
            "SecondaryIndexExecutor": self.stats.secondary_index_executor,
            "SecondaryIndexManagement": self.stats.secondary_index_management,
            "StatusPropagationExecutor": self.stats.status_propagation_executor,
            "ValidationExecutor": self.stats.validation_executor,
            "ViewBuildExecutor": self.stats.view_build_executor,
        }
        
        for name, stat in pool_mapping.items():
            if stat is pool:
                return name
        
        return "Unknown"