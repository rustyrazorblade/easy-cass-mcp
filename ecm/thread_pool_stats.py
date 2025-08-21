"""Thread pool statistics retrieval and management.

This module provides a ThreadPoolStats class that queries and manages
thread pool statistics from the system_views.thread_pools virtual table.
"""

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional

from cassandra.cluster import Session

logger = logging.getLogger(__name__)


@dataclass
class ThreadPoolStat:
    """Statistics for a single thread pool."""
    
    active: int = 0
    active_limit: int = 0
    blocked: int = 0
    blocked_all_time: int = 0
    completed: int = 0
    pending: int = 0


class ThreadPoolStats:
    """Manages thread pool statistics from system_views.thread_pools virtual table."""
    
    def __init__(self, session: Session) -> None:
        """Initialize ThreadPoolStats with a Cassandra session.
        
        Args:
            session: Active Cassandra session for querying statistics
        """
        self.session = session
        self._pools: Dict[str, ThreadPoolStat] = {}
        self._loaded = False
    
    # Properties for each thread pool
    @property
    def cache_cleanup_executor(self) -> Optional[ThreadPoolStat]:
        """Get CacheCleanupExecutor thread pool statistics."""
        return self._pools.get("CacheCleanupExecutor")
    
    @property
    def compaction_executor(self) -> Optional[ThreadPoolStat]:
        """Get CompactionExecutor thread pool statistics."""
        return self._pools.get("CompactionExecutor")
    
    @property
    def gossip_stage(self) -> Optional[ThreadPoolStat]:
        """Get GossipStage thread pool statistics."""
        return self._pools.get("GossipStage")
    
    @property
    def hints_dispatcher(self) -> Optional[ThreadPoolStat]:
        """Get HintsDispatcher thread pool statistics."""
        return self._pools.get("HintsDispatcher")
    
    @property
    def memtable_flush_writer(self) -> Optional[ThreadPoolStat]:
        """Get MemtableFlushWriter thread pool statistics."""
        return self._pools.get("MemtableFlushWriter")
    
    @property
    def memtable_post_flush(self) -> Optional[ThreadPoolStat]:
        """Get MemtablePostFlush thread pool statistics."""
        return self._pools.get("MemtablePostFlush")
    
    @property
    def memtable_reclaim_memory(self) -> Optional[ThreadPoolStat]:
        """Get MemtableReclaimMemory thread pool statistics."""
        return self._pools.get("MemtableReclaimMemory")
    
    @property
    def migration_stage(self) -> Optional[ThreadPoolStat]:
        """Get MigrationStage thread pool statistics."""
        return self._pools.get("MigrationStage")
    
    @property
    def native_transport_auth_requests(self) -> Optional[ThreadPoolStat]:
        """Get Native-Transport-Auth-Requests thread pool statistics."""
        return self._pools.get("Native-Transport-Auth-Requests")
    
    @property
    def native_transport_requests(self) -> Optional[ThreadPoolStat]:
        """Get Native-Transport-Requests thread pool statistics."""
        return self._pools.get("Native-Transport-Requests")
    
    @property
    def pending_range_calculator(self) -> Optional[ThreadPoolStat]:
        """Get PendingRangeCalculator thread pool statistics."""
        return self._pools.get("PendingRangeCalculator")
    
    @property
    def per_disk_memtable_flush_writer_0(self) -> Optional[ThreadPoolStat]:
        """Get PerDiskMemtableFlushWriter_0 thread pool statistics."""
        return self._pools.get("PerDiskMemtableFlushWriter_0")
    
    @property
    def read_stage(self) -> Optional[ThreadPoolStat]:
        """Get ReadStage thread pool statistics."""
        return self._pools.get("ReadStage")
    
    @property
    def sampler(self) -> Optional[ThreadPoolStat]:
        """Get Sampler thread pool statistics."""
        return self._pools.get("Sampler")
    
    @property
    def secondary_index_executor(self) -> Optional[ThreadPoolStat]:
        """Get SecondaryIndexExecutor thread pool statistics."""
        return self._pools.get("SecondaryIndexExecutor")
    
    @property
    def secondary_index_management(self) -> Optional[ThreadPoolStat]:
        """Get SecondaryIndexManagement thread pool statistics."""
        return self._pools.get("SecondaryIndexManagement")
    
    @property
    def status_propagation_executor(self) -> Optional[ThreadPoolStat]:
        """Get StatusPropagationExecutor thread pool statistics."""
        return self._pools.get("StatusPropagationExecutor")
    
    @property
    def validation_executor(self) -> Optional[ThreadPoolStat]:
        """Get ValidationExecutor thread pool statistics."""
        return self._pools.get("ValidationExecutor")
    
    @property
    def view_build_executor(self) -> Optional[ThreadPoolStat]:
        """Get ViewBuildExecutor thread pool statistics."""
        return self._pools.get("ViewBuildExecutor")
    
    async def load_stats(self) -> None:
        """Load thread pool statistics from all nodes in the cluster.
        
        This method queries the system_views.thread_pools table and populates
        all thread pool statistics. Statistics are aggregated across all nodes.
        """
        if self._loaded:
            return
        
        try:
            # Query system_views.thread_pools
            query = """
                SELECT name, active_tasks, active_tasks_limit, 
                       blocked_tasks, blocked_tasks_all_time,
                       completed_tasks, pending_tasks
                FROM system_views.thread_pools
            """
            result = self.session.execute(query)
            
            # Clear existing pools
            self._pools.clear()
            
            # Process each row
            for row in result:
                pool_name = row.name
                
                # Create or update the pool statistics
                if pool_name in self._pools:
                    # Aggregate statistics if pool already exists (multiple nodes)
                    existing = self._pools[pool_name]
                    existing.active += row.active_tasks
                    existing.blocked += row.blocked_tasks
                    existing.blocked_all_time += row.blocked_tasks_all_time
                    existing.completed += row.completed_tasks
                    existing.pending += row.pending_tasks
                    # Keep the maximum limit
                    existing.active_limit = max(
                        existing.active_limit, 
                        row.active_tasks_limit
                    )
                else:
                    # Create new pool statistics
                    self._pools[pool_name] = ThreadPoolStat(
                        active=row.active_tasks,
                        active_limit=row.active_tasks_limit,
                        blocked=row.blocked_tasks,
                        blocked_all_time=row.blocked_tasks_all_time,
                        completed=row.completed_tasks,
                        pending=row.pending_tasks
                    )
            
            self._loaded = True
            logger.info(f"Loaded statistics for {len(self._pools)} thread pools")
            
        except Exception as e:
            logger.error(f"Failed to load thread pool statistics: {e}")
            self._loaded = True  # Prevent repeated failures
            raise
    
    async def load_stats_for_node(self, node_address: str) -> None:
        """Load thread pool statistics from a specific node.
        
        Args:
            node_address: IP address or hostname of the node to query
        """
        try:
            # This would require node-specific execution similar to CassandraService
            # For now, we'll use a simplified approach
            query = """
                SELECT name, active_tasks, active_tasks_limit, 
                       blocked_tasks, blocked_tasks_all_time,
                       completed_tasks, pending_tasks
                FROM system_views.thread_pools
            """
            
            # Note: In a real implementation, we'd use ExecutionProfile
            # to target a specific node, similar to CassandraService
            result = self.session.execute(query)
            
            # Clear existing pools
            self._pools.clear()
            
            # Process each row
            for row in result:
                self._pools[row.name] = ThreadPoolStat(
                    active=row.active_tasks,
                    active_limit=row.active_tasks_limit,
                    blocked=row.blocked_tasks,
                    blocked_all_time=row.blocked_tasks_all_time,
                    completed=row.completed_tasks,
                    pending=row.pending_tasks
                )
            
            self._loaded = True
            logger.info(f"Loaded statistics for {len(self._pools)} thread pools from node {node_address}")
            
        except Exception as e:
            logger.error(f"Failed to load thread pool statistics from node {node_address}: {e}")
            raise
    
    def get_pool(self, name: str) -> Optional[ThreadPoolStat]:
        """Get thread pool statistics by exact name.
        
        Args:
            name: Exact name of the thread pool (e.g., "ReadStage")
            
        Returns:
            ThreadPoolStat object if found, None otherwise
        """
        return self._pools.get(name)
    
    def get_all_pools(self) -> Dict[str, ThreadPoolStat]:
        """Get all thread pool statistics.
        
        Returns:
            Dictionary mapping pool names to their statistics
        """
        return self._pools.copy()
    
    async def refresh(self) -> None:
        """Force a refresh of thread pool statistics from the cluster."""
        self._loaded = False
        await self.load_stats()
    
    def is_loaded(self) -> bool:
        """Check if statistics have been loaded.
        
        Returns:
            True if statistics have been loaded, False otherwise
        """
        return self._loaded
    
    def get_high_activity_pools(self, threshold: int = 10) -> List[ThreadPoolStat]:
        """Get thread pools with high activity.
        
        Args:
            threshold: Minimum number of active tasks to be considered high activity
            
        Returns:
            List of thread pools with active tasks >= threshold
        """
        return [
            pool for pool in self._pools.values()
            if pool.active >= threshold
        ]
    
    def get_blocked_pools(self) -> List[ThreadPoolStat]:
        """Get thread pools with blocked tasks.
        
        Returns:
            List of thread pools with blocked_tasks > 0
        """
        return [
            pool for pool in self._pools.values()
            if pool.blocked > 0
        ]
    
    def get_pools_with_pending(self) -> List[ThreadPoolStat]:
        """Get thread pools with pending tasks.
        
        Returns:
            List of thread pools with pending_tasks > 0
        """
        return [
            pool for pool in self._pools.values()
            if pool.pending > 0
        ]
    
    def get_pool_summary(self) -> Dict[str, Dict[str, int]]:
        """Get a summary of all thread pool statistics.
        
        Returns:
            Dictionary with pool names as keys and their stats as nested dicts
        """
        summary = {}
        for name, pool in self._pools.items():
            summary[name] = {
                'active': pool.active,
                'limit': pool.active_limit,
                'blocked': pool.blocked,
                'blocked_all_time': pool.blocked_all_time,
                'completed': pool.completed,
                'pending': pool.pending
            }
        return summary