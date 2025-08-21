"""Unit tests for ThreadPoolStats class.

Tests thread pool statistics retrieval and management.
"""

from unittest.mock import Mock

import pytest

from ecm.thread_pool_stats import ThreadPoolStat, ThreadPoolStats


class TestThreadPoolStats:
    """Tests for ThreadPoolStats class."""
    
    @pytest.fixture
    def mock_session(self):
        """Create a mock Cassandra session."""
        return Mock()
    
    @pytest.fixture
    def sample_thread_pool_data(self):
        """Create sample thread pool data matching actual Cassandra output."""
        # Create mock row objects with attributes
        rows = []
        
        # Sample data based on user's provided output
        data = [
            ("CacheCleanupExecutor", 0, 1, 0, 0, 0, 0),
            ("CompactionExecutor", 0, 2, 0, 0, 139, 0),
            ("GossipStage", 0, 1, 0, 0, 0, 0),
            ("HintsDispatcher", 0, 2, 0, 0, 0, 0),
            ("MemtableFlushWriter", 0, 2, 0, 0, 5, 0),
            ("MemtablePostFlush", 0, 1, 0, 0, 6, 0),
            ("MemtableReclaimMemory", 0, 1, 0, 0, 5, 0),
            ("MigrationStage", 0, 1, 0, 0, 0, 0),
            ("Native-Transport-Auth-Requests", 0, 4, 0, 0, 0, 0),
            ("Native-Transport-Requests", 1, 128, 0, 0, 37, 0),
            ("PendingRangeCalculator", 0, 1, 0, 0, 2, 0),
            ("PerDiskMemtableFlushWriter_0", 0, 2, 0, 0, 3, 0),
            ("ReadStage", 0, 32, 0, 0, 13, 0),
            ("Sampler", 0, 1, 0, 0, 0, 0),
            ("SecondaryIndexExecutor", 0, 2, 0, 0, 0, 0),
            ("SecondaryIndexManagement", 0, 1, 0, 0, 1, 0),
            ("StatusPropagationExecutor", 0, 1, 0, 0, 0, 0),
            ("ValidationExecutor", 0, 2, 0, 0, 0, 0),
            ("ViewBuildExecutor", 0, 1, 0, 0, 0, 0),
        ]
        
        for (name, active, limit, blocked, blocked_all, completed, pending) in data:
            row = Mock()
            row.name = name
            row.active_tasks = active
            row.active_tasks_limit = limit
            row.blocked_tasks = blocked
            row.blocked_tasks_all_time = blocked_all
            row.completed_tasks = completed
            row.pending_tasks = pending
            rows.append(row)
        
        return rows
    
    @pytest.fixture
    def thread_pool_stats(self, mock_session):
        """Create ThreadPoolStats instance with mock session."""
        return ThreadPoolStats(mock_session)
    
    def test_initialization(self, thread_pool_stats, mock_session):
        """Test ThreadPoolStats initialization."""
        assert thread_pool_stats.session == mock_session
        assert thread_pool_stats._pools == {}
        assert thread_pool_stats._loaded is False
    
    @pytest.mark.asyncio
    async def test_load_stats(self, thread_pool_stats, mock_session, sample_thread_pool_data):
        """Test loading thread pool statistics."""
        # Mock the query result
        mock_result = Mock()
        mock_result.__iter__ = Mock(return_value=iter(sample_thread_pool_data))
        mock_session.execute = Mock(return_value=mock_result)
        
        # Load statistics
        await thread_pool_stats.load_stats()
        
        # Verify the query was executed
        mock_session.execute.assert_called_once()
        query = mock_session.execute.call_args[0][0]
        assert "system_views.thread_pools" in query
        
        # Verify pools were loaded
        assert thread_pool_stats._loaded is True
        assert len(thread_pool_stats._pools) == 19
        
        # Verify specific pool data
        compaction = thread_pool_stats.get_pool("CompactionExecutor")
        assert compaction is not None
        assert compaction.active == 0
        assert compaction.active_limit == 2
        assert compaction.completed == 139
        
        # Test that second load doesn't query again
        mock_session.execute.reset_mock()
        await thread_pool_stats.load_stats()
        mock_session.execute.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_properties_access(self, thread_pool_stats, mock_session, sample_thread_pool_data):
        """Test accessing thread pools via properties."""
        # Mock and load data
        mock_result = Mock()
        mock_result.__iter__ = Mock(return_value=iter(sample_thread_pool_data))
        mock_session.execute = Mock(return_value=mock_result)
        
        await thread_pool_stats.load_stats()
        
        # Test various properties
        assert thread_pool_stats.cache_cleanup_executor is not None
        assert thread_pool_stats.cache_cleanup_executor.active_limit == 1
        
        assert thread_pool_stats.compaction_executor is not None
        assert thread_pool_stats.compaction_executor.completed == 139
        
        assert thread_pool_stats.native_transport_requests is not None
        assert thread_pool_stats.native_transport_requests.active == 1
        assert thread_pool_stats.native_transport_requests.active_limit == 128
        assert thread_pool_stats.native_transport_requests.completed == 37
        
        assert thread_pool_stats.read_stage is not None
        assert thread_pool_stats.read_stage.active_limit == 32
        assert thread_pool_stats.read_stage.completed == 13
    
    @pytest.mark.asyncio
    async def test_get_pool(self, thread_pool_stats, mock_session, sample_thread_pool_data):
        """Test getting pools by name."""
        # Mock and load data
        mock_result = Mock()
        mock_result.__iter__ = Mock(return_value=iter(sample_thread_pool_data))
        mock_session.execute = Mock(return_value=mock_result)
        
        await thread_pool_stats.load_stats()
        
        # Test getting existing pool
        pool = thread_pool_stats.get_pool("ReadStage")
        assert pool is not None
        assert pool.active_limit == 32
        
        # Test getting non-existent pool
        pool = thread_pool_stats.get_pool("NonExistentPool")
        assert pool is None
    
    @pytest.mark.asyncio
    async def test_get_all_pools(self, thread_pool_stats, mock_session, sample_thread_pool_data):
        """Test getting all pools."""
        # Mock and load data
        mock_result = Mock()
        mock_result.__iter__ = Mock(return_value=iter(sample_thread_pool_data))
        mock_session.execute = Mock(return_value=mock_result)
        
        await thread_pool_stats.load_stats()
        
        all_pools = thread_pool_stats.get_all_pools()
        assert len(all_pools) == 19
        assert "CompactionExecutor" in all_pools
        assert "ReadStage" in all_pools
        assert "Native-Transport-Requests" in all_pools
    
    @pytest.mark.asyncio
    async def test_refresh(self, thread_pool_stats, mock_session, sample_thread_pool_data):
        """Test refreshing statistics."""
        # Mock and load data
        mock_result = Mock()
        mock_result.__iter__ = Mock(return_value=iter(sample_thread_pool_data))
        mock_session.execute = Mock(return_value=mock_result)
        
        await thread_pool_stats.load_stats()
        assert thread_pool_stats._loaded is True
        
        # Reset mock and refresh
        mock_session.execute.reset_mock()
        await thread_pool_stats.refresh()
        
        # Should query again
        mock_session.execute.assert_called_once()
        assert thread_pool_stats._loaded is True
    
    @pytest.mark.asyncio
    async def test_get_high_activity_pools(self, thread_pool_stats, mock_session):
        """Test getting high activity pools."""
        # Create data with some active tasks
        rows = []
        for name, active in [("Pool1", 15), ("Pool2", 5), ("Pool3", 20)]:
            row = Mock()
            row.name = name
            row.active_tasks = active
            row.active_tasks_limit = 100
            row.blocked_tasks = 0
            row.blocked_tasks_all_time = 0
            row.completed_tasks = 0
            row.pending_tasks = 0
            rows.append(row)
        
        mock_result = Mock()
        mock_result.__iter__ = Mock(return_value=iter(rows))
        mock_session.execute = Mock(return_value=mock_result)
        
        await thread_pool_stats.load_stats()
        
        # Test with default threshold (10)
        high_activity = thread_pool_stats.get_high_activity_pools()
        assert len(high_activity) == 2
        # Pool1 has 15 active, Pool3 has 20 active
        
        # Test with custom threshold
        high_activity = thread_pool_stats.get_high_activity_pools(threshold=20)
        assert len(high_activity) == 1
        # Only Pool3 has 20 active
    
    @pytest.mark.asyncio
    async def test_get_blocked_pools(self, thread_pool_stats, mock_session):
        """Test getting pools with blocked tasks."""
        # Create data with some blocked tasks
        rows = []
        for name, blocked in [("Pool1", 0), ("Pool2", 5), ("Pool3", 10)]:
            row = Mock()
            row.name = name
            row.active_tasks = 0
            row.active_tasks_limit = 100
            row.blocked_tasks = blocked
            row.blocked_tasks_all_time = blocked * 2
            row.completed_tasks = 0
            row.pending_tasks = 0
            rows.append(row)
        
        mock_result = Mock()
        mock_result.__iter__ = Mock(return_value=iter(rows))
        mock_session.execute = Mock(return_value=mock_result)
        
        await thread_pool_stats.load_stats()
        
        blocked = thread_pool_stats.get_blocked_pools()
        assert len(blocked) == 2
        # Pool2 and Pool3 have blocked tasks
    
    @pytest.mark.asyncio
    async def test_get_pools_with_pending(self, thread_pool_stats, mock_session):
        """Test getting pools with pending tasks."""
        # Create data with some pending tasks
        rows = []
        for name, pending in [("Pool1", 0), ("Pool2", 10), ("Pool3", 5)]:
            row = Mock()
            row.name = name
            row.active_tasks = 0
            row.active_tasks_limit = 100
            row.blocked_tasks = 0
            row.blocked_tasks_all_time = 0
            row.completed_tasks = 0
            row.pending_tasks = pending
            rows.append(row)
        
        mock_result = Mock()
        mock_result.__iter__ = Mock(return_value=iter(rows))
        mock_session.execute = Mock(return_value=mock_result)
        
        await thread_pool_stats.load_stats()
        
        with_pending = thread_pool_stats.get_pools_with_pending()
        assert len(with_pending) == 2
        # Pool2 and Pool3 have pending tasks
    
    @pytest.mark.asyncio
    async def test_get_pool_summary(self, thread_pool_stats, mock_session, sample_thread_pool_data):
        """Test getting pool summary."""
        # Mock and load data
        mock_result = Mock()
        mock_result.__iter__ = Mock(return_value=iter(sample_thread_pool_data))
        mock_session.execute = Mock(return_value=mock_result)
        
        await thread_pool_stats.load_stats()
        
        summary = thread_pool_stats.get_pool_summary()
        
        assert len(summary) == 19
        assert "CompactionExecutor" in summary
        
        compaction_summary = summary["CompactionExecutor"]
        assert compaction_summary["active"] == 0
        assert compaction_summary["limit"] == 2
        assert compaction_summary["blocked"] == 0
        assert compaction_summary["blocked_all_time"] == 0
        assert compaction_summary["completed"] == 139
        assert compaction_summary["pending"] == 0
    
    @pytest.mark.asyncio
    async def test_error_handling(self, thread_pool_stats, mock_session):
        """Test error handling during load."""
        # Mock execute to raise an exception
        mock_session.execute = Mock(side_effect=Exception("Connection error"))
        
        # Should raise and still mark as loaded to prevent repeated failures
        with pytest.raises(Exception, match="Connection error"):
            await thread_pool_stats.load_stats()
        
        assert thread_pool_stats._loaded is True
    
    def test_is_loaded(self, thread_pool_stats):
        """Test checking if statistics are loaded."""
        assert thread_pool_stats.is_loaded() is False
        
        thread_pool_stats._loaded = True
        assert thread_pool_stats.is_loaded() is True
    
    def test_thread_pool_stat_dataclass(self):
        """Test ThreadPoolStat dataclass."""
        pool = ThreadPoolStat(
            active=5,
            active_limit=100,
            blocked=2,
            blocked_all_time=10,
            completed=1000,
            pending=3
        )
        
        assert pool.active == 5
        assert pool.active_limit == 100
        assert pool.blocked == 2
        assert pool.blocked_all_time == 10
        assert pool.completed == 1000
        assert pool.pending == 3
        
        # Test default values
        pool_defaults = ThreadPoolStat()
        assert pool_defaults.active == 0
        assert pool_defaults.active_limit == 0
        assert pool_defaults.blocked == 0
        assert pool_defaults.blocked_all_time == 0
        assert pool_defaults.completed == 0
        assert pool_defaults.pending == 0