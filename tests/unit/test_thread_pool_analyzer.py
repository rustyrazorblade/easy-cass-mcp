"""Unit tests for ThreadPoolAnalyzer class."""

import pytest
from unittest.mock import Mock, AsyncMock

from ecm.cassandra_settings import CassandraSettings
from ecm.cassandra_version import CassandraVersion
from ecm.thread_pool_analyzer import ThreadPoolAnalyzer
from ecm.thread_pool_stats import ThreadPoolStats, ThreadPoolStat
from ecm.recommendation import Recommendation, RecommendationCategory, RecommendationPriority


@pytest.fixture
def mock_thread_pool_stats():
    """Create a mock ThreadPoolStats instance."""
    stats = Mock(spec=ThreadPoolStats)
    stats.is_loaded = Mock(return_value=False)
    stats.load_stats = AsyncMock()
    
    # Default all pools to None
    stats.cache_cleanup_executor = None
    stats.compaction_executor = None
    stats.gossip_stage = None
    stats.hints_dispatcher = None
    stats.memtable_flush_writer = None
    stats.memtable_post_flush = None
    stats.memtable_reclaim_memory = None
    stats.migration_stage = None
    stats.native_transport_auth_requests = None
    stats.native_transport_requests = None
    stats.pending_range_calculator = None
    stats.per_disk_memtable_flush_writer_0 = None
    stats.read_stage = None
    stats.sampler = None
    stats.secondary_index_executor = None
    stats.secondary_index_management = None
    stats.status_propagation_executor = None
    stats.validation_executor = None
    stats.view_build_executor = None
    
    # Default empty lists for get methods
    stats.get_blocked_pools = Mock(return_value=[])
    stats.get_pools_with_pending = Mock(return_value=[])
    
    return stats


@pytest.fixture
def mock_cassandra_settings():
    """Create a mock CassandraSettings instance."""
    mock_session = Mock()
    settings = CassandraSettings(mock_session, CassandraVersion(5, 0, 0))
    
    # Mock the get_setting method to return reasonable defaults
    def get_setting_side_effect(key, default=None):
        settings_map = {
            'native_transport_max_threads': 128,
            'concurrent_reads': 32,
            'concurrent_compactors': 2,
            'memtable_flush_writers': 2
        }
        return settings_map.get(key, default)
    
    settings.get_setting = Mock(side_effect=get_setting_side_effect)
    return settings


class TestThreadPoolAnalyzer:
    """Tests for ThreadPoolAnalyzer class."""
    
    @pytest.mark.asyncio
    async def test_analyzer_creation(self, mock_thread_pool_stats, mock_cassandra_settings):
        """Test ThreadPoolAnalyzer creation."""
        analyzer = ThreadPoolAnalyzer(mock_thread_pool_stats, mock_cassandra_settings)
        assert analyzer.stats == mock_thread_pool_stats
        assert analyzer.settings == mock_cassandra_settings
    
    @pytest.mark.asyncio
    async def test_analyze_loads_stats_if_needed(self, mock_thread_pool_stats, mock_cassandra_settings):
        """Test that analyze loads stats if not already loaded."""
        analyzer = ThreadPoolAnalyzer(mock_thread_pool_stats, mock_cassandra_settings)
        
        await analyzer.analyze()
        
        mock_thread_pool_stats.is_loaded.assert_called_once()
        mock_thread_pool_stats.load_stats.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_analyze_doesnt_reload_stats(self, mock_thread_pool_stats, mock_cassandra_settings):
        """Test that analyze doesn't reload stats if already loaded."""
        mock_thread_pool_stats.is_loaded.return_value = True
        analyzer = ThreadPoolAnalyzer(mock_thread_pool_stats, mock_cassandra_settings)
        
        await analyzer.analyze()
        
        mock_thread_pool_stats.is_loaded.assert_called_once()
        mock_thread_pool_stats.load_stats.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_native_transport_at_critical_capacity(self, mock_thread_pool_stats, mock_cassandra_settings):
        """Test native transport at critical capacity recommendation."""
        # Create native transport pool at 95% capacity
        native_pool = ThreadPoolStat(
            active=95,
            active_limit=100,
            blocked=0,
            blocked_all_time=0,
            completed=1000,
            pending=0
        )
        mock_thread_pool_stats.native_transport_requests = native_pool
        mock_thread_pool_stats.is_loaded.return_value = True
        
        analyzer = ThreadPoolAnalyzer(mock_thread_pool_stats, mock_cassandra_settings)
        recommendations = await analyzer.analyze()
        
        assert len(recommendations) == 1
        rec = recommendations[0]
        assert rec.category == RecommendationCategory.CAPACITY
        assert rec.priority == RecommendationPriority.HIGH
        assert "95%" in rec.reason
        assert rec.pool_name == "Native-Transport-Requests"
        assert "native_transport_max_threads: 256" in rec.suggested  # 128 * 2
    
    @pytest.mark.asyncio
    async def test_native_transport_at_warning_capacity(self, mock_thread_pool_stats, mock_cassandra_settings):
        """Test native transport at warning capacity recommendation."""
        # Create native transport pool at 80% capacity
        native_pool = ThreadPoolStat(
            active=80,
            active_limit=100,
            blocked=0,
            blocked_all_time=0,
            completed=1000,
            pending=0
        )
        mock_thread_pool_stats.native_transport_requests = native_pool
        mock_thread_pool_stats.is_loaded.return_value = True
        
        analyzer = ThreadPoolAnalyzer(mock_thread_pool_stats, mock_cassandra_settings)
        recommendations = await analyzer.analyze()
        
        assert len(recommendations) == 1
        rec = recommendations[0]
        assert rec.category == RecommendationCategory.CAPACITY
        assert rec.priority == RecommendationPriority.MEDIUM
        assert "80%" in rec.reason
        assert "native_transport_max_threads: 192" in rec.suggested  # int(128 * 1.5)
    
    @pytest.mark.asyncio
    async def test_read_stage_high_pending(self, mock_thread_pool_stats, mock_cassandra_settings):
        """Test read stage with high pending tasks."""
        read_pool = ThreadPoolStat(
            active=10,
            active_limit=32,
            blocked=0,
            blocked_all_time=0,
            completed=5000,
            pending=150  # High pending
        )
        mock_thread_pool_stats.read_stage = read_pool
        mock_thread_pool_stats.is_loaded.return_value = True
        
        analyzer = ThreadPoolAnalyzer(mock_thread_pool_stats, mock_cassandra_settings)
        recommendations = await analyzer.analyze()
        
        assert len(recommendations) == 1
        rec = recommendations[0]
        assert rec.category == RecommendationCategory.PERFORMANCE
        assert rec.priority == RecommendationPriority.HIGH
        assert "150 pending tasks" in rec.reason
        assert rec.pool_name == "ReadStage"
        assert "concurrent_reads: 64" in rec.suggested  # 32 * 2
    
    @pytest.mark.asyncio
    async def test_read_stage_warning_pending(self, mock_thread_pool_stats, mock_cassandra_settings):
        """Test read stage with warning level pending tasks."""
        read_pool = ThreadPoolStat(
            active=10,
            active_limit=32,
            blocked=0,
            blocked_all_time=0,
            completed=5000,
            pending=60  # Warning level
        )
        mock_thread_pool_stats.read_stage = read_pool
        mock_thread_pool_stats.is_loaded.return_value = True
        
        analyzer = ThreadPoolAnalyzer(mock_thread_pool_stats, mock_cassandra_settings)
        recommendations = await analyzer.analyze()
        
        assert len(recommendations) == 1
        rec = recommendations[0]
        assert rec.category == RecommendationCategory.PERFORMANCE
        assert rec.priority == RecommendationPriority.MEDIUM
        assert "60 pending tasks" in rec.reason
        assert "concurrent_reads: 48" in rec.suggested  # int(32 * 1.5)
    
    @pytest.mark.asyncio
    async def test_compaction_backlog(self, mock_thread_pool_stats, mock_cassandra_settings):
        """Test compaction executor with backlog."""
        compaction_pool = ThreadPoolStat(
            active=2,
            active_limit=2,
            blocked=0,
            blocked_all_time=0,
            completed=100,
            pending=15  # Backlog
        )
        mock_thread_pool_stats.compaction_executor = compaction_pool
        mock_thread_pool_stats.is_loaded.return_value = True
        
        analyzer = ThreadPoolAnalyzer(mock_thread_pool_stats, mock_cassandra_settings)
        recommendations = await analyzer.analyze()
        
        assert len(recommendations) == 1
        rec = recommendations[0]
        assert rec.category == RecommendationCategory.PERFORMANCE
        assert rec.priority == RecommendationPriority.MEDIUM
        assert "15 pending tasks" in rec.reason
        assert rec.pool_name == "CompactionExecutor"
        assert "concurrent_compactors: 3" in rec.suggested  # 2 + 1
    
    @pytest.mark.asyncio
    async def test_compaction_disabled(self, mock_thread_pool_stats, mock_cassandra_settings):
        """Test compaction executor disabled."""
        compaction_pool = ThreadPoolStat(
            active=0,
            active_limit=0,  # Disabled
            blocked=0,
            blocked_all_time=0,
            completed=0,
            pending=0
        )
        mock_thread_pool_stats.compaction_executor = compaction_pool
        mock_thread_pool_stats.is_loaded.return_value = True
        
        # Override the concurrent_compactors setting to 0 for this test
        mock_cassandra_settings.get_setting = Mock(side_effect=lambda key, default=None: 0 if key == 'concurrent_compactors' else default)
        
        analyzer = ThreadPoolAnalyzer(mock_thread_pool_stats, mock_cassandra_settings)
        recommendations = await analyzer.analyze()
        
        assert len(recommendations) == 1
        rec = recommendations[0]
        assert rec.category == RecommendationCategory.CONFIGURATION
        assert rec.priority == RecommendationPriority.HIGH
        assert "disabled" in rec.reason
        assert "concurrent_compactors: 2" in rec.suggested
    
    @pytest.mark.asyncio
    async def test_memtable_flush_backlog(self, mock_thread_pool_stats, mock_cassandra_settings):
        """Test memtable flush writer with backlog."""
        flush_pool = ThreadPoolStat(
            active=2,
            active_limit=2,
            blocked=0,
            blocked_all_time=0,
            completed=50,
            pending=8  # Backlog
        )
        mock_thread_pool_stats.memtable_flush_writer = flush_pool
        mock_thread_pool_stats.is_loaded.return_value = True
        
        analyzer = ThreadPoolAnalyzer(mock_thread_pool_stats, mock_cassandra_settings)
        recommendations = await analyzer.analyze()
        
        assert len(recommendations) == 1
        rec = recommendations[0]
        assert rec.category == RecommendationCategory.PERFORMANCE
        assert rec.priority == RecommendationPriority.HIGH
        assert "8 pending flushes" in rec.reason
        assert rec.pool_name == "MemtableFlushWriter"
        assert "memtable_flush_writers: 3" in rec.suggested  # 2 + 1
    
    @pytest.mark.asyncio
    async def test_blocked_pools(self, mock_thread_pool_stats, mock_cassandra_settings):
        """Test detection of blocked tasks."""
        # Create a pool with blocked tasks
        blocked_pool = ThreadPoolStat(
            active=10,
            active_limit=10,
            blocked=5,  # Blocked tasks
            blocked_all_time=100,
            completed=1000,
            pending=20
        )
        
        mock_thread_pool_stats.read_stage = blocked_pool
        mock_thread_pool_stats.get_blocked_pools.return_value = [blocked_pool]
        mock_thread_pool_stats.is_loaded.return_value = True
        
        analyzer = ThreadPoolAnalyzer(mock_thread_pool_stats, mock_cassandra_settings)
        recommendations = await analyzer.analyze()
        
        # Should have recommendations for blocked tasks
        blocked_recs = [r for r in recommendations if r.category == RecommendationCategory.BACKPRESSURE]
        assert len(blocked_recs) >= 1
        rec = blocked_recs[0]
        assert rec.priority == RecommendationPriority.HIGH
        assert "5 blocked tasks" in rec.reason
        assert "resource contention" in rec.reason
    
    @pytest.mark.asyncio
    async def test_other_pools_with_high_pending(self, mock_thread_pool_stats, mock_cassandra_settings):
        """Test detection of high pending in other pools."""
        # Create a pool with high pending tasks
        pending_pool = ThreadPoolStat(
            active=1,
            active_limit=1,
            blocked=0,
            blocked_all_time=0,
            completed=100,
            pending=120  # High pending
        )
        
        mock_thread_pool_stats.migration_stage = pending_pool
        mock_thread_pool_stats.get_pools_with_pending.return_value = [pending_pool]
        mock_thread_pool_stats.is_loaded.return_value = True
        
        analyzer = ThreadPoolAnalyzer(mock_thread_pool_stats, mock_cassandra_settings)
        recommendations = await analyzer.analyze()
        
        # Should have recommendation for high pending
        pending_recs = [r for r in recommendations 
                       if "pending tasks" in r.reason 
                       and r.pool_name == "MigrationStage"]
        assert len(pending_recs) == 1
        rec = pending_recs[0]
        assert rec.priority == RecommendationPriority.HIGH
        assert "120 pending tasks" in rec.reason
    
    @pytest.mark.asyncio
    async def test_no_recommendations_when_healthy(self, mock_thread_pool_stats, mock_cassandra_settings):
        """Test no recommendations when all pools are healthy."""
        # Create healthy pools
        healthy_pool = ThreadPoolStat(
            active=5,
            active_limit=32,
            blocked=0,
            blocked_all_time=0,
            completed=1000,
            pending=0
        )
        
        mock_thread_pool_stats.native_transport_requests = healthy_pool
        mock_thread_pool_stats.read_stage = healthy_pool
        mock_thread_pool_stats.compaction_executor = ThreadPoolStat(
            active=1, active_limit=2, blocked=0, 
            blocked_all_time=0, completed=100, pending=0
        )
        mock_thread_pool_stats.is_loaded.return_value = True
        
        analyzer = ThreadPoolAnalyzer(mock_thread_pool_stats, mock_cassandra_settings)
        recommendations = await analyzer.analyze()
        
        assert len(recommendations) == 0
    
    @pytest.mark.asyncio
    async def test_multiple_issues_detected(self, mock_thread_pool_stats, mock_cassandra_settings):
        """Test multiple issues detected across different pools."""
        # Native transport at capacity
        mock_thread_pool_stats.native_transport_requests = ThreadPoolStat(
            active=95, active_limit=100, blocked=0,
            blocked_all_time=0, completed=1000, pending=0
        )
        
        # Read stage with pending
        mock_thread_pool_stats.read_stage = ThreadPoolStat(
            active=10, active_limit=32, blocked=0,
            blocked_all_time=0, completed=5000, pending=150
        )
        
        # Compaction disabled
        mock_thread_pool_stats.compaction_executor = ThreadPoolStat(
            active=0, active_limit=0, blocked=0,
            blocked_all_time=0, completed=0, pending=0
        )
        
        mock_thread_pool_stats.is_loaded.return_value = True
        
        # Override the concurrent_compactors setting to 0 for this test
        def get_setting_side_effect(key, default=None):
            if key == 'concurrent_compactors':
                return 0
            elif key == 'native_transport_max_threads':
                return 128
            elif key == 'concurrent_reads':
                return 32
            else:
                return default
        
        mock_cassandra_settings.get_setting = Mock(side_effect=get_setting_side_effect)
        
        analyzer = ThreadPoolAnalyzer(mock_thread_pool_stats, mock_cassandra_settings)
        recommendations = await analyzer.analyze()
        
        assert len(recommendations) >= 3
        
        # Check for each type of issue
        categories = {r.category for r in recommendations}
        assert RecommendationCategory.CAPACITY in categories
        assert RecommendationCategory.PERFORMANCE in categories
        assert RecommendationCategory.CONFIGURATION in categories
    
    def test_get_pool_name(self, mock_thread_pool_stats, mock_cassandra_settings):
        """Test _get_pool_name helper method."""
        # Create a pool and assign it to a property
        test_pool = ThreadPoolStat(
            active=1, active_limit=2, blocked=0,
            blocked_all_time=0, completed=10, pending=0
        )
        mock_thread_pool_stats.compaction_executor = test_pool
        
        analyzer = ThreadPoolAnalyzer(mock_thread_pool_stats, mock_cassandra_settings)
        
        # Should identify the pool name correctly
        name = analyzer._get_pool_name(test_pool)
        assert name == "CompactionExecutor"
        
        # Unknown pool should return "Unknown"
        unknown_pool = ThreadPoolStat(
            active=0, active_limit=0, blocked=0,
            blocked_all_time=0, completed=0, pending=0
        )
        name = analyzer._get_pool_name(unknown_pool)
        assert name == "Unknown"