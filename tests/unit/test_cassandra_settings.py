"""Unit tests for CassandraSettings class.

Tests settings normalization and retrieval across Cassandra versions.
"""

from unittest.mock import Mock

import pytest

from ecm.cassandra_settings import (
    AuditLoggingOptions,
    CassandraSettings,
    ClientEncryptionOptions,
    ServerEncryptionOptions,
)
from ecm.cassandra_version import CassandraVersion


class TestCassandraSettings:
    """Tests for CassandraSettings class."""

    @pytest.fixture
    def mock_session(self):
        """Create a mock Cassandra session."""
        return Mock()

    @pytest.fixture
    def settings_v5(self, mock_session):
        """Create CassandraSettings for Cassandra 5.0."""
        version = CassandraVersion(5, 0, 0)
        return CassandraSettings(mock_session, version)

    @pytest.fixture
    def settings_v4(self, mock_session):
        """Create CassandraSettings for Cassandra 4.0."""
        version = CassandraVersion(4, 0, 11)
        return CassandraSettings(mock_session, version)

    def test_initialization(self, mock_session):
        """Test CassandraSettings initialization."""
        version = CassandraVersion(5, 0, 1)
        settings = CassandraSettings(mock_session, version)
        
        assert settings.session == mock_session
        assert settings.version == version
        assert settings._loaded is False
        
        # Check nested options are initialized
        assert isinstance(settings.audit_logging_options, AuditLoggingOptions)
        assert isinstance(settings.client_encryption_options, ClientEncryptionOptions)
        assert isinstance(settings.server_encryption_options, ServerEncryptionOptions)
        
        # Check some default values
        assert settings.cluster_name == "Test Cluster"
        assert settings.native_transport_port == 9042
        assert settings.storage_port == 7000

    @pytest.mark.asyncio
    async def test_load_settings_only_once(self, settings_v5, mock_session):
        """Test that load_settings only loads once."""
        # Mock the query result
        mock_result = Mock()
        mock_result.current_rows = 0
        mock_result.__iter__ = Mock(return_value=iter([]))
        mock_session.execute = Mock(return_value=mock_result)
        
        # First load
        await settings_v5.load_settings()
        assert settings_v5._loaded is True
        mock_session.execute.assert_called_once()
        
        # Reset mock
        mock_session.execute.reset_mock()
        
        # Second load should not query again
        await settings_v5.load_settings()
        mock_session.execute.assert_not_called()

    def test_get_setting_default(self, settings_v5):
        """Test get_setting returns default when key not found."""
        default_value = "default"
        result = settings_v5.get_setting("nonexistent_key", default_value)
        assert result == default_value
        
    def test_get_setting_direct(self, settings_v5):
        """Test get_setting returns direct attributes."""
        settings_v5.cluster_name = "MyCluster"
        result = settings_v5.get_setting("cluster_name")
        assert result == "MyCluster"
        
    def test_get_setting_nested(self, settings_v5):
        """Test get_setting returns nested attributes."""
        settings_v5.audit_logging_options.enabled = True
        result = settings_v5.get_setting("audit_logging_options.enabled")
        assert result is True

    def test_value_conversion_bool(self, settings_v5):
        """Test boolean value conversion."""
        assert settings_v5._convert_value("enabled", "true") is True
        assert settings_v5._convert_value("enabled", "false") is False
        assert settings_v5._convert_value("enabled", "null") is None
        
    def test_value_conversion_numeric(self, settings_v5):
        """Test numeric value conversion."""
        assert settings_v5._convert_value("timeout_in_ms", "5000") == 5000
        assert settings_v5._convert_value("size_in_mb", "100") == 100
        assert settings_v5._convert_value("threshold", "0.95") == 0.95
        
    def test_value_conversion_list(self, settings_v5):
        """Test list value conversion."""
        assert settings_v5._parse_list("[]") == []
        assert settings_v5._parse_list("[a, b, c]") == ["a", "b", "c"]
        
    def test_value_conversion_dict(self, settings_v5):
        """Test dictionary value conversion."""
        assert settings_v5._parse_dict("{}") == {}
        assert settings_v5._parse_dict("{key=value}") == {"key": "value"}
        
    def test_is_duration(self, settings_v5):
        """Test duration string detection."""
        assert settings_v5._is_duration("10s") is True
        assert settings_v5._is_duration("5m") is True
        assert settings_v5._is_duration("3h") is True
        assert settings_v5._is_duration("100ms") is True
        assert settings_v5._is_duration("not_duration") is False
        
    def test_is_size(self, settings_v5):
        """Test size string detection."""
        assert settings_v5._is_size("100MiB") is True
        assert settings_v5._is_size("1KiB") is True
        assert settings_v5._is_size("5GB") is True
        assert settings_v5._is_size("not_size") is False

    def test_get_all_settings(self, settings_v5):
        """Test get_all_settings returns all settings."""
        result = settings_v5.get_all_settings()
        assert isinstance(result, dict)
        # Should include direct settings
        assert "cluster_name" in result
        assert "native_transport_port" in result
        # Should include nested settings objects
        assert "audit_logging_options" in result
        assert "client_encryption_options" in result

    def test_convenience_properties(self, settings_v5):
        """Test convenience properties."""
        # Authentication disabled by default
        assert settings_v5.is_authentication_enabled is False
        settings_v5.authenticator_class_name = "PasswordAuthenticator"
        assert settings_v5.is_authentication_enabled is True
        
        # Authorization disabled by default
        assert settings_v5.is_authorization_enabled is False
        settings_v5.authorizer_class_name = "CassandraAuthorizer"
        assert settings_v5.is_authorization_enabled is True
        
        # Encryption disabled by default
        assert settings_v5.is_encryption_enabled is False
        settings_v5.client_encryption_options.enabled = True
        assert settings_v5.is_encryption_enabled is True
        
        # Audit logging disabled by default
        assert settings_v5.is_audit_logging_enabled is False
        settings_v5.audit_logging_options.enabled = True
        assert settings_v5.is_audit_logging_enabled is True

    @pytest.mark.asyncio
    async def test_refresh_settings(self, settings_v5, mock_session):
        """Test refresh_settings clears cache and reloads."""
        # Mock the query result
        mock_result = Mock()
        mock_result.current_rows = 0
        mock_result.__iter__ = Mock(return_value=iter([]))
        settings_v5.session.execute = Mock(return_value=mock_result)
        
        # Set initial state
        settings_v5._loaded = True
        settings_v5.cluster_name = "OldCluster"
        
        # Refresh
        await settings_v5.refresh_settings()
        
        # Check state was reset and reloaded
        assert settings_v5._loaded is True
        # Should have queried the database
        settings_v5.session.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_load_settings_with_data(self, mock_session):
        """Test loading settings from database."""
        version = CassandraVersion(5, 0, 0)
        settings = CassandraSettings(mock_session, version)
        
        # Create proper mock rows
        class MockRow:
            def __init__(self, name, value):
                self.name = name
                self.value = value
        
        # Mock query result with some sample settings
        mock_rows = [
            MockRow("cluster_name", "Production Cluster"),
            MockRow("native_transport_port", "9043"),
            MockRow("concurrent_reads", "64"),
            MockRow("audit_logging_options.enabled", "true"),
            MockRow("client_encryption_options.enabled", "false"),
        ]
        mock_result = Mock()
        mock_result.current_rows = len(mock_rows)
        # Make result iterable
        mock_result.__iter__ = lambda self: iter(mock_rows)
        mock_session.execute = Mock(return_value=mock_result)
        
        await settings.load_settings()
        
        # Verify settings were loaded
        assert settings.cluster_name == "Production Cluster"
        assert settings.native_transport_port == 9043
        assert settings.concurrent_reads == 64
        assert settings.audit_logging_options.enabled is True
        assert settings.client_encryption_options.enabled is False

    @pytest.mark.asyncio
    async def test_version_compatibility(self, mock_session):
        """Test handling of older Cassandra versions."""
        # Cassandra 3.x should not query system_views.settings
        version = CassandraVersion(3, 11, 16)
        settings = CassandraSettings(mock_session, version)
        
        await settings.load_settings()
        
        # Should not have queried
        mock_session.execute.assert_not_called()
        assert settings._loaded is True

    def test_version_specific_settings(self, mock_session):
        """Test that different versions can be handled."""
        # Cassandra 3.x
        v3_settings = CassandraSettings(mock_session, CassandraVersion(3, 11, 16))
        assert v3_settings.version.major == 3
        
        # Cassandra 4.x
        v4_settings = CassandraSettings(mock_session, CassandraVersion(4, 1, 0))
        assert v4_settings.version.major == 4
        
        # Cassandra 5.x
        v5_settings = CassandraSettings(mock_session, CassandraVersion(5, 0, 0))
        assert v5_settings.version.major == 5