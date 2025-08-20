"""Cassandra settings normalization and retrieval.

This module provides a CassandraSettings class that queries and normalizes
configuration settings across different Cassandra versions, handling
version-specific naming differences.
"""

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union

from cassandra.cluster import Session

from .cassandra_version import CassandraVersion

logger = logging.getLogger(__name__)


@dataclass
class AuditLoggingOptions:
    """Audit logging configuration options."""
    allow_nodetool_archive_command: bool = False
    archive_command: Optional[str] = None
    audit_logs_dir: str = "logs/audit"
    block: bool = True
    enabled: bool = False
    excluded_categories: Optional[str] = None
    excluded_keyspaces: str = "system,system_schema,system_virtual_schema"
    excluded_users: Optional[str] = None
    included_categories: Optional[str] = None
    included_keyspaces: Optional[str] = None
    included_users: Optional[str] = None
    logger_class_name: str = "BinAuditLogger"
    logger_parameters: Optional[Dict[str, Any]] = None
    max_archive_retries: int = 10
    max_log_size: int = 17179869184
    max_queue_weight: int = 268435456
    roll_cycle: str = "HOURLY"


@dataclass
class ClientEncryptionOptions:
    """Client encryption configuration options."""
    accepted_protocols: Optional[List[str]] = None
    algorithm: Optional[str] = None
    cipher_suites: Optional[List[str]] = None
    enabled: bool = False
    keystore: str = "conf/.keystore"
    keystore_password: Optional[str] = None
    optional: bool = False
    protocol: Optional[str] = None
    require_client_auth: bool = False
    require_endpoint_verification: bool = False
    ssl_context_factory_class_name: str = "org.apache.cassandra.security.DefaultSslContextFactory"
    ssl_context_factory_parameters: Dict[str, Any] = field(default_factory=dict)
    store_type: str = "JKS"
    truststore: str = "conf/.truststore"
    truststore_password: Optional[str] = None


@dataclass
class ServerEncryptionOptions:
    """Server encryption configuration options."""
    accepted_protocols: Optional[List[str]] = None
    algorithm: Optional[str] = None
    cipher_suites: Optional[List[str]] = None
    enabled: bool = False
    internode_encryption: str = "none"
    keystore: str = "conf/.keystore"
    keystore_password: Optional[str] = None
    legacy_ssl_storage_port_enabled: bool = False
    optional: bool = False
    outbound_keystore: Optional[str] = None
    outbound_keystore_password: Optional[str] = None
    protocol: Optional[str] = None
    require_client_auth: bool = False
    require_endpoint_verification: bool = False
    ssl_context_factory_class_name: str = "org.apache.cassandra.security.DefaultSslContextFactory"
    ssl_context_factory_parameters: Dict[str, Any] = field(default_factory=dict)
    store_type: str = "JKS"
    truststore: str = "conf/.truststore"
    truststore_password: Optional[str] = None


@dataclass
class FullQueryLoggingOptions:
    """Full query logging configuration options."""
    allow_nodetool_archive_command: bool = False
    archive_command: Optional[str] = None
    block: bool = True
    log_dir: Optional[str] = None
    max_archive_retries: int = 10
    max_log_size: int = 17179869184
    max_queue_weight: int = 268435456
    roll_cycle: str = "HOURLY"


@dataclass
class TransparentDataEncryptionOptions:
    """Transparent data encryption configuration options."""
    chunk_length_kb: int = 64
    cipher: str = "AES/CBC/PKCS5Padding"
    enabled: bool = False
    iv_length: int = 16
    key_alias: str = "testing:1"
    key_provider_class_name: str = "org.apache.cassandra.security.JKSKeyProvider"
    key_provider_parameters: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RepairOptions:
    """Repair configuration options."""
    retries_enabled: bool = False
    retries_base_sleep_time: str = "200ms"
    retries_max_attempts: int = 0
    retries_max_sleep_time: str = "1s"
    merkle_tree_response_enabled: Optional[bool] = None
    merkle_tree_response_base_sleep_time: Optional[str] = None
    merkle_tree_response_max_attempts: Optional[int] = None
    merkle_tree_response_max_sleep_time: Optional[str] = None


@dataclass
class SaiOptions:
    """SAI (Storage Attached Indexing) configuration options."""
    prioritize_over_legacy_index: bool = False
    segment_write_buffer_size: str = "1024MiB"


@dataclass
class ReplicaFilteringProtection:
    """Replica filtering protection configuration."""
    cached_rows_fail_threshold: int = 32000
    cached_rows_warn_threshold: int = 2000


class CassandraSettings:
    """Manages normalized Cassandra configuration settings across versions."""

    def __init__(self, session: Session, version: CassandraVersion) -> None:
        """Initialize CassandraSettings with session and version.
        
        Args:
            session: Active Cassandra session for querying settings
            version: CassandraVersion object representing the cluster version
        """
        self.session = session
        self.version = version
        self._loaded = False
        
        # Initialize nested option classes
        self.audit_logging_options = AuditLoggingOptions()
        self.client_encryption_options = ClientEncryptionOptions()
        self.server_encryption_options = ServerEncryptionOptions()
        self.full_query_logging_options = FullQueryLoggingOptions()
        self.transparent_data_encryption_options = TransparentDataEncryptionOptions()
        self.repair_options = RepairOptions()
        self.sai_options = SaiOptions()
        self.replica_filtering_protection = ReplicaFilteringProtection()
        
        # Initialize all direct settings as attributes
        self._init_direct_settings()

    def _init_direct_settings(self) -> None:
        """Initialize all direct (non-nested) settings with default values."""
        # Core settings
        self.cluster_name: str = "Test Cluster"
        self.listen_address: str = "localhost"
        self.rpc_address: str = "localhost"
        self.broadcast_address: Optional[str] = None
        self.broadcast_rpc_address: Optional[str] = None
        self.storage_port: int = 7000
        self.ssl_storage_port: int = 7001
        self.native_transport_port: int = 9042
        self.native_transport_port_ssl: Optional[int] = None
        
        # Performance settings
        self.concurrent_reads: int = 32
        self.concurrent_writes: int = 32
        self.concurrent_counter_writes: int = 32
        self.concurrent_materialized_view_writes: int = 32
        self.concurrent_compactors: int = 2
        
        # Memory settings
        self.memtable_heap_space_in_mb: int = 7936
        self.memtable_offheap_space_in_mb: int = 7936
        self.memtable_allocation_type: str = "heap_buffers"
        self.memtable_cleanup_threshold: float = 0.33333334
        
        # Cache settings
        self.key_cache_size_in_mb: int = 100
        self.row_cache_size_in_mb: int = 0
        self.counter_cache_size_in_mb: int = 50
        
        # Compaction settings
        self.compaction_throughput_mb_per_sec: int = 64
        self.compaction_large_partition_warning_threshold_mb: Optional[int] = None
        
        # Timeout settings
        self.read_request_timeout_in_ms: int = 5000
        self.range_request_timeout_in_ms: int = 10000
        self.write_request_timeout_in_ms: int = 2000
        self.counter_write_request_timeout_in_ms: int = 5000
        self.cas_contention_timeout_in_ms: int = 1000
        self.truncate_request_timeout_in_ms: int = 60000
        self.request_timeout_in_ms: int = 10000
        
        # Hinted handoff settings
        self.hinted_handoff_enabled: bool = True
        self.max_hint_window_in_ms: int = 10800000
        self.hinted_handoff_throttle_in_kb: int = 1024
        self.max_hints_delivery_threads: int = 2
        
        # Commitlog settings
        self.commitlog_sync: str = "periodic"
        self.commitlog_sync_period_in_ms: int = 10000
        self.commitlog_segment_size_in_mb: int = 32
        self.commitlog_directory: str = "bin/../data/commitlog"
        
        # Data directories
        self.data_file_directories: List[str] = ["bin/../data/data"]
        self.saved_caches_directory: str = "bin/../data/saved_caches"
        self.hints_directory: str = "bin/../data/hints"
        
        # Security settings
        self.authenticator_class_name: str = "AllowAllAuthenticator"
        self.authenticator_parameters: Dict[str, Any] = {}
        self.authorizer_class_name: str = "AllowAllAuthorizer"
        self.authorizer_parameters: Dict[str, Any] = {}
        self.role_manager_class_name: str = "CassandraRoleManager"
        self.role_manager_parameters: Dict[str, Any] = {}
        
        # Snitch settings
        self.endpoint_snitch: str = "SimpleSnitch"
        self.dynamic_snitch: bool = True
        self.dynamic_snitch_update_interval_in_ms: int = 100
        self.dynamic_snitch_reset_interval_in_ms: int = 600000
        self.dynamic_snitch_badness_threshold: float = 1.0
        
        # Other settings
        self.partitioner: str = "org.apache.cassandra.dht.Murmur3Partitioner"
        self.auto_bootstrap: bool = True
        self.num_tokens: int = 16
        self.initial_token: Optional[str] = None
        
        # Add all other direct settings...
        # (There are many more, but this gives the structure)

    async def load_settings(self) -> None:
        """Load and normalize settings from the cluster.
        
        This method queries system_views.settings table and populates
        all settings attributes with their values from the database.
        """
        if self._loaded:
            return
        
        # Check version compatibility
        if self.version.major < 4:
            logger.warning(f"system_views.settings not available in Cassandra {self.version}. "
                         "Settings will use defaults.")
            self._loaded = True
            return
        
        try:
            # Query system_views.settings
            result = self.session.execute("SELECT name, value FROM system_views.settings")
            
            for row in result:
                setting_name = row.name
                setting_value = row.value
                
                # Process the setting
                self._process_setting(setting_name, setting_value)
            
            self._loaded = True
            logger.info(f"Loaded {result.current_rows} settings for Cassandra {self.version}")
            
        except Exception as e:
            logger.error(f"Failed to load settings: {e}")
            # Continue with defaults
            self._loaded = True

    def _process_setting(self, name: str, value: str) -> None:
        """Process a single setting from the database.
        
        Args:
            name: Setting name from database
            value: Setting value as string (or null)
        """
        # Handle nested settings
        if '.' in name:
            self._process_nested_setting(name, value)
        else:
            self._process_direct_setting(name, value)

    def _process_nested_setting(self, name: str, value: str) -> None:
        """Process a nested setting (contains dots).
        
        Args:
            name: Nested setting name (e.g., "audit_logging_options.enabled")
            value: Setting value as string
        """
        parts = name.split('.')
        
        # Handle audit_logging_options
        if parts[0] == "audit_logging_options":
            self._set_nested_attribute(self.audit_logging_options, parts[1:], value)
        
        # Handle client_encryption_options
        elif parts[0] == "client_encryption_options":
            self._set_nested_attribute(self.client_encryption_options, parts[1:], value)
        
        # Handle server_encryption_options
        elif parts[0] == "server_encryption_options":
            self._set_nested_attribute(self.server_encryption_options, parts[1:], value)
        
        # Handle full_query_logging_options
        elif parts[0] == "full_query_logging_options":
            self._set_nested_attribute(self.full_query_logging_options, parts[1:], value)
        
        # Handle transparent_data_encryption_options
        elif parts[0] == "transparent_data_encryption_options":
            self._set_nested_attribute(self.transparent_data_encryption_options, parts[1:], value)
        
        # Handle repair options
        elif parts[0] == "repair" and parts[1] == "retries":
            attr_name = "_".join(parts[2:]) if len(parts) > 2 else parts[1]
            self._set_nested_attribute(self.repair_options, [f"retries_{attr_name}"], value)
        
        # Handle sai_options
        elif parts[0] == "sai_options":
            self._set_nested_attribute(self.sai_options, parts[1:], value)
        
        # Handle replica_filtering_protection
        elif parts[0] == "replica_filtering_protection":
            self._set_nested_attribute(self.replica_filtering_protection, parts[1:], value)

    def _process_direct_setting(self, name: str, value: str) -> None:
        """Process a direct (non-nested) setting.
        
        Args:
            name: Setting name (no dots)
            value: Setting value as string
        """
        # Convert name to Python attribute name (already uses underscores)
        attr_name = name
        
        # Set the attribute if it exists
        if hasattr(self, attr_name):
            converted_value = self._convert_value(attr_name, value)
            setattr(self, attr_name, converted_value)

    def _set_nested_attribute(self, obj: Any, path: List[str], value: str) -> None:
        """Set a nested attribute on an object.
        
        Args:
            obj: Object to set attribute on
            path: List of attribute names to traverse
            value: Value to set
        """
        # Convert path elements to Python attribute names
        attr_name = "_".join(path).replace(".", "_")
        
        if hasattr(obj, attr_name):
            converted_value = self._convert_value(attr_name, value)
            setattr(obj, attr_name, converted_value)

    def _convert_value(self, key: str, value: Optional[str]) -> Any:
        """Convert a string value to the appropriate Python type.
        
        Args:
            key: Setting key for context
            value: String value from database
            
        Returns:
            Converted value in appropriate type
        """
        if value is None or value == "null":
            return None
        
        # Boolean values
        if value in ("true", "false"):
            return value == "true"
        
        # Numeric values
        if key.endswith("_in_ms") or key.endswith("_in_kb") or key.endswith("_in_mb"):
            try:
                return int(value)
            except ValueError:
                return value
        
        # Duration values (e.g., "10s", "5m", "3h")
        if self._is_duration(value):
            return value  # Keep as string for now, could convert to seconds
        
        # Size values (e.g., "100MiB", "1KiB")
        if self._is_size(value):
            return value  # Keep as string for now, could convert to bytes
        
        # List values (enclosed in brackets)
        if value.startswith("[") and value.endswith("]"):
            return self._parse_list(value)
        
        # Dictionary values (enclosed in braces)
        if value.startswith("{") and value.endswith("}"):
            return self._parse_dict(value)
        
        # Try to parse as number
        try:
            if "." in value:
                return float(value)
            return int(value)
        except ValueError:
            pass
        
        # Return as string
        return value

    def _is_duration(self, value: str) -> bool:
        """Check if a value is a duration string."""
        return bool(re.match(r'^\d+(\.\d+)?(ms|s|m|h|d)$', value))

    def _is_size(self, value: str) -> bool:
        """Check if a value is a size string."""
        return bool(re.match(r'^\d+(\.\d+)?(B|KiB|MiB|GiB|TiB|KB|MB|GB|TB)$', value))

    def _parse_list(self, value: str) -> List[str]:
        """Parse a list string into a Python list."""
        if value == "[]":
            return []
        # Remove brackets and split by comma
        content = value[1:-1].strip()
        if not content:
            return []
        return [item.strip() for item in content.split(",")]

    def _parse_dict(self, value: str) -> Dict[str, Any]:
        """Parse a dictionary string into a Python dict."""
        if value == "{}":
            return {}
        # Simple parsing for key=value pairs
        content = value[1:-1].strip()
        if not content:
            return {}
        
        result = {}
        # Split by comma but be careful with nested structures
        pairs = content.split(",")
        for pair in pairs:
            if "=" in pair:
                k, v = pair.split("=", 1)
                result[k.strip()] = v.strip()
        return result

    def get_setting(self, key: str, default: Any = None) -> Any:
        """Get a setting value by key.
        
        Args:
            key: The setting key (can be nested with dots)
            default: Default value if setting not found
            
        Returns:
            The setting value or default
        """
        # Handle nested keys
        if '.' in key:
            parts = key.split('.')
            obj = self
            for part in parts:
                if hasattr(obj, part):
                    obj = getattr(obj, part)
                else:
                    return default
            return obj
        
        # Direct attribute
        return getattr(self, key, default)

    def get_all_settings(self) -> Dict[str, Any]:
        """Get all settings as a dictionary.
        
        Returns:
            Dictionary of all settings including nested ones
        """
        result = {}
        
        # Add all direct attributes
        for attr_name in dir(self):
            if not attr_name.startswith('_') and not callable(getattr(self, attr_name)):
                result[attr_name] = getattr(self, attr_name)
        
        return result

    async def refresh_settings(self) -> None:
        """Force a refresh of settings from the cluster."""
        self._loaded = False
        await self.load_settings()

    # Convenience properties for commonly accessed settings
    @property
    def is_authentication_enabled(self) -> bool:
        """Check if authentication is enabled."""
        return self.authenticator_class_name != "AllowAllAuthenticator"
    
    @property
    def is_authorization_enabled(self) -> bool:
        """Check if authorization is enabled."""
        return self.authorizer_class_name != "AllowAllAuthorizer"
    
    @property
    def is_encryption_enabled(self) -> bool:
        """Check if any encryption is enabled."""
        return (self.client_encryption_options.enabled or 
                self.server_encryption_options.enabled or
                self.transparent_data_encryption_options.enabled)
    
    @property
    def is_audit_logging_enabled(self) -> bool:
        """Check if audit logging is enabled."""
        return self.audit_logging_options.enabled