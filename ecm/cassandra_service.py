import asyncio
import logging
from typing import Any, Dict, List, Optional

from .cassandra_connection import CassandraConnection
from .constants import MAX_CONCURRENT_QUERIES, MAX_DISPLAY_ROWS
from .exceptions import CassandraMetadataError, CassandraVersionError

logger = logging.getLogger(__name__)


class CassandraService:
    """Service layer for async Cassandra operations."""

    def __init__(self, connection: CassandraConnection) -> None:
        self.connection = connection
        self._system_tables_cache: Optional[Dict[str, List[str]]] = None
        self._cassandra_version: Optional[tuple] = None

    async def get_keyspaces(self, include_system: bool = False) -> List[Dict[str, Any]]:
        """Get all keyspaces in the cluster with their metadata.
        
        Args:
            include_system: If True, include system keyspaces. Default False.
            
        Returns:
            List of dictionaries containing keyspace name and replication info
        """
        logger.info(f"Retrieving keyspaces (include_system={include_system})")
        
        result = await self.connection.execute_async(
            self.connection.prepared_statements["select_keyspaces"]
        )
        
        keyspaces = []
        for row in result:
            keyspace_name = row.keyspace_name
            
            # Filter system keyspaces if requested
            if not include_system and keyspace_name.startswith('system'):
                continue
                
            keyspace_info = {
                'name': keyspace_name,
                'replication': row.replication if hasattr(row, 'replication') else {},
                'durable_writes': row.durable_writes if hasattr(row, 'durable_writes') else True
            }
            keyspaces.append(keyspace_info)
        
        logger.info(f"Found {len(keyspaces)} keyspaces")
        return keyspaces
    
    async def get_tables(self, keyspace: str) -> List[str]:
        """Get all tables in a keyspace asynchronously."""
        logger.info(f"Retrieving tables for keyspace: {keyspace}")
        result = await self.connection.execute_async(
            self.connection.prepared_statements["select_tables"], [keyspace]
        )
        tables = [row.table_name for row in result]
        logger.info(f"Found {len(tables)} tables in keyspace {keyspace}")
        return tables

    async def get_create_table(self, keyspace: str, table: str) -> Optional[str]:
        """Get CREATE TABLE statement for a specific table asynchronously."""
        logger.info(f"Retrieving CREATE TABLE definition for {keyspace}.{table}")

        try:
            # Get table metadata from cluster metadata
            if not self.connection.cluster:
                raise CassandraMetadataError("Cluster connection not established")

            # Check if keyspace exists in metadata
            if keyspace not in self.connection.cluster.metadata.keyspaces:
                raise CassandraMetadataError(f"Keyspace {keyspace} not found")

            keyspace_metadata = self.connection.cluster.metadata.keyspaces[keyspace]

            # Check if table exists in keyspace
            if table not in keyspace_metadata.tables:
                raise CassandraMetadataError(f"Table {keyspace}.{table} not found")

            table_metadata = keyspace_metadata.tables[table]

            # Use export_as_string() to get CREATE TABLE statement with indexes
            create_statement = table_metadata.export_as_string()

            logger.info(f"Retrieved CREATE TABLE statement for {keyspace}.{table}")
            return create_statement

        except CassandraMetadataError:
            raise
        except Exception as e:
            logger.error(f"Error retrieving CREATE TABLE for {keyspace}.{table}: {e}")
            raise CassandraMetadataError(
                f"Error retrieving CREATE TABLE for {keyspace}.{table}: {e}"
            ) from e

    async def execute_query(
        self, query: str, parameters: Optional[tuple] = None
    ) -> Any:
        """Execute arbitrary CQL for testing purposes asynchronously."""
        logger.debug(f"Executing query: {query}")
        return await self.connection.execute_async(query, parameters)

    async def execute_on_node(
        self, node_address: str, query: str, parameters: Optional[tuple] = None
    ) -> Any:
        """Execute a query on a specific node."""
        logger.info(f"Executing query on node {node_address}")
        try:
            result = await self.connection.execute_on_host(
                node_address, query, parameters
            )
            return result
        except Exception as e:
            logger.error(f"Error executing query on node {node_address}: {e}")
            raise

    async def execute_on_all_nodes(
        self, query: str, parameters: Optional[tuple] = None
    ) -> Dict[str, Any]:
        """Execute a query on all nodes in the cluster and return results per node."""
        logger.info("Executing query on all nodes in the cluster")

        # Get all hosts
        hosts = self.connection.get_all_hosts()
        if not hosts:
            logger.warning("No hosts found in cluster")
            return {}

        results = {}

        # Execute query on each host concurrently
        async def query_host(host):
            host_address = host.address
            try:
                logger.debug(f"Querying host: {host_address}")
                result = await self.connection.execute_on_host(
                    host_address, query, parameters
                )
                # Convert result to list to avoid iterator issues
                return host_address, list(result) if result else []
            except Exception as e:
                logger.error(f"Failed to query host {host_address}: {e}")
                return host_address, {"error": str(e)}

        # Run queries concurrently with limit
        # Process in batches to avoid overwhelming the cluster
        batch_size = min(MAX_CONCURRENT_QUERIES, len(hosts))
        query_results = []

        for i in range(0, len(hosts), batch_size):
            batch = hosts[i : i + batch_size]
            tasks = [query_host(host) for host in batch]
            batch_results = await asyncio.gather(*tasks, return_exceptions=False)
            query_results.extend(batch_results)

        # Build results dictionary
        for host_address, result in query_results:
            results[host_address] = result

        logger.info(f"Completed query on {len(results)} nodes")
        return results

    def format_node_results(self, results: Dict[str, Any], query: str = None) -> str:
        """Format results from multiple nodes for display.
        
        Args:
            results: Dictionary mapping node addresses to query results
            query: Optional query string to include in output
            
        Returns:
            Formatted string for display
        """
        if not results:
            return "No results returned"
            
        formatted_results = []
        
        # Add query header if provided
        if query:
            formatted_results.append(f"=== Query: {query} ===")
        
        for node, data in results.items():
            formatted_results.append(f"\n=== Node: {node} ===")
            
            if isinstance(data, dict) and "error" in data:
                formatted_results.append(f"Error: {data['error']}")
            elif isinstance(data, list):
                if not data:
                    formatted_results.append("No results")
                else:
                    # Convert rows to readable format
                    for row in data:
                        formatted_results.append(str(row))
            else:
                formatted_results.append(str(data))
                
        return "\n".join(formatted_results)
    
    def format_system_table_results(
        self, results: Dict[str, Any], keyspace: str, table: str
    ) -> str:
        """Format system table query results with row limiting.
        
        Args:
            results: Dictionary mapping node addresses to query results
            keyspace: Keyspace name
            table: Table name
            
        Returns:
            Formatted string for display with row limits applied
        """
        if not results:
            return "No results returned"
            
        formatted_results = []
        formatted_results.append(f"=== Query: SELECT * FROM {keyspace}.{table} ===")
        
        for node, data in results.items():
            formatted_results.append(f"\n--- Node: {node} ---")
            
            if isinstance(data, dict) and "error" in data:
                formatted_results.append(f"Error: {data['error']}")
            elif isinstance(data, list):
                if not data:
                    formatted_results.append("No results")
                else:
                    # Show row count and first few rows
                    formatted_results.append(f"Returned {len(data)} rows")
                    for i, row in enumerate(data[:MAX_DISPLAY_ROWS]):
                        formatted_results.append(f"  {row}")
                    if len(data) > MAX_DISPLAY_ROWS:
                        formatted_results.append(
                            f"  ... and {len(data) - MAX_DISPLAY_ROWS} more rows"
                        )
            else:
                formatted_results.append(str(data))
                
        return "\n".join(formatted_results)
    
    def format_single_node_results(
        self, result: Any, node_address: str
    ) -> str:
        """Format results from a single node query.
        
        Args:
            result: Query result from the node
            node_address: Address of the node
            
        Returns:
            Formatted string for display
        """
        if result is None:
            return f"No results from node {node_address}"
            
        formatted_results = [f"=== Results from node {node_address} ==="]
        rows = list(result) if result else []
        
        if not rows:
            formatted_results.append("No results")
        else:
            for row in rows:
                formatted_results.append(str(row))
                
        return "\n".join(formatted_results)

    async def query_system_table_on_nodes(
        self, keyspace: str, table: str, node_addresses: Optional[List[str]] = None
    ) -> Dict[str, List[Any]]:
        """Query a system or system_views table on specified nodes.

        Args:
            keyspace: Must be either 'system' or 'system_views'
            table: Table name to query
            node_addresses: Optional list of node addresses to query. If None, queries all nodes.

        Returns:
            Dict mapping node addresses to list of result rows

        Raises:
            ValueError: If keyspace is not 'system' or 'system_views'
        """
        # Validate keyspace
        if keyspace not in ["system", "system_views"]:
            raise ValueError(
                f"Invalid keyspace '{keyspace}'. Must be 'system' or 'system_views'"
            )

        logger.info(
            f"Querying {keyspace}.{table} on {'specified' if node_addresses else 'all'} nodes"
        )

        # Build query
        query = f"SELECT * FROM {keyspace}.{table}"

        # Get hosts to query
        all_hosts = self.connection.get_all_hosts()
        if not all_hosts:
            logger.warning("No hosts found in cluster")
            return {}

        # Filter hosts if specific nodes requested
        if node_addresses:
            hosts_to_query = [h for h in all_hosts if h.address in node_addresses]
            if not hosts_to_query:
                logger.warning(
                    f"None of the specified nodes {node_addresses} found in cluster"
                )
                return {}
        else:
            hosts_to_query = all_hosts

        results = {}

        # Execute query on each host concurrently
        async def query_host(host):
            host_address = host.address
            try:
                logger.debug(f"Querying {keyspace}.{table} on host: {host_address}")
                result = await self.connection.execute_on_host(host_address, query)
                # Convert result to list to avoid iterator issues
                return host_address, list(result) if result else []
            except Exception as e:
                logger.error(
                    f"Failed to query {keyspace}.{table} on host {host_address}: {e}"
                )
                return host_address, {"error": str(e)}

        # Run queries concurrently with limit
        # Process in batches to avoid overwhelming the cluster
        batch_size = min(MAX_CONCURRENT_QUERIES, len(hosts_to_query))
        query_results = []

        for i in range(0, len(hosts_to_query), batch_size):
            batch = hosts_to_query[i : i + batch_size]
            tasks = [query_host(host) for host in batch]
            batch_results = await asyncio.gather(*tasks, return_exceptions=False)
            query_results.extend(batch_results)

        # Build results dictionary
        for host_address, result in query_results:
            results[host_address] = result

        logger.info(f"Completed querying {keyspace}.{table} on {len(results)} nodes")
        return results
    
    async def get_cassandra_version(self) -> tuple:
        """Get the Cassandra version as a tuple (major, minor, patch).
        
        Returns:
            Tuple of (major, minor, patch) version numbers
        """
        if self._cassandra_version:
            return self._cassandra_version
            
        try:
            # Query system.local for version
            result = await self.connection.execute_async(
                "SELECT release_version FROM system.local"
            )
            if result:
                row = result[0] if isinstance(result, list) else next(iter(result), None)
                if row and hasattr(row, 'release_version'):
                    version_str = row.release_version
                    # Parse version like "4.0.11" or "5.0.0-SNAPSHOT"
                    version_parts = version_str.split("-")[0].split(".")
                    major = int(version_parts[0])
                    minor = int(version_parts[1]) if len(version_parts) > 1 else 0
                    patch = int(version_parts[2]) if len(version_parts) > 2 else 0
                    self._cassandra_version = (major, minor, patch)
                    logger.info(f"Detected Cassandra version: {major}.{minor}.{patch}")
                    return self._cassandra_version
        except Exception as e:
            logger.error(f"Failed to get Cassandra version: {e}")
            
        # Default to assuming 4.0 if we can't determine
        logger.warning("Could not determine Cassandra version, assuming 4.0.0")
        self._cassandra_version = (4, 0, 0)
        return self._cassandra_version
    
    async def discover_system_tables(self) -> Dict[str, List[str]]:
        """Discover available system tables in the cluster.
        
        Returns:
            Dictionary mapping keyspace names to lists of table names
        """
        if self._system_tables_cache:
            return self._system_tables_cache
            
        result = {}
        
        # Always available: system keyspace tables
        try:
            system_tables = await self.get_tables("system")
            result["system"] = system_tables
            logger.info(f"Found {len(system_tables)} tables in system keyspace")
        except Exception as e:
            logger.error(f"Failed to get system tables: {e}")
            result["system"] = []
        
        # Check for system_views (Cassandra 4.0+)
        version = await self.get_cassandra_version()
        if version[0] >= 4:
            try:
                system_views_tables = await self.get_tables("system_views")
                result["system_views"] = system_views_tables
                logger.info(f"Found {len(system_views_tables)} tables in system_views keyspace")
            except Exception as e:
                logger.warning(f"system_views keyspace not available: {e}")
                result["system_views"] = []
        else:
            logger.info(f"Cassandra {version[0]}.{version[1]} does not have system_views keyspace")
            
        self._system_tables_cache = result
        return result
    
    def generate_system_table_description(self, discovered_tables: Dict[str, List[str]]) -> str:
        """Generate a dynamic description for the query_system_table tool.
        
        Args:
            discovered_tables: Dictionary of discovered system tables
            
        Returns:
            Formatted description string for the MCP tool
        """
        description_parts = ["Query database internal statistics from system keyspaces."]
        
        # Known table descriptions for common tables
        system_table_docs = {
            "system": {
                "local": "Current node information (cluster name, DC, rack, tokens)",
                "peers": "Information about other nodes in the cluster",
                "peers_v2": "Extended peer information (Cassandra 4.0+)",
                "size_estimates": "Table size estimates for each range",
                "available_ranges": "Token ranges available on this node",
                "transferred_ranges": "Token ranges being transferred",
                "compaction_history": "History of completed compactions",
                "sstable_activity": "Current SSTable activity",
                "built_views": "Materialized views build status",
                "view_builds_in_progress": "Currently building materialized views"
            },
            "system_views": {
                "disk_usage": "Disk space usage per keyspace/table",
                "local_read_latency": "Read latency statistics per table (count field shows number of reads)",
                "local_write_latency": "Write latency statistics per table (count field shows number of writes)", 
                "local_scan_latency": "Scan latency statistics per table (count field shows number of scans)",
                "thread_pools": "Thread pool statistics and queue depths",
                "sstable_tasks": "Active SSTable operations (compaction, cleanup, etc)",
                "streaming": "Active streaming operations between nodes",
                "clients": "Currently connected client sessions",
                "caches": "Key cache, row cache, and counter cache statistics",
                "settings": "Current database configuration settings",
                "system_properties": "JVM system properties",
                "internode_inbound": "Inbound internode messaging metrics",
                "internode_outbound": "Outbound internode messaging metrics",
                "coordinator_read_latency": "Coordinator read latency",
                "coordinator_write_latency": "Coordinator write latency",
                "coordinator_scan_latency": "Coordinator scan latency",
                "tombstones_scanned": "Tombstone scan statistics",
                "live_scanned": "Live cells scanned statistics",
                "max_partition_size": "Maximum partition sizes per table",
                "rows_per_partition": "Row count statistics per partition"
            }
        }
        
        # Add system tables section if available
        if "system" in discovered_tables and discovered_tables["system"]:
            description_parts.append("\nAVAILABLE SYSTEM TABLES (cluster metadata):")
            for table in sorted(discovered_tables["system"]):
                if table in system_table_docs["system"]:
                    description_parts.append(f"  - {table}: {system_table_docs['system'][table]}")
                else:
                    # Only show tables we know are useful for operators
                    if table not in ["IndexInfo", "batches", "paxos", "prepared_statements", 
                                    "schema_aggregates", "schema_columnfamilies", "schema_columns",
                                    "schema_functions", "schema_keyspaces", "schema_triggers",
                                    "schema_types", "schema_usertypes"]:
                        description_parts.append(f"  - {table}")
        
        # Add system_views section if available  
        if "system_views" in discovered_tables and discovered_tables["system_views"]:
            description_parts.append("\nAVAILABLE SYSTEM_VIEWS TABLES (performance metrics):")
            for table in sorted(discovered_tables["system_views"]):
                if table in system_table_docs["system_views"]:
                    description_parts.append(f"  - {table}: {system_table_docs['system_views'][table]}")
                else:
                    description_parts.append(f"  - {table}")
        
        description_parts.append("\nAll tables return node-specific data. Use node_addresses parameter to query specific nodes.")
        
        return "\n".join(description_parts)
