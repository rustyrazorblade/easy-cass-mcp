import asyncio
import logging
from typing import Any, Dict, List, Optional

from cassandra_connection import CassandraConnection
from constants import MAX_CONCURRENT_QUERIES
from exceptions import CassandraMetadataError, CassandraQueryError

logger = logging.getLogger(__name__)


class CassandraService:
    """Service layer for async Cassandra operations."""

    def __init__(self, connection: CassandraConnection) -> None:
        self.connection = connection

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
