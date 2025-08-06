import logging
from typing import List, Optional

from fastmcp import FastMCP

from cassandra_service import CassandraService
from cassandra_utility import CassandraUtility
from compaction_analyzer import CompactionAnalyzer
from constants import MAX_DISPLAY_ROWS, MCP_SERVER_NAME, VALID_SYSTEM_KEYSPACES

logger = logging.getLogger(__name__)


def create_mcp_server(service: CassandraService) -> FastMCP:
    """Create and configure the MCP server with async Cassandra tools."""
    mcp = FastMCP(name=MCP_SERVER_NAME)

    # Create utility instance
    utility = CassandraUtility(service.connection.session)

    @mcp.tool(description="Retrieve all the tables in the requested keyspace.")
    async def get_tables(keyspace: str) -> str:
        """Get all tables in a keyspace."""
        try:
            tables = await service.get_tables(keyspace)
            if not tables:
                return f"No tables found in keyspace: {keyspace}"
            return "Tables:\n" + "\n".join(tables)
        except Exception as e:
            logger.error(f"Error getting tables: {e}")
            return f"Error retrieving tables: {str(e)}"

    @mcp.tool(
        description="DESCRIBE the requested table - view the CREATE TABLE definition."
    )
    async def get_create_table(keyspace: str, table: str) -> str:
        """Get CREATE TABLE statement for a specific table."""
        try:
            create_statement = await service.get_create_table(keyspace, table)
            if not create_statement:
                return f"Table {keyspace}.{table} not found"
            return create_statement
        except Exception as e:
            logger.error(f"Error getting CREATE TABLE: {e}")
            return f"Error retrieving CREATE TABLE: {str(e)}"

    @mcp.tool(
        description="""Query database internal statistics from system or system_views keyspaces.

    COMMON SYSTEM_VIEWS TABLES (performance metrics & statistics):
    - disk_usage: Disk space usage per keyspace/table
    - local_read_latency: Read latency statistics per table.  The count field exposes the number of reads.
    - local_write_latency: Write latency statistics per table.  The count field exposes the number of writes.
    - local_scan_latency: Scan latency statistics per table.  The count field exposes the number of scans.
    - thread_pools: Thread pool statistics and queue depths.
    - sstable_tasks: Active SSTable operations (compaction, cleanup, etc)
    - streaming: Active streaming operations between nodes
    - clients: Currently connected client sessions
    - caches: Key cache, row cache, and counter cache statistics
    - settings: Current database configuration settings
    - system_properties: JVM system properties
    - internode_inbound: Inbound internode messaging metrics
    - internode_outbound: Outbound internode messaging metrics

    COMMON SYSTEM TABLES (cluster metadata):
    - local: Current node information (cluster name, DC, rack, tokens)
    - peers: Information about other nodes in the cluster
    - peers_v2: Extended peer information (Cassandra 4.0+)
    - available_ranges: Token ranges available on this node
    - transferred_ranges: Token ranges being transferred
    - size_estimates: Table size estimates for each range

    COMMON SYSTEM TABLES (performance and data statistics):
    - compaction_history: History of completed compactions

    All tables return node-specific data. Use node_addresses parameter to query specific nodes."""
    )
    async def query_system_table(
        keyspace: str, table: str, node_addresses: Optional[List[str]] = None
    ) -> str:
        """Query a system or system_views table across specified nodes."""
        try:
            # Validate inputs
            if keyspace not in VALID_SYSTEM_KEYSPACES:
                return f"Error: keyspace must be one of {VALID_SYSTEM_KEYSPACES}, got '{keyspace}'"

            # Execute query
            results = await service.query_system_table_on_nodes(
                keyspace, table, node_addresses
            )

            # Format results for display
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

        except Exception as e:
            logger.error(f"Error querying {keyspace}.{table}: {e}")
            return f"Error querying {keyspace}.{table}: {str(e)}"

    @mcp.tool(
        description="Execute a CQL query on all nodes in the cluster. Useful for querying virtual tables and node-specific system tables."
    )
    async def query_all_nodes(query: str) -> str:
        """Execute a query on all nodes and return node-specific results."""
        try:
            results = await service.execute_on_all_nodes(query)

            # Format results for display
            formatted_results = []
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
        except Exception as e:
            logger.error(f"Error executing query on all nodes: {e}")
            return f"Error executing query on all nodes: {str(e)}"

    @mcp.tool(
        description="Execute a CQL query on a specific node. Useful for querying node-local data."
    )
    async def query_node(node_address: str, query: str) -> str:
        """Execute a query on a specific node."""
        try:
            result = await service.execute_on_node(node_address, query)

            # Format results
            if not result:
                return f"No results from node {node_address}"

            formatted_results = [f"=== Results from node {node_address} ==="]
            rows = list(result)
            if not rows:
                formatted_results.append("No results")
            else:
                for row in rows:
                    formatted_results.append(str(row))

            return "\n".join(formatted_results)
        except Exception as e:
            logger.error(f"Error executing query on node {node_address}: {e}")
            return f"Error executing query on node {node_address}: {str(e)}"

    @mcp.tool(
        description="Analyze a table and suggest optimization opportunities including compaction strategy improvements."
    )
    async def analyze_table_optimizations(keyspace: str, table: str) -> str:
        """Analyze a table and suggest optimizations."""
        try:
            # Get table object using utility
            table_obj = utility.get_table(keyspace, table)

            # Get Cassandra version
            version = await utility.get_version()

            # Create compaction analyzer and run analysis
            compaction_analyzer = CompactionAnalyzer(table_obj, version)
            optimizations = await compaction_analyzer.analyze()

            # Format output
            if not optimizations:
                return (
                    f"No optimization suggestions found for {keyspace}.{table}. "
                    f"The table appears to be well-configured for the current "
                    f"Cassandra version."
                )

            output = [f"=== Optimization Analysis for {keyspace}.{table} ==="]
            output.append(
                f"Detected Cassandra version: {version[0]}.{version[1]}.{version[2]}"
            )
            output.append("")

            for i, opt in enumerate(optimizations, 1):
                output.append(
                    f"{i}. {opt['type'].replace('_', ' ').title()} Optimization"
                )
                output.append(f"   Current: {opt['current']}")
                output.append(f"   Recommendation: {opt['recommendation']}")
                output.append(f"   Reason: {opt['reason']}")
                if "reference" in opt:
                    output.append(f"   Reference: {opt['reference']}")
                output.append("")

            output.append(
                "To apply these optimizations, use ALTER TABLE with the "
                "recommended settings."
            )

            return "\n".join(output)

        except Exception as e:
            logger.error(f"Error analyzing table optimizations: {e}")
            return f"Error analyzing table optimizations: {str(e)}"

    return mcp
