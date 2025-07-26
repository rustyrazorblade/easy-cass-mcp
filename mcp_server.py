from fastmcp import FastMCP
from cassandra_service import CassandraService
import logging
import json
from typing import Optional, List

logger = logging.getLogger(__name__)


def create_mcp_server(service: CassandraService) -> FastMCP:
    """Create and configure the MCP server with async Cassandra tools."""
    mcp = FastMCP(name="Cassandra MCP Server")
    
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
    
    @mcp.tool(description="DESCRIBE the requested table - view the CREATE TABLE definition.")
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
    
    @mcp.tool(description="""Query database internal statistics from system or system_views keyspaces.
    
    COMMON SYSTEM_VIEWS TABLES (performance metrics & statistics):
    - disk_usage: Disk space usage per keyspace/table
    - local_read_latency: Read latency statistics per table  
    - local_write_latency: Write latency statistics per table
    - local_scan_latency: Scan latency statistics per table
    - thread_pools: Thread pool statistics and queue depths
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
    
    All tables return node-specific data. Use node_addresses parameter to query specific nodes.""")
    async def query_system_table(
        keyspace: str,
        table: str,
        node_addresses: Optional[List[str]] = None
    ) -> str:
        """Query a system or system_views table across specified nodes."""
        try:
            # Validate inputs
            if keyspace not in ['system', 'system_views']:
                return f"Error: keyspace must be 'system' or 'system_views', got '{keyspace}'"
            
            # Execute query
            results = await service.query_system_table_on_nodes(keyspace, table, node_addresses)
            
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
                        for i, row in enumerate(data[:10]):  # Show first 10 rows
                            formatted_results.append(f"  {row}")
                        if len(data) > 10:
                            formatted_results.append(f"  ... and {len(data) - 10} more rows")
                else:
                    formatted_results.append(str(data))
            
            return "\n".join(formatted_results)
            
        except Exception as e:
            logger.error(f"Error querying {keyspace}.{table}: {e}")
            return f"Error querying {keyspace}.{table}: {str(e)}"
    
    @mcp.tool(description="Execute a CQL query on all nodes in the cluster. Useful for querying virtual tables and node-specific system tables.")
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
    
    @mcp.tool(description="Execute a CQL query on a specific node. Useful for querying node-local data.")
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
    
    return mcp