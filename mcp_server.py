import logging
from typing import List, Optional

from fastmcp import FastMCP

from cassandra_service import CassandraService
from cassandra_utility import CassandraUtility
from compaction_analyzer import CompactionAnalyzer
from configuration_analyzer import ConfigurationAnalyzer
from constants import MCP_SERVER_NAME, VALID_SYSTEM_KEYSPACES

logger = logging.getLogger(__name__)


async def create_mcp_server(service: CassandraService) -> FastMCP:
    """Create and configure the MCP server with async Cassandra tools.
    
    This is now async to allow discovery of system tables for dynamic descriptions.
    """
    mcp = FastMCP(name=MCP_SERVER_NAME)

    # Create utility instance
    utility = CassandraUtility(service.connection.session)
    
    # Discover available system tables for dynamic descriptions
    try:
        discovered_tables = await service.discover_system_tables()
        system_table_description = service.generate_system_table_description(discovered_tables)
    except Exception as e:
        logger.warning(f"Failed to discover system tables: {e}")
        # Fallback to a generic description
        system_table_description = """Query database internal statistics from system or system_views keyspaces.
        
        Available tables depend on Cassandra version. Common tables include:
        - system.local: Node information
        - system.peers: Cluster peer information
        - system_views.* (Cassandra 4.0+): Performance metrics and statistics
        
        All tables return node-specific data. Use node_addresses parameter to query specific nodes."""

    @mcp.tool(description="Get all keyspaces in the Cassandra cluster with optional system keyspace filtering.")
    async def get_keyspaces(include_system: bool = False) -> str:
        """Get all keyspaces in the cluster.
        
        Args:
            include_system: If True, include system keyspaces (system, system_*, etc). Default False.
        """
        try:
            keyspaces = await service.get_keyspaces(include_system)
            
            if not keyspaces:
                if include_system:
                    return "No keyspaces found in the cluster"
                else:
                    return "No user keyspaces found. Use include_system=true to see system keyspaces."
            
            # Format output
            output = ["Keyspaces:"]
            for ks in keyspaces:
                output.append(f"  - {ks['name']}")
                
                # Add replication info if available
                if ks.get('replication'):
                    replication = ks['replication']
                    if isinstance(replication, dict):
                        strategy = replication.get('class', '').split('.')[-1]
                        if strategy:
                            output.append(f"    Replication: {strategy}")
                            # Show replication factor for SimpleStrategy or NetworkTopologyStrategy
                            if 'replication_factor' in replication:
                                output.append(f"    Replication Factor: {replication['replication_factor']}")
                            elif strategy == 'NetworkTopologyStrategy':
                                # Show datacenter replication factors
                                dcs = {k: v for k, v in replication.items() if k != 'class'}
                                if dcs:
                                    output.append(f"    Datacenters: {dcs}")
                
                # Add durable_writes info if not default
                if not ks.get('durable_writes', True):
                    output.append(f"    Durable Writes: False")
            
            return "\n".join(output)
            
        except Exception as e:
            logger.error(f"Error getting keyspaces: {e}")
            return f"Error retrieving keyspaces: {str(e)}"
    
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

    @mcp.tool(description=system_table_description)
    async def query_system_table(
        keyspace: str, table: str, node_addresses: Optional[List[str]] = None
    ) -> str:
        """Query a system or system_views table across specified nodes."""
        try:
            # Validate inputs
            if keyspace not in VALID_SYSTEM_KEYSPACES:
                return f"Error: keyspace must be one of {VALID_SYSTEM_KEYSPACES}, got '{keyspace}'"

            # Delegate to service layer
            results = await service.query_system_table_on_nodes(
                keyspace, table, node_addresses
            )

            # Use service formatting method
            return service.format_system_table_results(results, keyspace, table)

        except Exception as e:
            logger.error(f"Error querying {keyspace}.{table}: {e}")
            return f"Error querying {keyspace}.{table}: {str(e)}"

    @mcp.tool(
        description="Execute a CQL query on all nodes in the cluster. Useful for querying virtual tables and node-specific system tables."
    )
    async def query_all_nodes(query: str) -> str:
        """Execute a query on all nodes and return node-specific results."""
        try:
            # Delegate to service layer
            results = await service.execute_on_all_nodes(query)
            
            # Use service formatting method
            return service.format_node_results(results)
            
        except Exception as e:
            logger.error(f"Error executing query on all nodes: {e}")
            return f"Error executing query on all nodes: {str(e)}"

    @mcp.tool(
        description="Execute a CQL query on a specific node. Useful for querying node-local data."
    )
    async def query_node(node_address: str, query: str) -> str:
        """Execute a query on a specific node."""
        try:
            # Delegate to service layer
            result = await service.execute_on_node(node_address, query)
            
            # Use service formatting method
            return service.format_single_node_results(result, node_address)
            
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

    @mcp.tool(
        description="Get configuration recommendations based on Cassandra version and current settings"
    )
    async def get_config_recommendations() -> str:
        """Get configuration recommendations for the cluster.
        
        Analyzes the Cassandra version and provides recommendations for:
        - JVM settings and garbage collection
        - Memory and performance tuning
        - Security configurations
        - Best practices for the specific version
        
        Note: Recommendation rules will be added incrementally.
        """
        try:
            # Get Cassandra version
            version = await utility.get_version()
            
            # Create analyzer with session and version
            config_analyzer = ConfigurationAnalyzer(
                service.connection.session,
                version
            )
            recommendations = await config_analyzer.analyze()
            
            # Format output
            output = [f"=== Configuration Recommendations ==="]
            output.append(f"Cassandra Version: {version[0]}.{version[1]}.{version[2]}")
            output.append("")
            
            if not recommendations:
                output.append("No configuration recommendations available yet.")
                output.append("Recommendation rules will be added in future updates.")
            else:
                # Format recommendations when they exist
                for i, rec in enumerate(recommendations, 1):
                    output.append(f"{i}. {rec.get('recommendation', 'Unknown')}")
                    if 'category' in rec:
                        output.append(f"   Category: {rec['category']}")
                    if 'priority' in rec:
                        output.append(f"   Priority: {rec['priority']}")
                    if 'current' in rec:
                        output.append(f"   Current: {rec['current']}")
                    if 'suggested' in rec:
                        output.append(f"   Suggested: {rec['suggested']}")
                    if 'reason' in rec:
                        output.append(f"   Reason: {rec['reason']}")
                    output.append("")
                    
            return "\n".join(output)
            
        except Exception as e:
            logger.error(f"Error getting config recommendations: {e}")
            return f"Error getting configuration recommendations: {str(e)}"

    return mcp
