from fastmcp import FastMCP
from cassandra_service import CassandraService
import logging

logger = logging.getLogger(__name__)


def create_mcp_server(service: CassandraService) -> FastMCP:
    """Create and configure the MCP server with Cassandra tools."""
    mcp = FastMCP(name="Cassandra MCP Server")
    
    @mcp.tool(description="Retrieve all the tables in the requested keyspace.")
    def get_tables(keyspace: str) -> str:
        """Get all tables in a keyspace."""
        try:
            tables = service.get_tables(keyspace)
            if not tables:
                return f"No tables found in keyspace: {keyspace}"
            return "Tables:\n" + "\n".join(tables)
        except Exception as e:
            logger.error(f"Error getting tables: {e}")
            return f"Error retrieving tables: {str(e)}"
    
    @mcp.tool(description="DESCRIBE the requested table - view the CREATE TABLE definition.")
    def get_create_table(keyspace: str, table: str) -> str:
        """Get CREATE TABLE statement for a specific table."""
        try:
            create_statement = service.get_create_table(keyspace, table)
            if not create_statement:
                return f"Table {keyspace}.{table} not found"
            return create_statement
        except Exception as e:
            logger.error(f"Error getting CREATE TABLE: {e}")
            return f"Error retrieving CREATE TABLE: {str(e)}"
    
    return mcp