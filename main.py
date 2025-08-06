import asyncio
import logging

from cassandra_connection import CassandraConnection
from cassandra_service import CassandraService
from config import CassandraConfig
from mcp_server import create_mcp_server

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def main():
    """Main entry point for the Cassandra MCP server."""
    logger.info("Starting Cassandra MCP Server")

    # Load configuration
    config = CassandraConfig()

    # Create connection using context manager for proper cleanup
    async with CassandraConnection(
        contact_points=config.contact_points,
        port=config.port,
        datacenter=config.datacenter,
        username=config.username,
        password=config.password,
        protocol_version=config.protocol_version,
    ) as connection:
        # Create service
        service = CassandraService(connection)

        # Create and run MCP server
        mcp = create_mcp_server(service)
        logger.info("Starting MCP server with HTTP transport")
        # Use run_async() in async contexts
        await mcp.run_async(transport="http")


if __name__ == "__main__":
    asyncio.run(main())
