from fastmcp import FastMCP
from cassandra.cluster import Cluster, Session, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import DCAwareRoundRobinPolicy
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PreparedStatements:
    """Enum for prepared statement identifiers."""
    SELECT_TABLES = None

    def __init__(self, session: Session):
        """Prepare CQL statements for reuse."""
        logger.info("Preparing CQL statements")
        self.SELECT_TABLES = session.prepare("SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?")



mcp = FastMCP(name="Cassandra MCP Server")

session: Session = None
prepared_statements: PreparedStatements = None

def connect(host: str, port: int):
    """Connect to a Cassandra cluster.

    Args:
        host: The host address of the Cassandra node
        port: The port number (default is usually 9042)
    """
    global session, prepared_statements
    logger.info(f"Connecting to Cassandra at {host}:{port}")
    # Create execution profile with load balancing policy
    profile = ExecutionProfile(
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1')
    )

    cluster = Cluster(
        [host],
        port=port,
        protocol_version=5,
        execution_profiles={EXEC_PROFILE_DEFAULT: profile}
    )
    session = cluster.connect()

    prepared_statements = PreparedStatements(session)

    logger.info("Successfully connected to Cassandra")



@mcp.tool(description="Retrieve all the tables in the requested keyspace.")
def get_tables(keyspace: str) -> str:
    global session, prepared_statements
    logger.info(f"Retrieving tables for {keyspace}")
    result = session.execute(prepared_statements.SELECT_TABLES, [keyspace])
    tmp = "Tables: \n" + "\n".join([table.table_name for table in result])
    logger.info(tmp)
    return tmp

@mcp.tool(description="DESCRIBE the requested table - view the CREATE TABLE definition.")
def get_create_table(keyspace: str, table: str) -> str:
    global session, prepared_statements
    logger.info(f"Retrieving CREATE TABLE definition for {keyspace} from {table}")
    result = session.execute(f"DESCRIBE {keyspace}.{table}")
    tmp = result[0]
    logger.info(tmp)
    return tmp.create_statement

def main():
    logger.info("Starting Cassandra MCP Server")
    # add a config file
    connect("localhost", 9042)
    logger.info("Starting MCP server with HTTP transport")
    mcp.run(transport="http")

if __name__ == "__main__":
    main()
