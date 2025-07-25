from typing import List, Optional, Any
import logging
from cassandra_connection import CassandraConnection

logger = logging.getLogger(__name__)


class CassandraService:
    """Service layer for Cassandra operations."""
    
    def __init__(self, connection: CassandraConnection):
        self.connection = connection
    
    def get_tables(self, keyspace: str) -> List[str]:
        """Get all tables in a keyspace."""
        logger.info(f"Retrieving tables for keyspace: {keyspace}")
        result = self.connection.session.execute(
            self.connection.prepared_statements['select_tables'],
            [keyspace]
        )
        tables = [row.table_name for row in result]
        logger.info(f"Found {len(tables)} tables in keyspace {keyspace}")
        return tables
    
    def get_create_table(self, keyspace: str, table: str) -> Optional[str]:
        """Get CREATE TABLE statement for a specific table."""
        logger.info(f"Retrieving CREATE TABLE definition for {keyspace}.{table}")
        # Using DESCRIBE requires careful handling of keyspace/table names
        stmt = f"DESCRIBE TABLE {keyspace}.{table}"
        try:
            result = self.connection.session.execute(stmt)
            if result:
                create_statement = result.one().create_statement
                logger.info(f"Retrieved CREATE TABLE statement for {keyspace}.{table}")
                return create_statement
            return None
        except Exception as e:
            logger.error(f"Error retrieving CREATE TABLE for {keyspace}.{table}: {e}")
            raise
    
    def execute_query(self, query: str, parameters: Optional[tuple] = None) -> Any:
        """Execute arbitrary CQL for testing purposes."""
        logger.debug(f"Executing query: {query}")
        return self.connection.session.execute(query, parameters)