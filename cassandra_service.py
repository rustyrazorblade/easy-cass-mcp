from typing import List, Optional, Any
import logging
from cassandra_connection import CassandraConnection

logger = logging.getLogger(__name__)


class CassandraService:
    """Service layer for async Cassandra operations."""
    
    def __init__(self, connection: CassandraConnection):
        self.connection = connection
    
    async def get_tables(self, keyspace: str) -> List[str]:
        """Get all tables in a keyspace asynchronously."""
        logger.info(f"Retrieving tables for keyspace: {keyspace}")
        result = await self.connection.execute_async(
            self.connection.prepared_statements['select_tables'],
            [keyspace]
        )
        tables = [row.table_name for row in result]
        logger.info(f"Found {len(tables)} tables in keyspace {keyspace}")
        return tables
    
    async def get_create_table(self, keyspace: str, table: str) -> Optional[str]:
        """Get CREATE TABLE statement for a specific table asynchronously."""
        logger.info(f"Retrieving CREATE TABLE definition for {keyspace}.{table}")
        
        # Query system_schema to build CREATE TABLE statement
        try:
            # Get table metadata
            columns_query = """
                SELECT column_name, type, kind
                FROM system_schema.columns
                WHERE keyspace_name = %s AND table_name = %s
            """
            columns_result = await self.connection.execute_async(columns_query, (keyspace, table))
            
            if not columns_result:
                raise Exception(f"Table {keyspace}.{table} not found")
            
            # Build CREATE TABLE statement
            columns = list(columns_result)
            if not columns:
                raise Exception(f"Table {keyspace}.{table} not found")
                
            # Separate partition key, clustering key, and regular columns
            partition_keys = [col for col in columns if col.kind == 'partition_key']
            clustering_keys = [col for col in columns if col.kind == 'clustering']
            regular_columns = [col for col in columns if col.kind == 'regular']
            
            # Build column definitions
            column_defs = []
            for col in partition_keys + clustering_keys + regular_columns:
                column_defs.append(f"{col.column_name} {col.type}")
            
            # Build PRIMARY KEY clause
            pk_parts = [col.column_name for col in partition_keys]
            ck_parts = [col.column_name for col in clustering_keys]
            
            if ck_parts:
                primary_key = f"PRIMARY KEY (({', '.join(pk_parts)}), {', '.join(ck_parts)})"
            else:
                primary_key = f"PRIMARY KEY ({', '.join(pk_parts)})"
            
            # Construct CREATE TABLE statement
            create_statement = f"CREATE TABLE {keyspace}.{table} (\n"
            create_statement += ",\n".join(f"    {col}" for col in column_defs)
            create_statement += f",\n    {primary_key}\n)"
            
            logger.info(f"Retrieved CREATE TABLE statement for {keyspace}.{table}")
            return create_statement
            
        except Exception as e:
            logger.error(f"Error retrieving CREATE TABLE for {keyspace}.{table}: {e}")
            raise
    
    async def execute_query(self, query: str, parameters: Optional[tuple] = None) -> Any:
        """Execute arbitrary CQL for testing purposes asynchronously."""
        logger.debug(f"Executing query: {query}")
        return await self.connection.execute_async(query, parameters)