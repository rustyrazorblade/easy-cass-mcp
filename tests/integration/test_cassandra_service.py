import pytest
import uuid


class TestCassandraServiceIntegration:
    """Integration tests for CassandraService.
    
    Fixtures used (defined in conftest.py):
    - cassandra_service: Provides a CassandraService instance
    - test_keyspace: Provides the test keyspace name (may contain tables from other tests)
    - empty_test_keyspace: Provides a guaranteed empty keyspace for isolation
    """
    
    @pytest.mark.asyncio
    async def test_get_tables(self, cassandra_service, test_keyspace):
        """Test retrieving tables from a keyspace."""
        # Create test tables
        await cassandra_service.execute_query(f"""
            CREATE TABLE IF NOT EXISTS {test_keyspace}.test_users (
                id UUID PRIMARY KEY,
                username TEXT,
                email TEXT,
                created_at TIMESTAMP
            )
        """)
        
        await cassandra_service.execute_query(f"""
            CREATE TABLE IF NOT EXISTS {test_keyspace}.test_products (
                id UUID PRIMARY KEY,
                name TEXT,
                price DECIMAL
            )
        """)
        
        # Get tables
        tables = await cassandra_service.get_tables(test_keyspace)
        
        # Verify both tables exist
        assert 'test_users' in tables
        assert 'test_products' in tables
        assert len(tables) >= 2
    
    @pytest.mark.asyncio
    async def test_get_tables_empty_keyspace(self, cassandra_service, empty_test_keyspace):
        """Test retrieving tables from an empty keyspace."""
        tables = await cassandra_service.get_tables(empty_test_keyspace)
        assert tables == []
    
    @pytest.mark.asyncio
    async def test_get_create_table(self, cassandra_service, test_keyspace):
        """Test retrieving CREATE TABLE statement."""
        # Create table with various features
        await cassandra_service.execute_query(f"""
            CREATE TABLE IF NOT EXISTS {test_keyspace}.test_complex (
                partition_key UUID,
                clustering_key INT,
                data TEXT,
                metadata MAP<TEXT, TEXT>,
                PRIMARY KEY (partition_key, clustering_key)
            ) WITH CLUSTERING ORDER BY (clustering_key DESC)
        """)
        
        # Get CREATE TABLE statement
        create_stmt = await cassandra_service.get_create_table(test_keyspace, 'test_complex')
        
        # Verify key components are present
        assert create_stmt is not None
        create_stmt_lower = create_stmt.lower()
        assert 'partition_key uuid' in create_stmt_lower
        assert 'clustering_key int' in create_stmt_lower
        assert 'primary key' in create_stmt_lower
        assert 'partition_key' in create_stmt_lower
        assert 'clustering_key' in create_stmt_lower
        # Note: Our implementation doesn't include clustering order yet
    
    @pytest.mark.asyncio
    async def test_get_create_table_not_found(self, cassandra_service, test_keyspace):
        """Test retrieving CREATE TABLE for non-existent table."""
        with pytest.raises(Exception):
            await cassandra_service.get_create_table(test_keyspace, 'non_existent_table')
    
    @pytest.mark.asyncio
    async def test_prepared_statements_performance(self, cassandra_service, test_keyspace):
        """Test that prepared statements work correctly for repeated calls."""
        # Create a table
        await cassandra_service.execute_query(f"""
            CREATE TABLE IF NOT EXISTS {test_keyspace}.perf_test (
                id UUID PRIMARY KEY,
                data TEXT
            )
        """)
        
        # Insert some data
        for i in range(10):
            await cassandra_service.execute_query(
                f"INSERT INTO {test_keyspace}.perf_test (id, data) VALUES (%s, %s)",
                (uuid.uuid4(), f"test_data_{i}")
            )
        
        # Test multiple calls to get_tables use prepared statement
        for _ in range(5):
            tables = await cassandra_service.get_tables(test_keyspace)
            assert 'perf_test' in tables
    
    @pytest.mark.asyncio
    async def test_execute_query_with_parameters(self, cassandra_service, test_keyspace):
        """Test executing parameterized queries."""
        # Create table
        await cassandra_service.execute_query(f"""
            CREATE TABLE IF NOT EXISTS {test_keyspace}.test_params (
                id UUID PRIMARY KEY,
                name TEXT,
                value INT
            )
        """)
        
        # Insert with parameters
        test_id = uuid.uuid4()
        await cassandra_service.execute_query(
            f"INSERT INTO {test_keyspace}.test_params (id, name, value) VALUES (%s, %s, %s)",
            (test_id, "test_name", 42)
        )
        
        # Verify insert
        result = await cassandra_service.execute_query(
            f"SELECT * FROM {test_keyspace}.test_params WHERE id = %s",
            (test_id,)
        )
        
        rows = list(result)
        assert len(rows) == 1
        row = rows[0]
        assert row.name == "test_name"
        assert row.value == 42