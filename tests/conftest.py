import pytest
import pytest_asyncio
import asyncio
import os
from cassandra_connection import CassandraConnection
from cassandra_service import CassandraService
from config import CassandraConfig


@pytest.fixture(scope="session")
def test_config():
    """Load test configuration from environment."""
    config = CassandraConfig()
    # Override with test-specific settings if provided
    return {
        'contact_points': config.test_contact_points or config.contact_points,
        'port': config.port,
        'datacenter': config.test_datacenter or config.datacenter,
        'username': config.username,
        'password': config.password,
        'test_keyspace': config.test_keyspace
    }


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="session")
async def cassandra_connection(test_config):
    """Create async connection for entire test session."""
    connection = CassandraConnection(
        contact_points=test_config['contact_points'],
        port=test_config['port'],
        datacenter=test_config['datacenter'],
        username=test_config['username'],
        password=test_config['password']
    )
    await connection.connect()
    
    # Create test keyspace
    await connection.execute_async(f"""
        CREATE KEYSPACE IF NOT EXISTS {test_config['test_keyspace']}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
    """)
    
    yield connection
    
    # Cleanup (optional - you might want to keep test data)
    if os.getenv('CLEANUP_TEST_DATA', 'false').lower() == 'true':
        await connection.execute_async(f"DROP KEYSPACE IF EXISTS {test_config['test_keyspace']}")
    
    connection.disconnect()


@pytest_asyncio.fixture
async def cassandra_service(cassandra_connection):
    """Create service instance for each test."""
    return CassandraService(cassandra_connection)


@pytest_asyncio.fixture
async def test_keyspace(test_config, cassandra_connection):
    """Provide test keyspace name and ensure it's clean."""
    keyspace = test_config['test_keyspace']
    # Truncate all tables in keyspace before each test
    tables_result = await cassandra_connection.execute_async(
        "SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s",
        (keyspace,)
    )
    for table in tables_result:
        await cassandra_connection.execute_async(f"TRUNCATE {keyspace}.{table.table_name}")
    
    return keyspace


@pytest_asyncio.fixture
async def empty_test_keyspace(test_config, cassandra_connection):
    """Provide a separate empty keyspace for tests that need no tables."""
    keyspace = f"{test_config['test_keyspace']}_empty"
    
    # Create the keyspace if it doesn't exist
    await cassandra_connection.execute_async(f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
    """)
    
    # Ensure it's truly empty by dropping any existing tables
    tables_result = await cassandra_connection.execute_async(
        "SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s",
        (keyspace,)
    )
    for table in tables_result:
        await cassandra_connection.execute_async(f"DROP TABLE IF EXISTS {keyspace}.{table.table_name}")
    
    yield keyspace
    
    # Cleanup after test if configured
    if os.getenv('CLEANUP_TEST_DATA', 'false').lower() == 'true':
        await cassandra_connection.execute_async(f"DROP KEYSPACE IF EXISTS {keyspace}")