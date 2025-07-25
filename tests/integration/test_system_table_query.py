"""Test script for system table query functionality."""
import asyncio
import logging
from config import CassandraConfig
from cassandra_connection import CassandraConnection
from cassandra_service import CassandraService

logging.basicConfig(level=logging.INFO)


async def test_system_table_queries():
    """Test the new system table query functionality."""
    # Setup
    config = CassandraConfig()
    connection = CassandraConnection(
        contact_points=config.contact_points,
        port=config.port,
        datacenter=config.datacenter,
        username=config.username,
        password=config.password
    )
    
    try:
        await connection.connect()
        service = CassandraService(connection)
        
        print("\n=== Testing System Table Query Functionality ===\n")
        
        # Test 1: Query system.local
        print("1. Testing system.local query on all nodes:")
        results = await service.query_system_table_on_nodes('system', 'local')
        for node, data in results.items():
            if isinstance(data, list) and data:
                print(f"   Node {node}: cluster_name={data[0].cluster_name}")
            else:
                print(f"   Node {node}: {data}")
        
        # Test 2: Query system_views.disk_usage
        print("\n2. Testing system_views.disk_usage query:")
        results = await service.query_system_table_on_nodes('system_views', 'disk_usage')
        for node, data in results.items():
            if isinstance(data, list):
                print(f"   Node {node}: {len(data)} rows")
            else:
                print(f"   Node {node}: {data}")
        
        # Test 3: Query with specific nodes (if multiple nodes exist)
        hosts = connection.get_all_hosts()
        if len(hosts) > 1:
            first_node = hosts[0].address
            print(f"\n3. Testing query on specific node ({first_node}):")
            results = await service.query_system_table_on_nodes(
                'system_views', 'thread_pools', [first_node]
            )
            print(f"   Queried {len(results)} node(s)")
        
        # Test 4: Invalid keyspace
        print("\n4. Testing invalid keyspace (should raise error):")
        try:
            await service.query_system_table_on_nodes('invalid_keyspace', 'test')
        except ValueError as e:
            print(f"   Correctly caught error: {e}")
        
        print("\n=== All tests completed successfully! ===")
        
    except Exception as e:
        print(f"Error during testing: {e}")
        import traceback
        traceback.print_exc()
    finally:
        connection.disconnect()


if __name__ == "__main__":
    asyncio.run(test_system_table_queries())