import uuid
from datetime import datetime
from typing import List
from cassandra_service import CassandraService


class CassandraTestHelper:
    """Helper utilities for Cassandra integration tests."""
    
    def __init__(self, service: CassandraService, keyspace: str):
        self.service = service
        self.keyspace = keyspace
    
    def create_test_schema(self):
        """Create a standard test schema for common test scenarios."""
        schemas = [
            f"""
            CREATE TABLE IF NOT EXISTS {self.keyspace}.users (
                id UUID PRIMARY KEY,
                username TEXT,
                email TEXT,
                created_at TIMESTAMP
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {self.keyspace}.events (
                user_id UUID,
                event_time TIMESTAMP,
                event_type TEXT,
                data MAP<TEXT, TEXT>,
                PRIMARY KEY (user_id, event_time)
            ) WITH CLUSTERING ORDER BY (event_time DESC)
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {self.keyspace}.counters (
                id TEXT PRIMARY KEY,
                count COUNTER
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {self.keyspace}.time_series (
                device_id UUID,
                timestamp TIMESTAMP,
                temperature DOUBLE,
                humidity DOUBLE,
                PRIMARY KEY (device_id, timestamp)
            ) WITH CLUSTERING ORDER BY (timestamp DESC)
            """
        ]
        
        for schema in schemas:
            self.service.execute_query(schema)
    
    def insert_test_users(self, count: int) -> List[uuid.UUID]:
        """Insert test users and return their IDs."""
        users = []
        for i in range(count):
            user_id = uuid.uuid4()
            self.service.execute_query(
                f"INSERT INTO {self.keyspace}.users (id, username, email, created_at) VALUES (?, ?, ?, ?)",
                (user_id, f"user_{i}", f"user_{i}@test.com", datetime.utcnow())
            )
            users.append(user_id)
        return users
    
    def insert_test_events(self, user_id: uuid.UUID, count: int):
        """Insert test events for a user."""
        for i in range(count):
            self.service.execute_query(
                f"""INSERT INTO {self.keyspace}.events 
                    (user_id, event_time, event_type, data) 
                    VALUES (?, ?, ?, ?)""",
                (
                    user_id,
                    datetime.utcnow(),
                    f"event_type_{i % 3}",
                    {"key": f"value_{i}", "index": str(i)}
                )
            )
    
    def insert_time_series_data(self, device_id: uuid.UUID, count: int):
        """Insert time series data for testing."""
        base_time = datetime.utcnow()
        for i in range(count):
            # Create data points at 1-minute intervals
            timestamp = base_time.replace(microsecond=0, second=0, minute=base_time.minute - i)
            self.service.execute_query(
                f"""INSERT INTO {self.keyspace}.time_series 
                    (device_id, timestamp, temperature, humidity) 
                    VALUES (?, ?, ?, ?)""",
                (
                    device_id,
                    timestamp,
                    20.0 + (i % 10) * 0.5,  # Temperature varies between 20-25
                    60.0 + (i % 20) * 0.5   # Humidity varies between 60-70
                )
            )
    
    def cleanup_keyspace(self):
        """Drop all tables in the test keyspace."""
        tables = self.service.execute_query(
            "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?",
            (self.keyspace,)
        )
        for table in tables:
            self.service.execute_query(f"DROP TABLE IF EXISTS {self.keyspace}.{table.table_name}")
    
    def verify_table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the keyspace."""
        tables = self.service.get_tables(self.keyspace)
        return table_name in tables