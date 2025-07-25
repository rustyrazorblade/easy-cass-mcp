import asyncio
from cassandra.cluster import Cluster, Session, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import DCAwareRoundRobinPolicy, WhiteListRoundRobinPolicy
from cassandra.auth import PlainTextAuthProvider
from cassandra import ConsistencyLevel
import logging
from typing import Optional, List, Dict, Any

logger = logging.getLogger(__name__)


class CassandraConnection:
    """Manages async connection to Cassandra cluster."""
    
    def __init__(self, 
                 contact_points: List[str],
                 port: int = 9042,
                 datacenter: str = 'datacenter1',
                 username: Optional[str] = None,
                 password: Optional[str] = None,
                 protocol_version: int = 5):
        self.contact_points = contact_points
        self.port = port
        self.datacenter = datacenter
        self.username = username
        self.password = password
        self.protocol_version = protocol_version
        self.cluster: Optional[Cluster] = None
        self.session: Optional[Session] = None
        self.prepared_statements: Dict[str, Any] = {}
    
    async def connect(self) -> None:
        """Establish connection to Cassandra cluster asynchronously."""
        logger.info(f"Connecting to Cassandra at {self.contact_points}:{self.port}")
        
        profile = ExecutionProfile(
            load_balancing_policy=DCAwareRoundRobinPolicy(local_dc=self.datacenter)
        )
        
        cluster_kwargs = {
            'contact_points': self.contact_points,
            'port': self.port,
            'protocol_version': self.protocol_version,
            'execution_profiles': {EXEC_PROFILE_DEFAULT: profile}
        }
        
        if self.username and self.password:
            cluster_kwargs['auth_provider'] = PlainTextAuthProvider(
                username=self.username,
                password=self.password
            )
        
        # Create cluster and session in executor to avoid blocking
        loop = asyncio.get_event_loop()
        self.cluster = await loop.run_in_executor(None, lambda: Cluster(**cluster_kwargs))
        self.session = await loop.run_in_executor(None, self.cluster.connect)
        logger.info("Successfully connected to Cassandra")
        await self._prepare_statements()

    async def _prepare_statements(self) -> None:
        """Prepare CQL statements for reuse asynchronously."""
        logger.info("Preparing CQL statements")
        # Prepare statements - prepare() is synchronous, so run in executor
        loop = asyncio.get_event_loop()
        self.prepared_statements['select_tables'] = await loop.run_in_executor(
            None,
            self.session.prepare,
            "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?"
        )
        
    async def execute_async(self, statement, parameters=None):
        """Execute a statement asynchronously and return the result."""
        # Create a future that we can await
        loop = asyncio.get_event_loop()
        future = loop.create_future()
        
        # Get the ResponseFuture from Cassandra
        response_future = self.session.execute_async(statement, parameters)
        
        # Define callbacks for the ResponseFuture
        def on_success(result):
            loop.call_soon_threadsafe(future.set_result, result)
        
        def on_error(exc):
            loop.call_soon_threadsafe(future.set_exception, exc)
        
        # Add callbacks to the ResponseFuture
        response_future.add_callback(on_success)
        response_future.add_errback(on_error)
        
        # Wait for the result
        return await future
    
    def get_all_hosts(self) -> List[Any]:
        """Get all hosts in the cluster."""
        if not self.cluster:
            return []
        
        # Get all hosts from cluster metadata
        return list(self.cluster.metadata.all_hosts())
    
    async def execute_on_host(self, host_address: str, statement: str, parameters=None):
        """Execute a statement on a specific host using a dedicated execution profile."""
        # Create execution profile for specific host
        profile_name = f"host_{host_address.replace('.', '_').replace(':', '_')}"
        
        # Check if profile already exists
        if profile_name not in self.cluster.profile_manager.profiles:
            # Create new profile for this host
            profile = ExecutionProfile(
                load_balancing_policy=WhiteListRoundRobinPolicy([host_address]),
                consistency_level=ConsistencyLevel.ONE
            )
            self.cluster.add_execution_profile(profile_name, profile)
            logger.debug(f"Created execution profile for host {host_address}")
        
        # Execute the statement using the host-specific profile
        logger.debug(f"Executing query on host {host_address}: {statement}")
        try:
            # Create a future that we can await
            loop = asyncio.get_event_loop()
            future = loop.create_future()
            
            # Execute with specific profile
            response_future = self.session.execute_async(
                statement, 
                parameters, 
                execution_profile=profile_name
            )
            
            # Define callbacks for the ResponseFuture
            def on_success(result):
                loop.call_soon_threadsafe(future.set_result, result)
            
            def on_error(exc):
                loop.call_soon_threadsafe(future.set_exception, exc)
            
            # Add callbacks to the ResponseFuture
            response_future.add_callback(on_success)
            response_future.add_errback(on_error)
            
            # Wait for the result
            return await future
        except Exception as e:
            logger.error(f"Error executing query on host {host_address}: {e}")
            raise
        
    def disconnect(self) -> None:
        """Close connection gracefully."""
        logger.info("Disconnecting from Cassandra")
        if self.session:
            self.session.shutdown()
        if self.cluster:
            self.cluster.shutdown()