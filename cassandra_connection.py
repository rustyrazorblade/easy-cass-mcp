from cassandra.cluster import Cluster, Session, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.auth import PlainTextAuthProvider
import logging
from typing import Optional, List, Dict

logger = logging.getLogger(__name__)


class CassandraConnection:
    """Manages connection to Cassandra cluster."""
    
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
        self.prepared_statements: Dict[str, any] = {}
    
    def connect(self) -> None:
        """Establish connection to Cassandra cluster."""
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
        
        self.cluster = Cluster(**cluster_kwargs)
        self.session = self.cluster.connect()
        logger.info("Successfully connected to Cassandra")
        self._prepare_statements()

    def _prepare_statements(self) -> None:
        """Prepare CQL statements for reuse."""
        logger.info("Preparing CQL statements")
        self.prepared_statements['select_tables'] = self.session.prepare(
            "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?"
        )
        
    def disconnect(self) -> None:
        """Close connection gracefully."""
        logger.info("Disconnecting from Cassandra")
        if self.session:
            self.session.shutdown()
        if self.cluster:
            self.cluster.shutdown()