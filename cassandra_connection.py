import asyncio
import logging
from typing import Any, Dict, List, Optional, Set

from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import (EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile,
                               Session)
from cassandra.policies import (DCAwareRoundRobinPolicy,
                                WhiteListRoundRobinPolicy)

from constants import CONNECTION_TIMEOUT, QUERY_TIMEOUT
from exceptions import CassandraConnectionError, CassandraQueryError

logger = logging.getLogger(__name__)


class CassandraConnection:
    """Manages async connection to Cassandra cluster.

    Can be used as an async context manager:
        async with CassandraConnection(...) as conn:
            # Use connection
            pass
    """

    def __init__(
        self,
        contact_points: List[str],
        port: int = 9042,
        datacenter: str = "datacenter1",
        username: Optional[str] = None,
        password: Optional[str] = None,
        protocol_version: int = 5,
    ) -> None:
        self.contact_points = contact_points
        self.port = port
        self.datacenter = datacenter
        self.username = username
        self.password = password
        self.protocol_version = protocol_version
        self.cluster: Optional[Cluster] = None
        self.session: Optional[Session] = None
        self.prepared_statements: Dict[str, Any] = {}
        self._execution_profiles: Set[str] = set()  # Track created profiles
        self._is_connected: bool = False

    async def connect(self) -> None:
        """Establish connection to Cassandra cluster asynchronously.

        Raises:
            CassandraConnectionError: If connection fails
        """
        if self._is_connected:
            logger.debug("Already connected to Cassandra")
            return

        logger.info(f"Connecting to Cassandra at {self.contact_points}:{self.port}")

        profile = ExecutionProfile(
            load_balancing_policy=DCAwareRoundRobinPolicy(local_dc=self.datacenter)
        )

        cluster_kwargs = {
            "contact_points": self.contact_points,
            "port": self.port,
            "protocol_version": self.protocol_version,
            "execution_profiles": {EXEC_PROFILE_DEFAULT: profile},
        }

        if self.username and self.password:
            cluster_kwargs["auth_provider"] = PlainTextAuthProvider(
                username=self.username, password=self.password
            )

        try:
            # Create cluster and session in executor to avoid blocking
            loop = asyncio.get_event_loop()
            self.cluster = await asyncio.wait_for(
                loop.run_in_executor(None, lambda: Cluster(**cluster_kwargs)),
                timeout=CONNECTION_TIMEOUT,
            )
            self.session = await asyncio.wait_for(
                loop.run_in_executor(None, self.cluster.connect),
                timeout=CONNECTION_TIMEOUT,
            )
            self._is_connected = True
            logger.info("Successfully connected to Cassandra")
            await self._prepare_statements()
        except asyncio.TimeoutError:
            raise CassandraConnectionError(
                f"Connection timeout after {CONNECTION_TIMEOUT} seconds"
            )
        except Exception as e:
            logger.error(f"Failed to connect to Cassandra: {e}")
            raise CassandraConnectionError(f"Failed to connect: {e}") from e

    async def _prepare_statements(self) -> None:
        """Prepare CQL statements for reuse asynchronously."""
        logger.info("Preparing CQL statements")
        # Prepare statements - prepare() is synchronous, so run in executor
        loop = asyncio.get_event_loop()

        # Prepare common system queries
        statements_to_prepare = {
            "select_tables": "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?",
            "select_keyspaces": "SELECT keyspace_name FROM system_schema.keyspaces",
            "select_columns": (
                "SELECT * FROM system_schema.columns "
                "WHERE keyspace_name = ? AND table_name = ?"
            ),
        }

        for name, query in statements_to_prepare.items():
            try:
                self.prepared_statements[name] = await loop.run_in_executor(
                    None,
                    self.session.prepare,
                    query,
                )
                logger.debug(f"Prepared statement: {name}")
            except Exception as e:
                logger.warning(f"Failed to prepare statement {name}: {e}")

    async def execute_async(
        self, statement: Any, parameters: Optional[Any] = None
    ) -> Any:
        """Execute a statement asynchronously and return the result.

        Args:
            statement: CQL statement or prepared statement
            parameters: Optional parameters for the statement

        Returns:
            Query result

        Raises:
            CassandraQueryError: If query execution fails
        """
        if not self._is_connected:
            raise CassandraQueryError("Not connected to Cassandra")

        try:
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

            # Wait for the result with timeout
            return await asyncio.wait_for(future, timeout=QUERY_TIMEOUT)
        except asyncio.TimeoutError:
            raise CassandraQueryError(f"Query timeout after {QUERY_TIMEOUT} seconds")
        except Exception as e:
            raise CassandraQueryError(f"Query execution failed: {e}") from e

    def get_all_hosts(self) -> List[Any]:
        """Get all hosts in the cluster.

        Returns:
            List of host objects from cluster metadata
        """
        if not self.cluster or not self._is_connected:
            logger.warning("Cannot get hosts: not connected to cluster")
            return []

        # Get all hosts from cluster metadata
        return list(self.cluster.metadata.all_hosts())

    async def execute_on_host(
        self, host_address: str, statement: str, parameters: Optional[Any] = None
    ) -> Any:
        """Execute a statement on a specific host using a dedicated execution profile."""
        # Create execution profile for specific host
        profile_name = f"host_{host_address.replace('.', '_').replace(':', '_')}"

        # Check if profile already exists
        if profile_name not in self._execution_profiles:
            # Create new profile for this host
            profile = ExecutionProfile(
                load_balancing_policy=WhiteListRoundRobinPolicy([host_address]),
                consistency_level=ConsistencyLevel.ONE,
            )
            self.cluster.add_execution_profile(profile_name, profile)
            self._execution_profiles.add(profile_name)
            logger.debug(f"Created execution profile for host {host_address}")

        # Execute the statement using the host-specific profile
        # Log query but truncate if too long
        query_log = statement[:100] + "..." if len(statement) > 100 else statement
        logger.debug(f"Executing query on host {host_address}: {query_log}")
        try:
            # Create a future that we can await
            loop = asyncio.get_event_loop()
            future = loop.create_future()

            # Execute with specific profile
            response_future = self.session.execute_async(
                statement, parameters, execution_profile=profile_name
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
        if not self._is_connected:
            logger.debug("Not connected, skipping disconnect")
            return

        logger.info("Disconnecting from Cassandra")
        try:
            if self.session:
                self.session.shutdown()
            if self.cluster:
                self.cluster.shutdown()
        except Exception as e:
            logger.error(f"Error during disconnect: {e}")
        finally:
            # Always mark as disconnected and clear resources, even on error
            self._is_connected = False
            self._execution_profiles.clear()
            self.prepared_statements.clear()

    async def __aenter__(self) -> "CassandraConnection":
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        self.disconnect()
