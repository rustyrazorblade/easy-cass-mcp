import logging
from typing import Optional

from cassandra.cluster import Session

from .cassandra_table import CassandraTable
from .cassandra_version import CassandraVersion
from .exceptions import CassandraVersionError

logger = logging.getLogger(__name__)


class CassandraUtility:
    """Utility class for common Cassandra operations."""

    def __init__(self, session: Session) -> None:
        self.session = session
        self._version_cache: Optional[CassandraVersion] = None

    async def get_version(self) -> CassandraVersion:
        """Get Cassandra version as a CassandraVersion object.

        First attempts to get version from driver metadata, falls back to system.local query.
        Raises CassandraVersionError if version cannot be determined.
        """
        if self._version_cache:
            return self._version_cache

        version_str = None

        try:
            # First try to get version from driver metadata
            if (
                hasattr(self.session.cluster, "metadata")
                and self.session.cluster.metadata
            ):
                if hasattr(self.session.cluster.metadata, "cluster_name"):
                    # Try to get from control connection
                    control_conn = getattr(
                        self.session.cluster, "control_connection", None
                    )
                    if control_conn and hasattr(
                        control_conn, "get_control_connection_host"
                    ):
                        host = control_conn.get_control_connection_host()
                        if host and hasattr(host, "release_version"):
                            version_str = host.release_version

            # Fallback to querying system.local if not found
            if not version_str:
                result = self.session.execute(
                    "SELECT release_version FROM system.local"
                )
                if result:
                    row = result.one()
                    if row and row.release_version:
                        version_str = row.release_version

        except Exception as e:
            logger.error(f"Failed to get Cassandra version: {e}")
            raise CassandraVersionError(
                f"Unable to determine Cassandra version: {e}"
            ) from e

        if not version_str:
            raise CassandraVersionError(
                "Unable to determine Cassandra version: no version information available"
            )

        self._version_cache = self._parse_version(version_str)
        return self._version_cache

    def _parse_version(self, version_str: str) -> CassandraVersion:
        """Parse version string into CassandraVersion object."""
        try:
            # Handle versions like "4.0.11" or "5.0.0-SNAPSHOT"
            version_parts = version_str.split("-")[0].split(".")
            major = int(version_parts[0])
            minor = int(version_parts[1]) if len(version_parts) > 1 else 0
            patch = int(version_parts[2]) if len(version_parts) > 2 else 0
            return CassandraVersion(major, minor, patch)
        except (ValueError, IndexError) as e:
            logger.error(f"Failed to parse version '{version_str}': {e}")
            raise CassandraVersionError(
                f"Invalid version format '{version_str}': {e}"
            ) from e

    def get_table(self, keyspace: str, table: str) -> CassandraTable:
        """Create a CassandraTable object for the specified keyspace and table.

        Args:
            keyspace: The keyspace name
            table: The table name

        Returns:
            CassandraTable object for interacting with the table
        """
        return CassandraTable(self.session, keyspace, table)
