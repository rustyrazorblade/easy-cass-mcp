import logging
from typing import Any, Dict, Optional

from cassandra.cluster import Session
from cassandra.metadata import TableMetadata

from exceptions import CassandraMetadataError

logger = logging.getLogger(__name__)


class CassandraTable:
    """Abstraction for Cassandra table operations and metadata."""

    def __init__(self, session: Session, keyspace: str, table: str) -> None:
        self.session = session
        self.keyspace = keyspace
        self.table = table
        self._metadata: Optional[TableMetadata] = None

    def _get_metadata(self) -> TableMetadata:
        """Get table metadata from cluster metadata."""
        if self._metadata:
            return self._metadata

        try:
            cluster_metadata = self.session.cluster.metadata
            keyspace_metadata = cluster_metadata.keyspaces.get(self.keyspace)
            if not keyspace_metadata:
                raise CassandraMetadataError(f"Keyspace '{self.keyspace}' not found")

            table_metadata = keyspace_metadata.tables.get(self.table)
            if not table_metadata:
                raise CassandraMetadataError(
                    f"Table '{self.table}' not found in keyspace '{self.keyspace}'"
                )

            self._metadata = table_metadata
            return self._metadata
        except CassandraMetadataError:
            raise
        except Exception as e:
            logger.error(
                f"Failed to get metadata for {self.keyspace}.{self.table}: {e}"
            )
            raise CassandraMetadataError(
                f"Failed to get metadata for {self.keyspace}.{self.table}: {e}"
            ) from e

    async def get_compaction_strategy(self) -> Dict[str, Any]:
        """Get the compaction strategy configuration for this table.

        Returns a dictionary with:
        - class: The compaction strategy class name
        - options: Dictionary of compaction strategy options
        """
        metadata = self._get_metadata()

        # Extract compaction info from options
        compaction_options = metadata.options.get("compaction", {})

        if isinstance(compaction_options, dict):
            strategy_class = compaction_options.get(
                "class", "SizeTieredCompactionStrategy"
            )
            # Remove 'class' from options to get just the strategy-specific options
            options = {k: v for k, v in compaction_options.items() if k != "class"}
        else:
            # Fallback for older versions or different formats
            strategy_class = "SizeTieredCompactionStrategy"
            options = {}

        # Simplify class name if it's fully qualified
        if "." in strategy_class:
            strategy_class = strategy_class.split(".")[-1]

        return {"class": strategy_class, "options": options}

    async def get_create_statement(self) -> str:
        """Get the CREATE TABLE statement for this table."""
        metadata = self._get_metadata()
        return metadata.export_as_string()
