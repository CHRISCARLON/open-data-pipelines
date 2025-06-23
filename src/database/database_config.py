from typing import Any, Dict, Protocol
from data_sources.data_source_config import DataSourceConfig


class DatabaseProtocolTrait(Protocol):
    """Protocol defining the interface for database managers."""

    def connect(self) -> Any:
        """Establish a connection to the database."""
        ...

    def close(self):
        """Close the database connection."""
        ...

    def create_schema_if_not_exists(self, schema: str) -> bool:
        """Create a schema if it doesn't already exist."""
        ...

    def create_table(self, schema: str, table: str, columns: Dict[str, str]) -> bool:
        """Create a table with specified schema and columns."""
        ...

    def create_table_from_data_source(self, config: DataSourceConfig) -> bool:
        """Create tables based on a data source configuration."""
        ...

    def setup_for_data_source(self, config: DataSourceConfig):
        """Complete setup for a data source - create schema and tables."""
        ...
