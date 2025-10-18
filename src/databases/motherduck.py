import duckdb
from loguru import logger
from typing import Dict, Optional
from ..data_sources.data_source_config import DataSourceConfig, DataProcessorType
from ..databases.database_config import DatabaseProtocolTrait


class MotherDuckManager(DatabaseProtocolTrait):
    """
    Generic MotherDuck manager that handles connections and table operations.
    """

    def __init__(self, token: str, database: str):
        """
        Initialise the MotherDuck manager.

        Args:
            token: MotherDuck authentication token
            database: Database name to connect to
        """
        self.token = token
        self.database = database
        self.connection = None

    def connect(self) -> Optional[duckdb.DuckDBPyConnection]:
        """
        Establish a connection to MotherDuck.

        Returns:
            DuckDB connection object or None if connection fails
        """
        if not self.token:
            logger.warning("No token provided, MotherDuck connection not made")
            raise ValueError("No token provided, MotherDuck connection not made")

        try:
            connection_string = f"md:{self.database}?motherduck_token={self.token}"
            self.connection = duckdb.connect(connection_string)
            logger.success("MotherDuck Connection Made")
            return self.connection
        except (duckdb.ConnectionException, duckdb.Error) as e:
            logger.warning(f"An error occurred with MotherDuck: {e}")
            raise e

    def create_table(self, schema: str, table: str, columns: Dict[str, str]) -> bool:
        """
        Create a table in MotherDuck with specified schema and columns.

        Args:
            schema: Schema name (must already exist)
            table: Table name to create
            columns: Dictionary of column names and their types

        Returns:
            Boolean indicating success
        """
        if self.connection is None:
            self.connect()

        if not self.connection:
            return False

        # Build column definitions from dictionary
        column_defs = ",\n                ".join(
            [f"{col_name} {col_type}" for col_name, col_type in columns.items()]
        )
        logger.info(f"Creating table {schema}.{table} with columns: {column_defs}")

        try:
            table_command = f"""CREATE OR REPLACE TABLE "{schema}"."{table}" (
                {column_defs}
            );"""
            self.connection.execute(table_command)
            logger.success(f"MotherDuck table '{schema}.{table}' created successfully")
            return True
        except Exception as e:
            logger.error(f"Error creating table: {e}")
            raise

    def create_metadata_table(
        self, schema: str, table: str, columns: Dict[str, str]
    ) -> bool:
        """
        Create a table in MotherDuck with specified schema and columns.

        Args:
            schema: Schema name (must already exist)
            table: Table name to create
            columns: Dictionary of column names and their types

        Returns:
            Boolean indicating success
        """
        if self.connection is None:
            self.connect()

        if not self.connection:
            return False

        # Build column definitions from dictionary
        column_defs = ",\n                ".join(
            [f"{col_name} {col_type}" for col_name, col_type in columns.items()]
        )
        logger.info(f"Creating table {schema}.{table} with columns: {column_defs}")

        try:
            table_command = f"""CREATE TABLE IF NOT EXISTS "{schema}"."{table}" (
                {column_defs}
            );"""
            self.connection.execute(table_command)
            logger.success(f"MotherDuck table '{schema}.{table}' created successfully")
            return True
        except Exception as e:
            logger.error(f"Error creating table: {e}")
            raise

    def create_table_from_data_source(self, config: DataSourceConfig) -> bool:
        """
        Create tables based on a data source configuration.

        Args:
            config: DataSourceConfig object containing schema and table information

        Returns:
            Boolean indicating success
        """
        # Check if processor type is MotherDuck
        if config.processor_type != DataProcessorType.MOTHERDUCK:
            logger.warning(
                f"Data source configured for processor type {config.processor_type}, not MotherDuck"
            )
            return False

        schema = config.schema_name

        if not hasattr(config, "db_template") or not config.db_template:
            logger.error(f"No db_template found in the config for {config.source_type}")
            return False

        success = True
        for table_name in config.table_names:
            try:
                # Determine the table schema
                # Check if config has get_table_template method (for data with varying schemas)
                if hasattr(config, 'get_table_template'):
                    table_schema = config.get_table_template(table_name)
                elif (
                    isinstance(config.db_template, dict)
                    and table_name in config.db_template
                ):
                    table_schema = config.db_template[table_name]
                else:
                    # For flat db_template (single table), use the entire template
                    table_schema = config.db_template

                table_success = self.create_table(schema, table_name, table_schema)
                if not table_success:
                    success = False
            except Exception as e:
                logger.error(f"Failed to create table {schema}.{table_name}: {e}")
                success = False

        return success

    def create_schema_if_not_exists(self, schema: str) -> bool:
        """
        Create a schema if it doesn't already exist.

        Args:
            schema: Schema name to create

        Returns:
            Boolean indicating success
        """
        if self.connection is None:
            self.connect()

        if not self.connection:
            return False

        try:
            self.connection.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')
            logger.success(f"Schema '{schema}' created or already exists")
            return True
        except Exception as e:
            logger.error(f"Error creating schema: {e}")
            raise

    def setup_for_data_source(self, config: DataSourceConfig):
        """
        Complete setup for a data source - create schema and tables.

        Args:
            config: DataSourceConfig object
        """
        # Create schema
        self.create_schema_if_not_exists(config.schema_name)
        self.create_table_from_data_source(config)

    def close(self):
        """Close the MotherDuck connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("MotherDuck Connection Closed")

    def __enter__(self):
        """Context manager entry point."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit point."""
        self.close()
