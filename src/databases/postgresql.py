import psycopg2
import psycopg2.extras
from loguru import logger
from typing import Dict, Optional, List, Any
from data_sources.data_source_config import DataSourceConfig, DataProcessorType
from databases.database_config import DatabaseProtocolTrait


class PostgreSQLManager(DatabaseProtocolTrait):
    """
    Generic PostgreSQL manager that handles connections and table operations.
    """

    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        """
        Initialise the PostgreSQL manager.

        Args:
            host: PostgreSQL server host
            port: PostgreSQL server port
            database: Database name to connect to
            user: Username for authentication
            password: Password for authentication
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connection: Optional[psycopg2.extensions.connection] = None
        self.cursor: Optional[psycopg2.extras.DictCursor] = None

    def connect(self) -> Optional[psycopg2.extensions.connection]:
        """
        Establish a connection to PostgreSQL.

        Returns:
            PostgreSQL connection object or None if connection fails
        """
        if not all([self.host, self.database, self.user, self.password]):
            logger.warning("Missing required connection parameters for PostgreSQL")
            raise ValueError("Missing required connection parameters for PostgreSQL")

        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
            )
            self.cursor = self.connection.cursor(
                cursor_factory=psycopg2.extras.DictCursor
            )
            logger.success("PostgreSQL Connection Made")
            return self.connection
        except psycopg2.Error as e:
            logger.warning(f"An error occurred with PostgreSQL: {e}")
            raise e

    def create_table(self, schema: str, table: str, columns: Dict[str, str]) -> bool:
        """
        Create a table in PostgreSQL with specified schema and columns.

        Args:
            schema: Schema name (must already exist)
            table: Table name to create
            columns: Dictionary of column names and their types

        Returns:
            Boolean indicating success
        """
        if self.connection is None:
            self.connect()

        if not self.connection or not self.cursor:
            return False

        # Build column definitions from dictionary
        column_defs = ",\n                ".join(
            [f'"{col_name}" {col_type}' for col_name, col_type in columns.items()]
        )
        logger.info(f"Creating table {schema}.{table} with columns: {column_defs}")

        try:
            table_command = f"""DROP TABLE IF EXISTS "{schema}"."{table}" CASCADE;
            CREATE TABLE "{schema}"."{table}" (
                {column_defs}
            );"""

            self.cursor.execute(table_command)
            self.connection.commit()
            logger.success(f"PostgreSQL table '{schema}.{table}' created successfully")
            return True
        except Exception as e:
            logger.error(f"Error creating table: {e}")
            self.connection.rollback()
            raise

    def create_table_from_data_source(self, config: DataSourceConfig) -> bool:
        """
        Create tables based on a data source configuration.

        Args:
            config: DataSourceConfig object containing schema and table information

        Returns:
            Boolean indicating success
        """
        # Check if processor type is PostgreSQL
        if config.processor_type != DataProcessorType.POSTGRESQL:
            logger.warning(
                f"Data source configured for processor type {config.processor_type}, not PostgreSQL"
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
                if hasattr(config, "get_table_template"):
                    table_schema = config.get_table_template(table_name)
                elif (
                    isinstance(config.db_template, dict)
                    and table_name in config.db_template
                ):
                    table_schema = config.db_template[table_name]
                else:
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

        if not self.connection or not self.cursor:
            return False

        try:
            self.cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')
            self.connection.commit()
            logger.success(f"Schema '{schema}' created or already exists")
            return True
        except Exception as e:
            logger.error(f"Error creating schema: {e}")
            self.connection.rollback()
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

    def execute_query(self, query: str) -> Optional[List[Any]]:
        """
        Execute a query and return results.

        Args:
            query: SQL query to execute

        Returns:
            List of results or None if query fails
        """
        if self.connection is None:
            self.connect()

        if not self.connection or not self.cursor:
            return None

        try:
            self.cursor.execute(query)
            if query.strip().upper().startswith("SELECT"):
                return self.cursor.fetchall()
            else:
                self.connection.commit()
                return []
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            self.connection.rollback()
            raise

    def close(self):
        """Close the PostgreSQL connection."""
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("PostgreSQL Connection Closed")

    def __enter__(self):
        """Context manager entry point."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit point."""
        self.close()
