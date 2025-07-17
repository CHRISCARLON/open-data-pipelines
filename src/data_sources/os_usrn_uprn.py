from typing import Optional, List
from .data_source_config import (
    DataProcessorType,
    DataSourceType,
    TimeRange,
    DataSourceConfig,
)
from datetime import datetime
from datetime import timedelta


class OsUsrnUprn(DataSourceConfig):
    """
    Configuration class for Street Manager data source.
    Implements the DataSourceConfigProtocol.
    """

    def __init__(
        self,
        processor_type: DataProcessorType,
        time_range: TimeRange,
        batch_limit: Optional[int] = None,
    ):
        """
        Initialise a Street Manager configuration.

        Args:
            processor_type: The type of data processor to use
            time_range: The time range for the data
            batch_limit: Optional limit for batch processing
            year: Specific year for historic data (defaults to previous year)
            start_month: Starting month for historic data (1-12, defaults to 1)
            end_month: Ending month for historic data (non-inclusive, 1-13, defaults to 13)
        """
        self._processor_type = processor_type
        self._time_range = time_range
        self.batch_limit = batch_limit
        self._source_type = DataSourceType.OS_USRN_UPRN

    @property
    def processor_type(self) -> DataProcessorType:
        return self._processor_type

    @property
    def source_type(self) -> DataSourceType:
        return self._source_type

    @property
    def time_range(self) -> TimeRange:
        return self._time_range

    @property
    def base_url(self) -> str:
        """Get the base URL for the configured data source."""
        return self.source_type.base_url

    @property
    def download_links(self) -> list[str]:
        """
        Constructs download URL using last month's data for USRN-UPRN.

        Returns:
            list[str]: List containing the download URL for USRN-UPRN data
        """
        # Always use last month since current month data may not be available yet
        now = datetime.now()
        last_month = now - timedelta(days=30)
        date_format = f"{last_month.year}-{last_month.month:02d}"

        file_name = f"lids-{date_format}_csv_BLPU-UPRN-Street-USRN-11.zip"
        download_url = (
            f"{self.base_url}?area=GB&format=CSV&fileName={file_name}&redirect"
        )

        return [download_url]

    @property
    def table_names(self) -> List[str]:
        """
        Get all table names when multiple historic tables are available.
        """

        return ["os_open_linked_identifiers_uprn_usrn_latest"]

    @property
    def schema_name(self) -> str:
        """
        Get the schema name for the Street Manager data based on last month.
        """
        return "os_open_linked_identifiers"

    @property
    def db_template(self) -> dict:
        return {
            "correlation_id": "VARCHAR",
            "identifier_1": "BIGINT",
            "version_number_1": "VARCHAR",
            "version_date_1": "BIGINT",
            "identifier_2": "BIGINT",
            "version_number_2": "VARCHAR",
            "version_date_2": "BIGINT",
            "confidence": "VARCHAR",
        }

    @property
    def metadata_schema_name(self) -> str:
        """Get the metadata schema name for tracking processing information."""
        return "metadata_logs"

    @property
    def metadata_table_name(self) -> str:
        """Get the metadata table name for logging processing runs."""
        return "processing_logs"

    @property
    def metadata_db_template(self) -> dict:
        """Get the database template for metadata logging table."""
        if self.processor_type == DataProcessorType.POSTGRESQL:
            return {
                "log_id": "SERIAL PRIMARY KEY",
                "data_source": "VARCHAR(100)",
                "schema_name": "VARCHAR(100)",
                "table_name": "VARCHAR(100)",
                "processor_type": "VARCHAR(50)",
                "url": "TEXT",
                "start_time": "TIMESTAMP",
                "end_time": "TIMESTAMP",
                "duration_seconds": "DOUBLE PRECISION",
                "rows_processed": "BIGINT",
                "file_size_bytes": "BIGINT",
                "status": "VARCHAR(20)",
                "error_message": "TEXT",
                "additional_info": "TEXT",
                "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            }
        else:  # MotherDuck
            return {
                "log_id": "VARCHAR(36) PRIMARY KEY",
                "data_source": "VARCHAR",
                "schema_name": "VARCHAR",
                "table_name": "VARCHAR",
                "processor_type": "VARCHAR",
                "url": "VARCHAR",
                "start_time": "TIMESTAMP",
                "end_time": "TIMESTAMP",
                "duration_seconds": "DOUBLE",
                "rows_processed": "BIGINT",
                "file_size_bytes": "BIGINT",
                "status": "VARCHAR",
                "error_message": "VARCHAR",
                "additional_info": "TEXT",
                "created_at": "TIMESTAMP",
            }

    def __str__(self) -> str:
        """String representation of the configuration."""
        links_str = ", ".join(self.download_links[:2])
        if len(self.download_links) > 2:
            links_str += f", ... ({len(self.download_links)} total)"

        return (
            f"StreetManagerConfig(processor={self.processor_type.value}, "
            f"source={self.source_type.code}, "
            f"base_url={self.base_url}, "
            f"time_range={self.time_range.value}, "
            f"batch_limit={self.batch_limit}, "
            f"download_links=[{links_str}]), "
            f"schema_name={self.schema_name}, "
            f"table_names={self.table_names}, "
            f"db_template={self.db_template}"
        )

    @classmethod
    def create_default_latest(cls) -> "OsUsrnUprn":
        """Create a default OS Open USRN configuration."""
        return cls(
            processor_type=DataProcessorType.MOTHERDUCK,
            time_range=TimeRange.LATEST,
            batch_limit=250000,
        )


if __name__ == "__main__":
    config = OsUsrnUprn.create_default_latest()
    print(config)
