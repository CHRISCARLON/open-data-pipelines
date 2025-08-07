from typing import Optional, List
from .data_source_config import (
    DataProcessorType,
    DataSourceType,
    TimeRange,
    DataSourceConfig,
)


class NationalStatisticPostcodeLookup(DataSourceConfig):
    """
    Configuration class for National Statistic Postcode Lookup data source.
    Implements the DataSourceConfigProtocol.
    """

    def __init__(
        self,
        processor_type: DataProcessorType,
        time_range: TimeRange,
        batch_limit: Optional[int] = None,
    ):
        """
        Initialise National Statistic Postcode Lookup configuration.

        Args:
            processor_type: The type of data processor to use
            time_range: The time range for the data (always uses 202503 data)
            batch_limit: Optional limit for batch processing
        """
        self._processor_type = processor_type
        self._time_range = time_range
        self.batch_limit = batch_limit
        self._source_type = DataSourceType.NATIONAL_STATISTIC_POSTCODE_LOOKUP

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
        Get the download links for National Statistic Postcode Lookup data.
        """
        return [self.source_type.base_url]

    @property
    def table_names(self) -> List[str]:
        """Get the table name for National Statistic Postcode Lookup data."""
        return ["national_statistic_postcode_lookup"]

    @property
    def schema_name(self) -> str:
        """Get the schema name for the Postcode P001 data."""
        return "post_code_data"

    @property
    def db_template(self) -> dict:
        """
        Database template for National Statistics Postcode Lookup data.
        Uses SQL-safe column names.
        """
        return {
            "pcd": "VARCHAR",
            "pcd2": "VARCHAR",
            "pcds": "VARCHAR",
            "dointr": "BIGINT",
            "doterm": "BIGINT",
            "usertype": "BIGINT",
            "oseast1m": "BIGINT",
            "osnrth1m": "VARCHAR",
            "osgrdind": "BIGINT",
            "oa21": "VARCHAR",
            "cty": "VARCHAR",
            "ced": "VARCHAR",
            "laua": "VARCHAR",
            "ward": "VARCHAR",
            "nhser": "VARCHAR",
            "ctry": "VARCHAR",
            "rgn": "VARCHAR",
            "pcon": "VARCHAR",
            "ttwa": "VARCHAR",
            "itl": "VARCHAR",
            "park": "VARCHAR",
            "lsoa21": "VARCHAR",
            "msoa21": "VARCHAR",
            "wz11": "VARCHAR",
            "sicbl": "VARCHAR",
            "bua24": "VARCHAR",
            "ruc21": "VARCHAR",
            "oac11": "VARCHAR",
            "lat": "DOUBLE",
            "long": "DOUBLE",
            "lep1": "VARCHAR",
            "lep2": "VARCHAR",
            "pfa": "VARCHAR",
            "imd": "BIGINT",
            "icb": "VARCHAR"
        }

    @property
    def metadata_schema_name(self) -> str:
        """Get the metadata schema name for tracking processing information."""
        return f"{self.schema_name}"

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
        return (
            f"NationalStatisticPostcodeLookup(processor={self.processor_type.value}, "
            f"source={self.source_type.code}, "
            f"time_range={self.time_range.value}, "
            f"batch_limit={self.batch_limit}, "
            f"download_links={self.download_links}, "
            f"schema_name={self.schema_name}, "
            f"table_names={self.table_names})"
        )

    @classmethod
    def create_default(cls) -> "NationalStatisticPostcodeLookup":
        """Create a default National Statistic Postcode Lookup configuration."""
        return cls(
            processor_type=DataProcessorType.MOTHERDUCK,
            time_range=TimeRange.LATEST,
            batch_limit=250000,
        )


if __name__ == "__main__":
    config = NationalStatisticPostcodeLookup.create_default()
    print(config)
