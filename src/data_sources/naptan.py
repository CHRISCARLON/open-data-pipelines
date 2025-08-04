from typing import Optional
from .data_source_config import (
    DataProcessorType,
    DataSourceType,
    TimeRange,
    DataSourceConfig,
)


class Naptan(DataSourceConfig):
    """
    Configuration class for NAPTAN (National Public Transport Access Nodes) data source.
    """

    def __init__(
        self,
        processor_type: DataProcessorType,
        time_range: TimeRange,
        batch_limit: Optional[int] = None,
    ):
        """
        Initialise a NAPTAN configuration.

        Args:
            processor_type: The type of data processor to use
            time_range: The time range for the data
            batch_limit: Optional limit for batch processing
        """

        self._processor_type = processor_type
        self._time_range = time_range
        self.batch_limit = batch_limit
        self._source_type = DataSourceType.NAPTAN

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
        """Get the download links for the configured data source."""
        # NAPTAN provides direct download link via base URL
        return [self.base_url]

    @property
    def schema_name(self) -> str:
        """Get the schema name for the configured data source."""
        return "naptan_data"

    @property
    def table_names(self) -> list[str]:
        """Get the table names for the configured data source."""
        return ["LATEST_STOPS"]

    @property
    def db_template(self) -> dict:
        """Get the database template for the configured data source."""
        return {
            "ATCOCode": "VARCHAR",
            "NaptanCode": "VARCHAR",
            "PlateCode": "VARCHAR",
            "CleardownCode": "VARCHAR",
            "CommonName": "VARCHAR",
            "CommonNameLang": "VARCHAR",
            "ShortCommonName": "VARCHAR",
            "ShortCommonNameLang": "VARCHAR",
            "Landmark": "VARCHAR",
            "LandmarkLang": "VARCHAR",
            "Street": "VARCHAR",
            "StreetLang": "VARCHAR",
            "Crossing": "VARCHAR",
            "CrossingLang": "VARCHAR",
            "Indicator": "VARCHAR",
            "IndicatorLang": "VARCHAR",
            "Bearing": "VARCHAR",
            "NptgLocalityCode": "VARCHAR",
            "LocalityName": "VARCHAR",
            "ParentLocalityName": "VARCHAR",
            "GrandParentLocalityName": "VARCHAR",
            "Town": "VARCHAR",
            "TownLang": "VARCHAR",
            "Suburb": "VARCHAR",
            "SuburbLang": "VARCHAR",
            "LocalityCentre": "VARCHAR",
            "GridType": "VARCHAR",
            "Easting": "BIGINT",
            "Northing": "BIGINT",
            "Longitude": "DOUBLE",
            "Latitude": "DOUBLE",
            "StopType": "VARCHAR",
            "BusStopType": "VARCHAR",
            "TimingStatus": "VARCHAR",
            "DefaultWaitTime": "VARCHAR",
            "Notes": "VARCHAR",
            "NotesLang": "VARCHAR",
            "AdministrativeAreaCode": "VARCHAR",
            "CreationDateTime": "TIMESTAMP",
            "ModificationDateTime": "TIMESTAMP",
            "RevisionNumber": "BIGINT",
            "Modification": "VARCHAR",
            "Status": "VARCHAR",
        }

    @property
    def metadata_schema_name(self) -> str:
        return f"{self.schema_name}"

    @property
    def metadata_table_name(self) -> str:
        return "processing_logs"

    @property
    def metadata_db_template(self) -> dict:
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
        return (
            f"NAPTAN Configuration: "
            f"processor={self.processor_type.value}, "
            f"source={self.source_type.code}, "
            f"base_url={self.base_url}, "
            f"time_range={self.time_range.value}, "
            f"batch_limit={self.batch_limit}, "
            f"download_links={self.download_links}, "
            f"schema_name={self.schema_name}, "
            f"table_names={self.table_names}"
        )

    @classmethod
    def create_default_latest(cls) -> "Naptan":
        return cls(
            processor_type=DataProcessorType.MOTHERDUCK,
            time_range=TimeRange.LATEST,
            batch_limit=100000,
        )

    @classmethod
    def create_postgresql_latest(cls) -> "Naptan":
        return cls(
            processor_type=DataProcessorType.POSTGRESQL,
            time_range=TimeRange.LATEST,
            batch_limit=100000,
        )


if __name__ == "__main__":
    config = Naptan.create_default_latest()
    config2 = Naptan.create_postgresql_latest()
    print(config)
    print(config2)
