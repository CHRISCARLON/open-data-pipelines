from typing import Optional
from .data_source_config import (
    DataProcessorType,
    DataSourceType,
    TimeRange,
    DataSourceConfig,
)


class BODSTimetables(DataSourceConfig):
    """
    Configuration class for BODS Timetables data source in GTFS format.
    """

    def __init__(
        self,
        processor_type: DataProcessorType,
        time_range: TimeRange,
        batch_limit: Optional[int] = None,
    ):
        """
        Initialise BODS Timetables configuration.

        Args:
            processor_type: The type of data processor to use
            time_range: The time range for the data
            batch_limit: Optional limit for batch processing
        """
        self._processor_type = processor_type
        self._time_range = time_range
        self.batch_limit = batch_limit
        self._source_type = DataSourceType.BODS_TIMETABLES

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
    def download_links(self):
        """Get the download links for BODS timetables data."""
        return [self.base_url]

    @property
    def schema_name(self) -> str:
        """Get the schema name for the configured data source."""
        return "bods_timetables"

    @property
    def table_names(self) -> list[str]:
        """Get the table names for GTFS files."""
        return [
            "agency",
            "calendar",
            "calendar_dates",
            "feed_info",
            "routes",
            "shapes",
            "stops",
            "stop_times",
            "trips",
        ]

    @property
    def db_template(self) -> dict:
        """Database schema templates for all GTFS tables."""
        return {
            "agency": {
                "agency_id": "VARCHAR",
                "agency_name": "VARCHAR",
                "agency_url": "VARCHAR",
                "agency_timezone": "VARCHAR",
                "agency_lang": "VARCHAR",
                "agency_phone": "VARCHAR",
                "agency_noc": "VARCHAR",
            },
            "calendar": {
                "service_id": "VARCHAR",
                "monday": "VARCHAR",
                "tuesday": "VARCHAR",
                "wednesday": "VARCHAR",
                "thursday": "VARCHAR",
                "friday": "VARCHAR",
                "saturday": "VARCHAR",
                "sunday": "VARCHAR",
                "start_date": "VARCHAR",
                "end_date": "VARCHAR",
            },
            "calendar_dates": {
                "service_id": "VARCHAR",
                "date": "VARCHAR",
                "exception_type": "VARCHAR",
            },
            "feed_info": {
                "feed_publisher_name": "VARCHAR",
                "feed_publisher_url": "VARCHAR",
                "feed_lang": "VARCHAR",
                "feed_start_date": "VARCHAR",
                "feed_end_date": "VARCHAR",
                "feed_version": "VARCHAR",
            },
            "routes": {
                "route_id": "VARCHAR",
                "agency_id": "VARCHAR",
                "route_short_name": "VARCHAR",
                "route_long_name": "VARCHAR",
                "route_type": "VARCHAR",
            },
            "shapes": {
                "shape_id": "VARCHAR",
                "shape_pt_lat": "VARCHAR",
                "shape_pt_lon": "VARCHAR",
                "shape_pt_sequence": "VARCHAR",
                "shape_dist_traveled": "VARCHAR",
            },
            "stops": {
                "stop_id": "VARCHAR",
                "stop_code": "VARCHAR",
                "stop_name": "VARCHAR",
                "stop_lat": "VARCHAR",
                "stop_lon": "VARCHAR",
                "wheelchair_boarding": "VARCHAR",
                "location_type": "VARCHAR",
                "parent_station": "VARCHAR",
                "platform_code": "VARCHAR",
            },
            "stop_times": {
                "trip_id": "VARCHAR",
                "arrival_time": "VARCHAR",
                "departure_time": "VARCHAR",
                "stop_id": "VARCHAR",
                "stop_sequence": "VARCHAR",
                "stop_headsign": "VARCHAR",
                "pickup_type": "VARCHAR",
                "drop_off_type": "VARCHAR",
                "shape_dist_traveled": "VARCHAR",
                "timepoint": "VARCHAR",
            },
            "trips": {
                "route_id": "VARCHAR",
                "service_id": "VARCHAR",
                "trip_id": "VARCHAR",
                "trip_headsign": "VARCHAR",
                "direction_id": "VARCHAR",
                "block_id": "VARCHAR",
                "shape_id": "VARCHAR",
                "wheelchair_accessible": "VARCHAR",
                "vehicle_journey_code": "VARCHAR",
            },
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
        """String representation of the configuration."""
        links_str = ", ".join(self.download_links[:2])
        if len(self.download_links) > 2:
            links_str += f", ... ({len(self.download_links)} total)"

        return (
            f"BODSTimetablesConfig(processor={self.processor_type.value}, "
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
    def create_default_latest(cls) -> "BODSTimetables":
        return cls(
            processor_type=DataProcessorType.MOTHERDUCK,
            time_range=TimeRange.LATEST,
            batch_limit=300000,
        )


if __name__ == "__main__":
    config = BODSTimetables.create_default_latest()
    print(config)
