from typing import Optional, List, Union
from datetime import date, timedelta, datetime
from .data_source_config import (
    DataProcessorType,
    DataSourceType,
    TimeRange,
    DataSourceConfig,
)


class StreetManager(DataSourceConfig):
    """
    Configuration class for Street Manager data source.
    Implements the DataSourceConfigProtocol.
    """

    def __init__(
        self,
        processor_type: DataProcessorType,
        time_range: TimeRange,
        batch_limit: Optional[int] = None,
        year: Optional[int] = None,
        start_month: Optional[int] = None,
        end_month: Optional[int] = None,
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
        self._source_type = DataSourceType.STREET_MANAGER

        # Set default values for year and month range
        self.year = year if year is not None else datetime.now().year - 1
        self.start_month = start_month if start_month is not None else 1
        self.end_month = end_month if end_month is not None else 13

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
        Get the download links for Street Manager data.

        Returns:
            A list of download links based on the time range
        """
        base_url = self.base_url.rstrip("/")

        if self.time_range == TimeRange.LATEST:
            # For latest data, generate the last month's link directly
            current_date = date.today()
            first_day_of_current_month = date(current_date.year, current_date.month, 1)
            last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
            year = last_day_of_previous_month.year
            month = f"{last_day_of_previous_month.month:02d}"

            return [f"{base_url}/{year}/{month}.zip"]

        elif self.time_range == TimeRange.HISTORIC:
            # For historic data, use the specified year and month range
            links = []
            for month in range(self.start_month, self.end_month):
                month_formatted = f"{month:02d}"
                url = f"{base_url}/{self.year}/{month_formatted}.zip"
                links.append(url)

            return links

        # Default fallback
        return ["NO Download Links Generated"]

    def last_month(self) -> list:
        """
        Get the previous month's year and month.

        Returns:
            [2024, "03"] if you run it in April 2024
        """
        current_date = date.today()
        first_day_of_current_month = date(current_date.year, current_date.month, 1)
        last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
        year = last_day_of_previous_month.year
        month = f"{last_day_of_previous_month.month:02d}"
        return [year, month]

    def date_for_table(self) -> Union[str, List[str]]:
        """
        Creates formatted date string(s) for table names based on time range.

        Returns:
            For LATEST: A string like "03_2024" for the previous month
            For HISTORIC: A list of strings like ["01_2023", "02_2023", ...]
        """
        if self.time_range == TimeRange.LATEST:
            # Get previous month
            year_month = self.last_month()
            year = str(year_month[0])
            month = year_month[1]
            return f"{month}_{year}"

        elif self.time_range == TimeRange.HISTORIC:
            # Extract dates from generated download links
            table_names = []
            for link in self.download_links:
                parts = link.split("/")
                year = parts[-2]
                month = parts[-1].replace(".zip", "")
                table_names.append(f"{month}_{year}")
            return table_names

        else:
            raise ValueError("Invalid time range")

    @property
    def table_names(self) -> List[str]:
        """
        Get all table names when multiple historic tables are available.
        """

        date_suffix = self.date_for_table()

        if isinstance(date_suffix, str):
            # Single table case
            return [f"{date_suffix}"]

        elif isinstance(date_suffix, list):
            # Multiple tables case
            return [f"{suffix}" for suffix in date_suffix]

        else:
            raise ValueError("Invalid date suffix")

    @property
    def schema_name(self) -> str:
        """
        Get the schema name for the Street Manager data based on last month.
        """
        if self.time_range == TimeRange.LATEST:
            year_month = self.last_month()
            previous_month_year = year_month[0]
            return f"raw_data_{previous_month_year}"
        elif self.time_range == TimeRange.HISTORIC:
            return f"raw_data_{self.year}"
        return f"raw_data_{date.today().year}"

    @property
    def db_template(self) -> dict:
        return {
            "version": "BIGINT",
            "event_reference": "BIGINT",
            "event_type": "VARCHAR",
            "event_time": "VARCHAR",
            "object_type": "VARCHAR",
            "object_reference": "VARCHAR",
            "work_reference_number": "VARCHAR",
            "work_category": "VARCHAR",
            "work_category_ref": "VARCHAR",
            "work_status": "VARCHAR",
            "work_status_ref": "VARCHAR",
            "activity_type": "VARCHAR",
            "permit_reference_number": "VARCHAR",
            "permit_status": "VARCHAR",
            "permit_conditions": "VARCHAR",
            "collaborative_working": "VARCHAR",
            "collaboration_type": "VARCHAR",
            "collaboration_type_ref": "VARCHAR",
            "promoter_swa_code": "VARCHAR",
            "promoter_organisation": "VARCHAR",
            "highway_authority": "VARCHAR",
            "highway_authority_swa_code": "VARCHAR",
            "works_location_coordinates": "VARCHAR",
            "works_location_type": "VARCHAR",
            "town": "VARCHAR",
            "street_name": "VARCHAR",
            "usrn": "VARCHAR",
            "road_category": "VARCHAR",
            "area_name": "VARCHAR",
            "traffic_management_type": "VARCHAR",
            "traffic_management_type_ref": "VARCHAR",
            "current_traffic_management_type": "VARCHAR",
            "current_traffic_management_type_ref": "VARCHAR",
            "current_traffic_management_update_date": "VARCHAR",
            "proposed_start_date": "VARCHAR",
            "proposed_start_time": "VARCHAR",
            "proposed_end_date": "VARCHAR",
            "proposed_end_time": "VARCHAR",
            "actual_start_date_time": "VARCHAR",
            "actual_end_date_time": "VARCHAR",
            "is_ttro_required": "VARCHAR",
            "is_covid_19_response": "VARCHAR",
            "is_traffic_sensitive": "VARCHAR",
            "is_deemed": "VARCHAR",
            "close_footway": "VARCHAR",
            "close_footway_ref": "VARCHAR",
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
    def create_default_latest(cls) -> "StreetManager":
        """Create a default Street Manager configuration."""
        return cls(
            processor_type=DataProcessorType.MOTHERDUCK,
            time_range=TimeRange.LATEST,
            batch_limit=300000,
        )

    @classmethod
    def create_default_historic_2025(cls) -> "StreetManager":
        """Create a default Street Manager configuration."""
        return cls(
            processor_type=DataProcessorType.MOTHERDUCK,
            time_range=TimeRange.HISTORIC,
            batch_limit=150000,
            year=2025,
            start_month=3,
            end_month=5,
        )

    @classmethod
    def create_default_historic_2024(cls) -> "StreetManager":
        """Create a default Street Manager configuration."""
        return cls(
            processor_type=DataProcessorType.MOTHERDUCK,
            time_range=TimeRange.HISTORIC,
            batch_limit=150000,
            year=2024,
            start_month=1,
            end_month=13,
        )

    @classmethod
    def create_default_historic_2023(cls) -> "StreetManager":
        """Create a default Street Manager configuration."""
        return cls(
            processor_type=DataProcessorType.MOTHERDUCK,
            time_range=TimeRange.HISTORIC,
            batch_limit=150000,
            year=2023,
            start_month=1,
            end_month=13,
        )

    @classmethod
    def create_default_historic_2022(cls) -> "StreetManager":
        """Create a default Street Manager configuration."""
        return cls(
            processor_type=DataProcessorType.MOTHERDUCK,
            time_range=TimeRange.HISTORIC,
            batch_limit=200000,
            year=2022,
            start_month=1,
            end_month=13,
        )


if __name__ == "__main__":
    config = StreetManager.create_default_historic_2025()
    print(config)
