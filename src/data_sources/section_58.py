from typing import Optional, List, Dict
from datetime import date, timedelta, datetime
from .data_source_config import (
    DataProcessorType,
    DataSourceType,
    TimeRange,
    DataSourceConfig,
)


class Section58(DataSourceConfig):
    """
    Configuration class for Section 58 data from Street Manager.
    Implements SCD Type 2 with staging and dimension tables.
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
        Initialise a Section 58 configuration.

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
        self._source_type = DataSourceType.SECTION_58

        # Set default values for year and month range
        self.year = year if year is not None else datetime.now().year - 1
        self.start_month = start_month if start_month is not None else 1
        self.end_month = end_month if end_month is not None else 13

        # Define schema names for Section 58 tables
        self.staging_schema = "section_58"
        self.staging_table = "staging_section_58"
        self.dimension_schema = "section_58"
        self.dimension_table = "dim_section_58"

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

    @property
    def staging_db_template(self) -> Dict[str, str]:
        """
        Staging table schema - all VARCHAR for easy loading from JSON.
        Maps directly to the flattened JSON structure from Street Manager.
        """
        return {
            "section_58_reference_number": "VARCHAR",
            "section_58_coordinates": "VARCHAR",
            "section_58_status": "VARCHAR",
            "start_date": "VARCHAR",
            "end_date": "VARCHAR",
            "section_58_duration": "VARCHAR",
            "section_58_extent": "VARCHAR",
            "section_58_location_type": "VARCHAR",
            "status_change_date": "VARCHAR",
            "highway_authority_swa_code": "VARCHAR",
            "highway_authority": "VARCHAR",
            "usrn": "VARCHAR",
            "street_name": "VARCHAR",
            "area_name": "VARCHAR",
            "town": "VARCHAR",
            "event_reference": "BIGINT",
            "event_type": "VARCHAR",
            "event_time": "VARCHAR",
            "object_type": "VARCHAR",
            "object_reference": "VARCHAR",
            "version": "INTEGER",
        }

    @property
    def dimension_db_template(self) -> Dict[str, str]:
        """
        Dimension table schema with proper data types and SCD Type 2 fields.
        """
        return {
            "surrogate_key": "INTEGER",
            "section_58_reference_number": "VARCHAR",
            "usrn": "VARCHAR",
            "status": "VARCHAR",
            "start_date": "DATE",
            "end_date": "DATE",
            "duration": "VARCHAR",
            "extent": "VARCHAR",
            "location_type": "VARCHAR",
            "coordinates": "VARCHAR",
            "status_change_date": "TIMESTAMP",
            "highway_authority_swa_code": "VARCHAR",
            "highway_authority": "VARCHAR",
            "street_name": "VARCHAR",
            "area_name": "VARCHAR",
            "town": "VARCHAR",
            "event_type": "VARCHAR",
            "event_time": "TIMESTAMP",
            "valid_from": "TIMESTAMP",
            "valid_to": "TIMESTAMP",
            "is_current": "BOOLEAN",
            "record_hash": "VARCHAR",
        }

    @property
    def schema_name(self) -> str:
        """
        For compatibility with existing code - returns staging schema.
        """
        return self.staging_schema

    @property
    def table_names(self) -> List[str]:
        """
        For compatibility with existing code - returns staging table.
        """
        return [self.staging_table]

    @property
    def db_template(self) -> dict:
        """
        For compatibility with existing code - returns staging template.
        """
        return self.staging_db_template

    @property
    def metadata_schema_name(self) -> str:
        """Get the metadata schema name for tracking processing information."""
        return f"{self.staging_schema}"

    @property
    def metadata_table_name(self) -> str:
        """Get the metadata table name for logging processing runs."""
        return "processing_logs"

    @property
    def metadata_db_template(self) -> dict:
        """Get the database template for metadata logging table."""
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

    def get_scd_sql(self) -> Dict[str, str]:
        """
        Returns SQL statements for SCD Type 2 processing.
        """
        return {
            "initial_load": f"""
                INSERT INTO {self.dimension_schema}.{self.dimension_table} (
                    surrogate_key, section_58_reference_number, usrn, status,
                    start_date, end_date, duration, extent, location_type, coordinates,
                    status_change_date, highway_authority_swa_code, highway_authority,
                    street_name, area_name, town, event_type, event_time,
                    valid_from, valid_to, is_current, record_hash
                )
                SELECT
                    nextval('s58_seq'),
                    latest.section_58_reference_number,
                    latest.usrn,
                    latest.section_58_status,
                    TRY_CAST(SUBSTR(latest.start_date, 1, 10) AS DATE),
                    TRY_CAST(SUBSTR(latest.end_date, 1, 10) AS DATE),
                    latest.section_58_duration,
                    latest.section_58_extent,
                    latest.section_58_location_type,
                    latest.section_58_coordinates,
                    TRY_CAST(latest.status_change_date AS TIMESTAMP),
                    latest.highway_authority_swa_code,
                    latest.highway_authority,
                    latest.street_name,
                    latest.area_name,
                    latest.town,
                    latest.event_type,
                    TRY_CAST(latest.event_time AS TIMESTAMP),
                    CURRENT_TIMESTAMP,
                    '9999-12-31'::TIMESTAMP,
                    TRUE,
                    md5(concat(
                        latest.section_58_status, '|',
                        latest.start_date, '|',
                        latest.end_date, '|',
                        latest.section_58_duration, '|',
                        latest.section_58_extent, '|',
                        latest.section_58_location_type
                    ))
                FROM (
                    SELECT s.*,
                           ROW_NUMBER() OVER (
                               PARTITION BY s.section_58_reference_number
                               ORDER BY TRY_CAST(s.event_time AS TIMESTAMP) DESC,
                                       s.event_reference DESC
                           ) as rn
                    FROM {self.staging_schema}.{self.staging_table} s
                    WHERE s.object_type = 'SECTION_58'
                ) latest
                WHERE latest.rn = 1
            """,
            "mark_changed": f"""
                UPDATE {self.dimension_schema}.{self.dimension_table} d
                SET
                    valid_to = CURRENT_TIMESTAMP,
                    is_current = FALSE
                WHERE d.is_current = TRUE
                AND EXISTS (
                    SELECT 1
                    FROM (
                        SELECT s.*,
                               ROW_NUMBER() OVER (
                                   PARTITION BY s.section_58_reference_number
                                   ORDER BY TRY_CAST(s.event_time AS TIMESTAMP) DESC,
                                           s.event_reference DESC
                               ) as rn
                        FROM {self.staging_schema}.{self.staging_table} s
                        WHERE s.object_type = 'SECTION_58'
                    ) latest_staging
                    WHERE latest_staging.rn = 1
                    AND latest_staging.section_58_reference_number = d.section_58_reference_number
                    AND TRY_CAST(latest_staging.event_time AS TIMESTAMP) > d.event_time
                )
            """,
            "insert_new_changed": f"""
                INSERT INTO {self.dimension_schema}.{self.dimension_table} (
                    surrogate_key, section_58_reference_number, usrn, status,
                    start_date, end_date, duration, extent, location_type, coordinates,
                    status_change_date, highway_authority_swa_code, highway_authority,
                    street_name, area_name, town, event_type, event_time,
                    valid_from, valid_to, is_current, record_hash
                )
                SELECT
                    nextval('s58_seq'),
                    latest.section_58_reference_number,
                    latest.usrn,
                    latest.section_58_status,
                    TRY_CAST(SUBSTR(latest.start_date, 1, 10) AS DATE),
                    TRY_CAST(SUBSTR(latest.end_date, 1, 10) AS DATE),
                    latest.section_58_duration,
                    latest.section_58_extent,
                    latest.section_58_location_type,
                    latest.section_58_coordinates,
                    TRY_CAST(latest.status_change_date AS TIMESTAMP),
                    latest.highway_authority_swa_code,
                    latest.highway_authority,
                    latest.street_name,
                    latest.area_name,
                    latest.town,
                    latest.event_type,
                    TRY_CAST(latest.event_time AS TIMESTAMP),
                    CURRENT_TIMESTAMP,
                    '9999-12-31'::TIMESTAMP,
                    TRUE,
                    md5(concat(
                        latest.section_58_status, '|',
                        latest.start_date, '|',
                        latest.end_date, '|',
                        latest.section_58_duration, '|',
                        latest.section_58_extent, '|',
                        latest.section_58_location_type
                    ))
                FROM (
                    SELECT s.*,
                           ROW_NUMBER() OVER (
                               PARTITION BY s.section_58_reference_number
                               ORDER BY TRY_CAST(s.event_time AS TIMESTAMP) DESC,
                                       s.event_reference DESC
                           ) as rn
                    FROM {self.staging_schema}.{self.staging_table} s
                    WHERE s.object_type = 'SECTION_58'
                ) latest
                WHERE latest.rn = 1
                AND (
                    NOT EXISTS (
                        SELECT 1
                        FROM {self.dimension_schema}.{self.dimension_table} d
                        WHERE d.section_58_reference_number = latest.section_58_reference_number
                    )
                    OR
                    TRY_CAST(latest.event_time AS TIMESTAMP) > (
                        SELECT MAX(d.event_time)
                        FROM {self.dimension_schema}.{self.dimension_table} d
                        WHERE d.section_58_reference_number = latest.section_58_reference_number
                    )
                )
            """,
            "clear_staging": f"TRUNCATE {self.staging_schema}.{self.staging_table}",
        }

    def __str__(self) -> str:
        """String representation of the configuration."""
        links_str = ", ".join(self.download_links[:2])
        if len(self.download_links) > 2:
            links_str += f", ... ({len(self.download_links)} total)"

        return (
            f"Section58Config(processor={self.processor_type.value}, "
            f"source={self.source_type.code}, "
            f"time_range={self.time_range.value}, "
            f"batch_limit={self.batch_limit}, "
            f"download_links=[{links_str}]), "
            f"staging={self.staging_schema}.{self.staging_table}, "
            f"dimension={self.dimension_schema}.{self.dimension_table}"
        )

    @classmethod
    def create_default_latest(cls) -> "Section58":
        """Create a default Section 58 configuration for latest month."""
        return cls(
            processor_type=DataProcessorType.MOTHERDUCK,
            time_range=TimeRange.LATEST,
            batch_limit=20000,
        )

    @classmethod
    def create_default_historic_2025(cls) -> "Section58":
        """Create a default Section 58 configuration for 2025."""
        return cls(
            processor_type=DataProcessorType.MOTHERDUCK,
            time_range=TimeRange.HISTORIC,
            batch_limit=20000,
            year=2025,
            start_month=1,
            end_month=13,
        )

    @classmethod
    def create_default_historic_2024(cls) -> "Section58":
        """Create a default Section 58 configuration for 2024."""
        return cls(
            processor_type=DataProcessorType.MOTHERDUCK,
            time_range=TimeRange.HISTORIC,
            batch_limit=20000,
            year=2024,
            start_month=1,
            end_month=13,
        )

    @classmethod
    def create_default_historic_2023(cls) -> "Section58":
        """Create a default Section 58 configuration for 2023."""
        return cls(
            processor_type=DataProcessorType.MOTHERDUCK,
            time_range=TimeRange.HISTORIC,
            batch_limit=20000,
            year=2023,
            start_month=1,
            end_month=13,
        )

    @classmethod
    def create_default_historic_2022(cls) -> "Section58":
        """Create a default Section 58 configuration for 2022."""
        return cls(
            processor_type=DataProcessorType.MOTHERDUCK,
            time_range=TimeRange.HISTORIC,
            batch_limit=20000,
            year=2022,
            start_month=3,
            end_month=13,
        )
