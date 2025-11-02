from typing import Optional, List, Dict, Any
import requests
from .data_source_config import (
    DataProcessorType,
    DataSourceType,
    TimeRange,
    DataSourceConfig,
)


class NHSEnglishPrescriptions(DataSourceConfig):
    """
    Configuration class for NHS English Prescribing data source.
    Implements the DataSourceConfigProtocol.
    """

    def __init__(
        self,
        processor_type: DataProcessorType,
        time_range: TimeRange,
        batch_limit: Optional[int] = None,
        max_months: Optional[int] = None,
        start_month: Optional[str] = None,
        end_month: Optional[str] = None,
    ):
        """
        Initialise NHS English Prescribing configuration.

        Args:
            processor_type: The type of data processor to use
            time_range: The time range for the data (LATEST for most recent, HISTORIC for all)
            batch_limit: Optional limit for batch processing
            max_months: Optional limit on number of months to process (for HISTORIC)
            start_month: Optional start month in YYYYMM format (e.g., "202402")
            end_month: Optional end month in YYYYMM format (e.g., "202407")
        """
        self._processor_type = processor_type
        self._time_range = time_range
        self.batch_limit = batch_limit
        self.max_months = max_months
        self.start_month = start_month
        self.end_month = end_month
        self._source_type = DataSourceType.NHS_ENGLISH_PRESCRIBING_DATA
        self._resources_cache: Optional[List[Dict[str, Any]]] = None

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

    def _fetch_api_resources(self) -> List[Dict[str, Any]]:
        """
        Fetch available resources from the NHS BSA API.

        Returns:
            List of resource dictionaries containing download information
        """
        if self._resources_cache is not None:
            return self._resources_cache

        try:
            response = requests.get(self.base_url, timeout=30)
            response.raise_for_status()
            data = response.json()

            if not data.get("success", False):
                raise ValueError(f"API returned success=false: {data}")

            resources = data.get("result", {}).get("resources", [])

            filtered_resources = [
                r
                for r in resources
                if r.get("format") == "CSV"
                and r.get("name", "").startswith("EPD_SNOMED_")
            ]

            filtered_resources.sort(key=lambda x: x.get("name", ""), reverse=True)

            self._resources_cache = filtered_resources
            return filtered_resources

        except requests.RequestException as e:
            raise RuntimeError(f"Failed to fetch NHS prescribing data resources: {e}")
        except (KeyError, ValueError) as e:
            raise RuntimeError(
                f"Failed to parse NHS prescribing data API response: {e}"
            )

    @property
    def download_links(self) -> list[str]:
        """
        Get the download links for NHS English Prescribing data.

        Returns:
            List of download URLs based on time_range setting:
            - LATEST: Only the most recent dataset
            - HISTORIC: All available datasets (or limited by max_months)
            - If start_month and end_month are set, filters to that date range
        """
        resources = self._fetch_api_resources()

        if not resources:
            raise ValueError("No NHS prescribing data resources available from API")

        # Filter by date range if specified
        if self.start_month and self.end_month:
            filtered_resources = []
            for r in resources:
                name = r.get("name", "")
                if name.startswith("EPD_SNOMED_") and len(name) >= 17:
                    date_str = name[11:17]  # Extract YYYYMM
                    if len(date_str) == 6 and date_str.isdigit():
                        if self.start_month <= date_str <= self.end_month:
                            filtered_resources.append(r)
            selected_resources = filtered_resources
        elif self.time_range == TimeRange.LATEST:
            selected_resources = resources[:1]
        else:  # HISTORIC
            if self.max_months:
                selected_resources = resources[: self.max_months]
            else:
                selected_resources = resources

        urls: list[str] = []
        for r in selected_resources:
            url = r.get("url")
            if url is not None:
                urls.append(url)

        if not urls:
            raise ValueError("No valid download URLs found in API resources")

        return urls

    def get_all_download_links(self) -> list[str]:
        """
        Get all available download links regardless of time_range or max_months settings.

        Returns:
            List of all available download URLs from the API
        """
        resources = self._fetch_api_resources()

        if not resources:
            raise ValueError("No NHS prescribing data resources available from API")

        # Extract all URLs without filtering
        urls: list[str] = []
        for r in resources:
            url = r.get("url")
            if url is not None:
                urls.append(url)

        if not urls:
            raise ValueError("No valid download URLs found in API resources")

        return urls

    def get_all_resources(self) -> List[Dict[str, Any]]:
        """
        Get all available resources from the API.

        Returns:
            List of all resource dictionaries with metadata
        """
        return self._fetch_api_resources()

    def get_all_table_names(self) -> list[str]:
        """
        Get all available table names regardless of time_range or max_months settings.

        Returns:
            List of all table names that would be generated from available resources
        """
        resources = self._fetch_api_resources()

        if not resources:
            raise ValueError("No NHS prescribing data resources available from API")

        table_names = []
        for resource in resources:
            name = resource.get("name", "")
            # Extract date from name (e.g., EPD_SNOMED_202503 -> 202503)
            if name.startswith("EPD_SNOMED_") and len(name) >= 17:
                date_str = name[11:17]  # Extract YYYYMM
                if len(date_str) == 6 and date_str.isdigit():
                    year = date_str[:4]
                    month = date_str[4:6]
                    table_name = f"nhs_prescriptions_{month}_{year}"
                    table_names.append(table_name)

        if not table_names:
            raise ValueError("Could not generate table names from API resources")

        return table_names

    @property
    def table_names(self) -> List[str]:
        """
        Get the table names for NHS prescriptions data.

        Returns:
            List of table names generated from resource dates.
            Format: nhs_prescriptions_MM_YYYY
            If start_month and end_month are set, filters to that date range
        """
        resources = self._fetch_api_resources()

        if not resources:
            raise ValueError("No NHS prescribing data resources available from API")

        # Filter by date range if specified
        if self.start_month and self.end_month:
            filtered_resources = []
            for r in resources:
                name = r.get("name", "")
                if name.startswith("EPD_SNOMED_") and len(name) >= 17:
                    date_str = name[11:17]  # Extract YYYYMM
                    if len(date_str) == 6 and date_str.isdigit():
                        if self.start_month <= date_str <= self.end_month:
                            filtered_resources.append(r)
            selected_resources = filtered_resources
        elif self.time_range == TimeRange.LATEST:
            selected_resources = resources[:1]
        else:  # HISTORIC
            if self.max_months:
                selected_resources = resources[: self.max_months]
            else:
                selected_resources = resources

        table_names = []
        for resource in selected_resources:
            name = resource.get("name", "")
            # Extract date from name (e.g., EPD_SNOMED_202503 -> 202503)
            if name.startswith("EPD_SNOMED_") and len(name) >= 17:
                date_str = name[11:17]  # Extract YYYYMM
                if len(date_str) == 6 and date_str.isdigit():
                    year = date_str[:4]
                    month = date_str[4:6]
                    table_name = f"nhs_prescriptions_{month}_{year}"
                    table_names.append(table_name)

        if not table_names:
            raise ValueError("Could not generate table names from API resources")

        return table_names

    @property
    def schema_name(self) -> str:
        """
        Get the schema name for the NHS English Prescribing data.

        Returns:
            Fixed schema name: 'nhs_prescribing_raw_data'
            All data goes into this single schema, partitioned by table name (month)
        """
        return "nhs_prescribing_raw_data"

    @property
    def db_template(self) -> dict:
        """
        Database template for NHS English Prescribing data.
        Based on the expected CSV structure.
        Updated to match NHS schema as of February 2025.
        """
        return {
            "YEAR_MONTH": "VARCHAR",
            "REGIONAL_OFFICE_NAME": "VARCHAR",
            "REGIONAL_OFFICE_CODE": "VARCHAR",
            "ICB_NAME": "VARCHAR",
            "ICB_CODE": "VARCHAR",
            "PCO_NAME": "VARCHAR",
            "PCO_CODE": "VARCHAR",
            "PRACTICE_NAME": "VARCHAR",
            "PRACTICE_CODE": "VARCHAR",
            "ADDRESS_1": "VARCHAR",
            "ADDRESS_2": "VARCHAR",
            "ADDRESS_3": "VARCHAR",
            "ADDRESS_4": "VARCHAR",
            "POSTCODE": "VARCHAR",
            "BNF_CHEMICAL_SUBSTANCE": "VARCHAR",
            "CHEMICAL_SUBSTANCE_BNF_DESCR": "VARCHAR",
            "BNF_CODE": "VARCHAR",
            "BNF_DESCRIPTION": "VARCHAR",
            "BNF_CHAPTER_PLUS_CODE": "VARCHAR",
            "QUANTITY": "DOUBLE",
            "ITEMS": "BIGINT",
            "TOTAL_QUANTITY": "DOUBLE",
            "ADQUSAGE": "DOUBLE",
            "NIC": "DOUBLE",
            "ACTUAL_COST": "DOUBLE",
            "UNIDENTIFIED": "VARCHAR",
            "SNOMED_CODE": "BIGINT",
        }

    def get_table_template(self, table_name: str) -> dict:
        """
        Get the database template for a specific table.

        Args:
            table_name: The name of the table

        Returns:
            Database template dictionary (same for all NHS prescription tables)
        """
        return self.db_template

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
            f"NHSEnglishPrescriptionsConfig(processor={self.processor_type.value}, "
            f"source={self.source_type.code}, "
            f"time_range={self.time_range.value}, "
            f"batch_limit={self.batch_limit}, "
            f"download_links={self.download_links}, "
            f"schema_name={self.schema_name}, "
            f"table_names={self.table_names})"
        )

    @classmethod
    def create_default(cls) -> "NHSEnglishPrescriptions":
        """
        Create a default NHS English Prescribing configuration.
        Returns configuration for the LATEST dataset only.
        """
        return cls(
            processor_type=DataProcessorType.MOTHERDUCK,
            time_range=TimeRange.LATEST,
            batch_limit=300000,
        )

    @classmethod
    def create_all_months(
        cls,
        processor_type: DataProcessorType = DataProcessorType.MOTHERDUCK,
        batch_limit: Optional[int] = 300000,
    ) -> "NHSEnglishPrescriptions":
        """
        Create configuration for ALL available NHS prescribing data months.

        Args:
            processor_type: The type of data processor to use (default: MOTHERDUCK)
            batch_limit: Optional limit for batch processing (default: 300000)

        Returns:
            NHSEnglishPrescriptions configured for all available historic data
        """
        return cls(
            processor_type=processor_type,
            time_range=TimeRange.HISTORIC,
            batch_limit=batch_limit,
            max_months=None,
        )

    @classmethod
    def create_last_n_months(
        cls,
        n_months: int,
        processor_type: DataProcessorType = DataProcessorType.MOTHERDUCK,
        batch_limit: Optional[int] = 300000,
    ) -> "NHSEnglishPrescriptions":
        """
        Create configuration for the last N months of NHS prescribing data.

        Args:
            n_months: Number of most recent months to include
            processor_type: The type of data processor to use (default: MOTHERDUCK)
            batch_limit: Optional limit for batch processing (default: 300000)

        Returns:
            NHSEnglishPrescriptions configured for the specified number of months

        Examples:
            # Get last 6 months
            config = NHSEnglishPrescriptions.create_last_n_months(6)

            # Get last 12 months
            config = NHSEnglishPrescriptions.create_last_n_months(12)
        """
        if n_months <= 0:
            raise ValueError(f"n_months must be positive, got {n_months}")

        return cls(
            processor_type=processor_type,
            time_range=TimeRange.HISTORIC,
            batch_limit=batch_limit,
            max_months=n_months,
        )

    @classmethod
    def create_date_range(
        cls,
        start_month: str,
        end_month: str,
        processor_type: DataProcessorType = DataProcessorType.MOTHERDUCK,
        batch_limit: Optional[int] = 300000,
    ) -> "NHSEnglishPrescriptions":
        """
        Create configuration for a specific date range of NHS prescribing data.

        Args:
            start_month: Start month in YYYYMM format (e.g., "202402" for Feb 2024)
            end_month: End month in YYYYMM format (e.g., "202407" for Jul 2024)
            processor_type: The type of data processor to use (default: MOTHERDUCK)
            batch_limit: Optional limit for batch processing (default: 300000)

        Returns:
            NHSEnglishPrescriptions configured for the specified date range

        Examples:
            # Get data from February 2024 to July 2024
            config = NHSEnglishPrescriptions.create_date_range("202402", "202407")

            # Get data for a single month
            config = NHSEnglishPrescriptions.create_date_range("202406", "202406")
        """
        from datetime import datetime

        # Validate date format
        try:
            start_date = datetime.strptime(start_month, "%Y%m")
            end_date = datetime.strptime(end_month, "%Y%m")
        except ValueError as e:
            raise ValueError(f"Invalid date format. Use YYYYMM (e.g., 202402): {e}")

        if start_date > end_date:
            raise ValueError(
                f"Start month {start_month} is after end month {end_month}"
            )

        return cls(
            processor_type=processor_type,
            time_range=TimeRange.HISTORIC,
            batch_limit=batch_limit,
            max_months=None,
            start_month=start_month,
            end_month=end_month,
        )


if __name__ == "__main__":
    print("=" * 80)
    print("Testing NHS English Prescriptions Configuration")
    print("=" * 80)

    # Test 1: Latest configuration
    print("\n1. LATEST Configuration:")
    config_latest = NHSEnglishPrescriptions.create_date_range('202408', '202502')
    print(f"   Schema: {config_latest.schema_name}")
    print(f"   Metadata Schema: {config_latest.metadata_schema_name}")
    print(f"   Number of tables: {len(config_latest.table_names)}")
    print(f"   Table names: {config_latest.table_names}")
    print(f"   Number of download links: {len(config_latest.download_links)}")
    print(config_latest.download_links)
