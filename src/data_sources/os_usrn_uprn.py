from typing import Optional, List
from .data_source_config import (
    DataProcessorType,
    DataSourceType,
    TimeRange,
    DataSourceConfig,
)
from datetime import datetime
import requests
from datetime import datetime, timedelta


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
        Constructs download URL with fallback strategy for USRN-UPRN data.
        
        Returns:
            list[str]: List containing the download URL for USRN-UPRN data
        """
        # Try different months, starting from current and going backwards
        now = datetime.now()
        
        # List of months to try (current, then previous months)
        months_to_try = []
        for i in range(6):  # Try current month and 5 months back
            test_date = now - timedelta(days=30 * i)
            months_to_try.append(f"{test_date.year}-{test_date.month:02d}")
        
        for i in range(1, 4): 
            test_date = now + timedelta(days=30 * i)
            months_to_try.append(f"{test_date.year}-{test_date.month:02d}")
        
        for date_format in months_to_try:
            file_name = f"lids-{date_format}_csv_BLPU-UPRN-Street-USRN-11.zip"
            test_url = f"{self.base_url}?area=GB&format=CSV&fileName={file_name}&redirect"
            
            try:
                response = requests.head(test_url, timeout=10)
                if response.status_code == 200:
                    return [test_url]
            except requests.RequestException as e:
                continue
        
        raise ValueError(
            f"No valid USRN-UPRN download URL found. Tried months: {months_to_try}. "
            "The OS API may have changed or no recent data is available."
        )

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
