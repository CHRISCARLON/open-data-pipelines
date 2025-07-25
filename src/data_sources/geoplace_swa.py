from typing import Optional
from .data_source_config import (
    DataProcessorType,
    DataSourceType,
    TimeRange,
    DataSourceConfig,
)
import requests
from bs4 import BeautifulSoup, Tag
from loguru import logger


class GeoplaceSwa(DataSourceConfig):
    """
    Configuration class for Geoplace SWA data source.
    Implements the DataSourceConfigProtocol.
    """

    def __init__(
        self,
        processor_type: DataProcessorType,
        time_range: TimeRange,
        batch_limit: Optional[int] = None,
    ):
        """
        Initialise a Geoplace SWA configuration.

        Args:
            processor_type: The type of data processor to use
            time_range: The time range for the data
            batch_limit: Optional limit for batch processing
            source_type: The type of data source
        """

        self._processor_type = processor_type
        self._time_range = time_range
        self.batch_limit = batch_limit
        self._source_type = DataSourceType.GEOPLACE_SWA
        self._cached_download_links = None  # Cache for download links

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
        # Return cached result if available
        if self._cached_download_links is not None:
            return self._cached_download_links

        # Otherwise, scrape and cache the result
        try:
            response = requests.get(self.base_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")
            download_link = soup.find("a", class_="download-item__download-link")
            if download_link and isinstance(download_link, Tag):
                href = download_link.get("href")
                logger.success("Download Link Found")
                self._cached_download_links = [str(href)]  # Cache the result
                return self._cached_download_links
            else:
                raise ValueError("No valid download link found on the page")
        except Exception as e:
            logger.error(f"Error in get_link: {e}")
            raise ValueError(f"Failed to get download link: {e}")

    @property
    def schema_name(self) -> str:
        """Get the schema name for the configured data source."""
        return "geoplace_swa_codes"

    @property
    def table_names(self) -> list[str]:
        """Get the table names for the configured data source."""
        return ["LATEST_ACTIVE"]

    @property
    def db_template(self) -> dict:
        return {
            "swa_code": "VARCHAR",
            "account_name": "VARCHAR",
            "prefix": "VARCHAR",
            "account_type": "VARCHAR",
            "registered_for_street_manager": "VARCHAR",
            "account_status": "VARCHAR",
            "companies_house_number": "VARCHAR",
            "previous_company_names": "VARCHAR",
            "linked_parent_company": "VARCHAR",
            "website": "VARCHAR",
            "plant_enquiries": "VARCHAR",
            "ofgem_electricity_licence": "VARCHAR",
            "ofgem_gas_licence": "VARCHAR",
            "ofcom_licence": "VARCHAR",
            "ofwat_licence": "VARCHAR",
            "company_subsumed_by": "VARCHAR",
            "swa_code_of_new_company": "VARCHAR",
            "date_time_processed": "VARCHAR",
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
            f"Geoplace SWA Configuration: "
            f"processor={self.processor_type.value}, "
            f"source={self.source_type.code}, "
            f"base_url={self.base_url}, "
            f"time_range={self.time_range.value}, "
            f"batch_limit={self.batch_limit}, "
            f"download_links={self.download_links}, "
            f"schema_name={self.schema_name}, "
            f"table_names={self.table_names}, "
            f"db_template={self.db_template}"
        )

    @classmethod
    def create_default_latest(cls) -> "GeoplaceSwa":
        return cls(
            processor_type=DataProcessorType.MOTHERDUCK,
            time_range=TimeRange.LATEST,
            batch_limit=150000,
        )

    @classmethod
    def create_postgresql_latest(cls) -> "GeoplaceSwa":
        return cls(
            processor_type=DataProcessorType.POSTGRESQL,
            time_range=TimeRange.LATEST,
            batch_limit=150000,
        )


if __name__ == "__main__":
    config = GeoplaceSwa.create_default_latest()
    config2 = GeoplaceSwa.create_postgresql_latest()
    print(config)
    print(config2)
