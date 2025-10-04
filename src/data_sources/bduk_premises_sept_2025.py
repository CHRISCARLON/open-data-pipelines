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


class BDUKPremises(DataSourceConfig):
    """
    Configuration class for BDUK Premises data source.
    Implements the DataSourceConfig Protocol.
    """

    def __init__(
        self,
        processor_type: DataProcessorType,
        time_range: TimeRange,
        batch_limit: Optional[int] = None,
    ):
        """
        Initialise BDUK configuration.

        Args:
            processor_type: The type of data processor to use
            time_range: The time range for the data
            batch_limit: Optional limit for batch processing
            source_type: The type of data source
        """

        self._processor_type = processor_type
        self._time_range = time_range
        self.batch_limit = batch_limit
        self._source_type = DataSourceType.BDUK_PREMISES_SEPT_2025

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
        """
        Get the download links for BDUK premises data for each region.
        Returns a list of urls.
        """
        try:
            response = requests.get(self.base_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")
            region_links = {}
            # Find sections that are attachments (filter for .zip)
            for section in soup.find_all("section", class_="gem-c-attachment"):
                if not isinstance(section, Tag):
                    continue

                link = section.find("a", href=True)
                if not isinstance(link, Tag):
                    continue

                title_el = section.find("h3", class_="gem-c-attachment__title")

                href_attr = str(link.get("href", ""))
                if href_attr and ".zip" in href_attr:
                    url = href_attr
                    if url.startswith("/"):
                        url = f"https://www.gov.uk{url}"
                    if url.startswith("https://assets.publishing.service.gov.uk"):
                        pass
                    elif str(link.get("href", "")).startswith(
                        "https://assets.publishing.service.gov.uk"
                    ):
                        url = str(link.get("href", ""))
                    # Extract region name from title
                    region = title_el.get_text(strip=True) if title_el else url
                    region_links[url] = region

            if not region_links:
                raise ValueError("No valid download link found on the page")
            return [url for url in region_links]
        except Exception as e:
            logger.error(f"Error in get_link: {e}")
            raise ValueError(f"Failed to get download link: {e}")

    @property
    def schema_name(self) -> str:
        """Get the schema name for the configured data source."""
        return "bduk_premises"

    @property
    def table_names(self) -> list[str]:
        """Get the table names for the configured data source."""
        # Extract YYYYMM from base_url (e.g., "may-2025" -> "202505")
        import re
        url_parts = self.base_url.split("/")
        month_year = next((p for p in url_parts if re.match(r'[a-z]+-\d{4}', p)), None)

        if month_year:
            parts = month_year.split("-")
            month_str, year = parts[0], parts[1]
            month_map = {
                "january": "01", "february": "02", "march": "03", "april": "04",
                "may": "05", "june": "06", "july": "07", "august": "08",
                "september": "09", "october": "10", "november": "11", "december": "12"
            }
            date_prefix = f"{year}{month_map.get(month_str.lower(), '00')}_"
        else:
            date_prefix = ""

        return [
            f"{date_prefix}BDUK_uprn_release_{url.split('/')[-1].replace('.zip', '').replace('_', ' ').title().replace(' ', '_')}"
            for url in self.download_links
        ]

    @property
    def db_template(self) -> dict:
        return {
            "uprn": "BIGINT",
            "struprn": "VARCHAR",
            "bduk_recognised_premises": "BOOLEAN",
            "country": "VARCHAR",
            "postcode": "VARCHAR",
            "lot_id": "BIGINT",
            "lot_name": "VARCHAR",
            "subsidy_control_status": "VARCHAR",
            "current_gigabit": "BOOLEAN",
            "future_gigabit": "BOOLEAN",
            "local_authority_district_ons_code": "VARCHAR",
            "local_authority_district_ons": "VARCHAR",
            "region_ons_code": "VARCHAR",
            "region_ons": "VARCHAR",
            "bduk_gis": "BOOLEAN",
            "bduk_gis_contract_scope": "VARCHAR",
            "bduk_gis_final_coverage_date": "VARCHAR",
            "bduk_gis_contract_name": "VARCHAR",
            "bduk_gis_supplier": "VARCHAR",
            "bduk_vouchers": "BOOLEAN",
            "bduk_vouchers_contract_name": "VARCHAR",
            "bduk_vouchers_supplier": "VARCHAR",
            "bduk_superfast": "BOOLEAN",
            "bduk_superfast_contract_name": "VARCHAR",
            "bduk_superfast_supplier": "VARCHAR",
            "bduk_hubs": "BOOLEAN",
            "bduk_hubs_contract_name": "VARCHAR",
            "bduk_hubs_supplier": "VARCHAR",
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
        return (
            f"BDUK Premises Configuration: "
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
    def create_default_latest(cls) -> "BDUKPremises":
        return cls(
            processor_type=DataProcessorType.MOTHERDUCK,
            time_range=TimeRange.LATEST,
            batch_limit=200000,
        )


if __name__ == "__main__":
    config = BDUKPremises.create_default_latest()
    print(config.table_names)
