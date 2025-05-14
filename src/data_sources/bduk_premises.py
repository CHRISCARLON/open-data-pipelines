from typing import Optional
from .data_source_config import (
    DataProcessorType,
    DataSourceType,
    TimeRange,
    DataSourceConfig,
)
import requests
from bs4 import BeautifulSoup
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
        self._source_type = DataSourceType.BDUK_PREMISES

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
                link = section.find("a", href=True)
                title_el = section.find("h3", class_="gem-c-attachment__title")
                if link and ".zip" in link["href"]:
                    url = link["href"]
                    if url.startswith("/"):
                        url = f"https://www.gov.uk{url}"
                    if url.startswith("https://assets.publishing.service.gov.uk"):
                        pass
                    elif link["href"].startswith(
                        "https://assets.publishing.service.gov.uk"
                    ):
                        url = link["href"]
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
        return [url.split("/")[-1].replace(".zip", "") for url in self.download_links]

    @property
    def db_template(self) -> dict:
        return {
            "UPRN": "VARCHAR",
            "struprn": "VARCHAR",
            "bduk_recognised_premises": "VARCHAR",
            "Country": "VARCHAR",
            "postcode": "VARCHAR",
            "lot_id": "VARCHAR",
            "lot_name": "VARCHAR",
            "subsidy_control_status": "VARCHAR",
            "current_gigabit": "VARCHAR",
            "future_gigabit": "VARCHAR",
            "local_authority_district_ons_code": "VARCHAR",
            "local_authority_district_ons": "VARCHAR",
            "region_ons_code": "VARCHAR",
            "region_ons": "VARCHAR",
            "bduk_gis": "VARCHAR",
            "bduk_gis_contract_scope": "VARCHAR",
            "gis_final_coverage_date": "VARCHAR",
            "bduk_vouchers": "VARCHAR",
            "bduk_superfast": "VARCHAR",
            "bduk_hubs": "VARCHAR",
            "supplier": "VARCHAR",
            "contract_name": "VARCHAR",
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
            batch_limit=100000,
        )


if __name__ == "__main__":
    config = BDUKPremises.create_default_latest()
    print(config)
