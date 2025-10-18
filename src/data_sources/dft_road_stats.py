import requests
from typing import Optional, List
from bs4 import BeautifulSoup, Tag
from loguru import logger
from .data_source_config import (
    DataProcessorType,
    DataSourceType,
    TimeRange,
    DataSourceConfig,
)


class DftRoadStats(DataSourceConfig):
    """
    Configuration class for DFT Road Stats data.
    Implements the DataSourceConfigProtocol.
    """

    def __init__(
        self,
        processor_type: DataProcessorType,
        time_range: TimeRange,
        batch_limit: Optional[int] = None,
    ):
        """
        Initialise a DFT Road Stats configuration.

        Args:
            processor_type: The type of data processor to use
            time_range: The time range for the data
            batch_limit: Optional limit for batch processing
        """
        self._processor_type = processor_type
        self._time_range = time_range
        self.batch_limit = batch_limit
        self._source_type = DataSourceType.DftRoadStats
        self._cached_download_links = None

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
    def download_links(self) -> dict:
        """
        Get the download links for DFT Road Stats data.
        Returns a dictionary with file codes as keys and URLs as values.

        For LATEST: Scrapes for rdl0101, rdl0102, rdl0103, rdl0201, rdl0202, rdl0203
        For HISTORIC: Returns historic ZIP files (2023, 2022, 2021, pre-2021-tables)
        """
        # Return cached result if available
        if self._cached_download_links is not None:
            return self._cached_download_links

        # Handle historic data with fixed ZIP file links
        if self.time_range == TimeRange.HISTORIC:
            historic_links = {
                "2023": "https://assets.publishing.service.gov.uk/media/67b5abf94e79a175a4c2feb5/2023.zip",
                "2022": "https://assets.publishing.service.gov.uk/media/67b5ac103e77ca8b737d3878/2022.zip",
                "2021": "https://assets.publishing.service.gov.uk/media/67b5ac24423f67c0e67d387a/2021.zip",
                "pre_2021": "https://assets.publishing.service.gov.uk/media/67b5adad423f67c0e67d387b/pre-2021-tables.zip"
            }
            logger.success(f"Using historic RDL download links ({len(historic_links)} files)")
            self._cached_download_links = historic_links
            return self._cached_download_links

        # Handle latest data by scraping the website
        target_files = ["rdl0101", "rdl0102", "rdl0103", "rdl0201", "rdl0202", "rdl0203"]

        try:
            response = requests.get(self.base_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")

            download_links = {}
            # Find all links that contain .ods files
            for link in soup.find_all("a", class_="govuk-link", href=True):
                if not isinstance(link, Tag):
                    continue

                href = link.get("href")
                if not href or not isinstance(href, str):
                    continue

                if ".ods" in href:
                    # Extract the filename from the URL
                    filename = href.split("/")[-1].replace(".ods", "").lower()

                    # Check if this is one of our target files
                    if filename in target_files:
                        title = link.get_text(strip=True)
                        download_links[filename] = href
                        logger.info(f"Found {filename}: {title}")

            if not download_links:
                logger.warning("No RDL files found on the page")
                self._cached_download_links = {}
                return self._cached_download_links

            logger.success(f"Found {len(download_links)} RDL download links")
            self._cached_download_links = download_links
            return self._cached_download_links

        except Exception as e:
            logger.error(f"Error scraping DFT Road Stats links: {e}")
            self._cached_download_links = {}
            return self._cached_download_links

    @property
    def table_names(self) -> List[str]:
        """
        Get all table names for DFT Road Stats files.
        Returns a list of file codes (rdl0101, rdl0102, etc.).
        """
        if isinstance(self.download_links, dict):
            return list(self.download_links.keys())
        return ["dft_road_stats"]

    @property
    def schema_name(self) -> str:
        """
        Get the schema name for the DFT Road Stats data.
        """
        return "dft_road_stats"

    @property
    def db_template(self) -> dict:
        """
        Database template for DFT Road Stats data.
        Returns a generic template - not used for actual table creation.
        Use get_table_template() instead for specific file types.
        """
        return self.get_region_template()

    def get_region_template(self) -> dict:
        """
        Template for region-level data (RDL0101, RDL0103, RDL0201, RDL0203).
        These files have region but no local_authority column.
        """
        return {
            "ons_area_code": "VARCHAR",
            "region": "VARCHAR",
            "centrally_managed_motorways": "VARCHAR",
            "locally_managed_motorways": "VARCHAR",
            "all_motorways": "VARCHAR",
            "centrally_managed_rural_a_roads": "VARCHAR",
            "centrally_managed_urban_a_roads": "VARCHAR",
            "locally_managed_rural_a_roads": "VARCHAR",
            "locally_managed_urban_a_roads": "VARCHAR",
            "all_a_roads": "VARCHAR",
            "major_centrally_managed_roads": "VARCHAR",
            "major_locally_managed_roads": "VARCHAR",
            "all_major_roads": "VARCHAR",
            "rural_b_roads": "VARCHAR",
            "urban_b_roads": "VARCHAR",
            "rural_c_and_u_roads": "VARCHAR",
            "urban_c_and_u_roads": "VARCHAR",
            "all_minor_roads": "VARCHAR",
            "total_road_length": "VARCHAR",
            "notes": "VARCHAR",
        }

    def get_local_authority_template(self) -> dict:
        """
        Template for local authority-level data (RDL0102, RDL0202).
        These files have both region AND local_authority columns.
        """
        return {
            "ons_area_code": "VARCHAR",
            "region": "VARCHAR",
            "local_authority": "VARCHAR",
            "centrally_managed_motorways": "VARCHAR",
            "locally_managed_motorways": "VARCHAR",
            "all_motorways": "VARCHAR",
            "centrally_managed_rural_a_roads": "VARCHAR",
            "centrally_managed_urban_a_roads": "VARCHAR",
            "locally_managed_rural_a_roads": "VARCHAR",
            "locally_managed_urban_a_roads": "VARCHAR",
            "all_a_roads": "VARCHAR",
            "major_centrally_managed_roads": "VARCHAR",
            "major_locally_managed_roads": "VARCHAR",
            "all_major_roads": "VARCHAR",
            "rural_b_roads": "VARCHAR",
            "urban_b_roads": "VARCHAR",
            "rural_c_and_u_roads": "VARCHAR",
            "urban_c_and_u_roads": "VARCHAR",
            "all_minor_roads": "VARCHAR",
            "total_road_length": "VARCHAR",
            "notes": "VARCHAR",
        }

    def get_table_template(self, table_name: str) -> dict:
        """
        Get the appropriate template for a specific table.

        Args:
            table_name: The table name (e.g., 'rdl0101', 'rdl0202')

        Returns:
            The appropriate database template
        """
        if table_name.lower() in ['rdl0102', 'rdl0202']:
            return self.get_local_authority_template()
        else:
            return self.get_region_template()

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
        if isinstance(self.download_links, dict):
            links_list = list(self.download_links.keys())
            links_str = ", ".join(links_list[:3])
            if len(links_list) > 3:
                links_str += f", ... ({len(links_list)} total)"
        else:
            links_str = str(self.download_links)

        return (
            f"DftRoadStatsConfig(processor={self.processor_type.value}, "
            f"source={self.source_type.code}, "
            f"base_url={self.base_url}, "
            f"time_range={self.time_range.value}, "
            f"batch_limit={self.batch_limit}, "
            f"download_links=[{links_str}]), "
            f"schema_name={self.schema_name}, "
            f"table_names={self.table_names}"
        )

    @classmethod
    def create_default_latest(cls) -> "DftRoadStats":
        """Create a default DFT Road Stats configuration for latest data."""
        return cls(
            processor_type=DataProcessorType.MOTHERDUCK,
            time_range=TimeRange.LATEST,
            batch_limit=250000,
        )

    @classmethod
    def create_default_historic(cls) -> "DftRoadStats":
        """Create a default DFT Road Stats configuration for historic data."""
        return cls(
            processor_type=DataProcessorType.MOTHERDUCK,
            time_range=TimeRange.HISTORIC,
            batch_limit=250000,
        )


if __name__ == "__main__":
    print("=== LATEST DATA ===")
    config_latest = DftRoadStats.create_default_latest()
    print(config_latest)
    print(f"\nTable names: {config_latest.table_names}")
    print(f"Download links:")
    for name, url in config_latest.download_links.items():
        print(f"  {name}: {url}")

    print("\n=== HISTORIC DATA ===")
    config_historic = DftRoadStats.create_default_historic()
    print(config_historic)
    print(f"\nTable names: {config_historic.table_names}")
    print(f"Download links:")
    for name, url in config_historic.download_links.items():
        print(f"  {name}: {url}")
