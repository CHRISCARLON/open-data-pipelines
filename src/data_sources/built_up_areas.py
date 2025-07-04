import requests
from typing import Optional, List
from .data_source_config import (
    DataProcessorType,
    DataSourceType,
    TimeRange,
    DataSourceConfig,
)


class BuiltUpAreas(DataSourceConfig):
    """
    Configuration class for OS Open Built Up Areas data source.
    Implements the DataSourceConfigProtocol.
    """

    def __init__(
        self,
        processor_type: DataProcessorType,
        time_range: TimeRange,
        batch_limit: Optional[int] = None,
    ):
        """
        Initialise Built Up Areas configuration.

        Args:
            processor_type: The type of data processor to use
            time_range: The time range for the data
            batch_limit: Optional limit for batch processing
        """
        self._processor_type = processor_type
        self._time_range = time_range
        self.batch_limit = batch_limit
        self._source_type = DataSourceType.BUILT_UP_AREAS

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
        response = requests.head(self.base_url, allow_redirects=True)
        return [response.url]

    @property
    def table_names(self) -> List[str]:
        """Get table names for built up areas."""
        return ["os_open_built_up_areas"]

    @property
    def schema_name(self) -> str:
        """Get the schema name for built up areas data."""
        return "built_up_areas"

    @property
    def db_template(self) -> dict:
        """Database schema template for OS Open Built Up Areas data."""
        return {
            "gsscode": "VARCHAR",
            "name1_text": "VARCHAR",
            "name1_language": "VARCHAR",
            "name2_text": "VARCHAR",
            "name2_language": "VARCHAR",
            "areahectares": "VARCHAR",
            "geometry_area_m": "VARCHAR",
            "geometry": "VARCHAR",
        }

    def __str__(self) -> str:
        """String representation of the configuration."""
        links_str = ", ".join(self.download_links[:2])
        if len(self.download_links) > 2:
            links_str += f", ... ({len(self.download_links)} total)"

        return (
            f"BuiltUpAreasConfig(processor={self.processor_type.value}, "
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
    def create_default_latest(cls) -> "BuiltUpAreas":
        """Create a default OS Open Built Up Areas configuration."""
        return cls(
            processor_type=DataProcessorType.MOTHERDUCK,
            time_range=TimeRange.LATEST,
            batch_limit=150000,
        )


if __name__ == "__main__":
    config = BuiltUpAreas.create_default_latest()
    print(config)
