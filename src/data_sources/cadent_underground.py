from typing import Optional
from .data_source_config import (
    DataProcessorType,
    DataSourceType,
    TimeRange,
    DataSourceConfig,
)
from loguru import logger
import os


class CadentUndergroundPipes(DataSourceConfig):
    """
    Configuration class for Cadent Underground Pipes data source.
    Implements the DataSourceConfig Protocol.
    """

    def __init__(
        self,
        processor_type: DataProcessorType,
        time_range: TimeRange,
        batch_limit: Optional[int] = None,
    ):
        """
        Initialise Cadent Underground Pipes configuration.

        Args:
            processor_type: The type of data processor to use
            time_range: The time range for the data
            batch_limit: Optional limit for batch processing
        """

        self._processor_type = processor_type
        self._time_range = time_range
        self.batch_limit = batch_limit
        self._source_type = DataSourceType.CADENT_GAS

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
        Get the download links for Cadent underground pipes data.
        Returns the direct API endpoint for the parquet data with API key.
        """
        # Get API key from environment variable
        api_key = os.getenv('CADENT')
        
        if not api_key:
            logger.warning("CADENT_API_KEY environment variable not found")
            # Return base URL without API key as fallback
            raise ValueError("CADENT_API_KEY environment variable not found")
        
        # Add API key as query parameter
        base_url = self.base_url
        separator = "&" if "?" in base_url else "?"
        url_with_key = f"{base_url}{separator}apikey={api_key}"
        
        logger.info("Using Cadent API with API key")
        return [url_with_key]

    @property
    def schema_name(self) -> str:
        """Get the schema name for the configured data source."""
        return "cadent_underground_pipes"

    @property
    def table_names(self) -> list[str]:
        """Get the table names for the configured data source."""
        return ["cadent_underground_pipes"]

    @property
    def db_template(self) -> dict:
        """Database schema template for Cadent underground pipes data."""
        return {
            "unique_id": "VARCHAR",
            "geo_point": "VARCHAR",
            "geo_shape": "VARCHAR", 
            "pipe_material": "VARCHAR",
            "pipe_orientation": "VARCHAR",
            "pipe_diameter": "VARCHAR",
            "pressure_tier": "VARCHAR",
            "main_or_service": "VARCHAR",
            "shape_length": "VARCHAR",
        }

    def __str__(self) -> str:
        return (
            f"Cadent Underground Pipes Configuration: "
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
    def create_default_latest(cls) -> "CadentUndergroundPipes":
        return cls(
            processor_type=DataProcessorType.MOTHERDUCK,
            time_range=TimeRange.LATEST,
            batch_limit=150000,
        )


if __name__ == "__main__":
    config = CadentUndergroundPipes.create_default_latest()
    print(config)
