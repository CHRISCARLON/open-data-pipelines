from typing import Optional, List
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
    ):
        """
        Initialise NHS English Prescribing configuration.

        Args:
            processor_type: The type of data processor to use
            time_range: The time range for the data (always uses 202503 data)
            batch_limit: Optional limit for batch processing
        """
        self._processor_type = processor_type
        self._time_range = time_range
        self.batch_limit = batch_limit
        self._source_type = DataSourceType.NHS_ENGLISH_PRESCRIBING_DATA

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
        Get the download links for NHS English Prescribing data.
        Always returns the 202503 dataset.

        This needs changed to handle all periods!!
        """
        # TODO: Change this to handle all periods!!
        # Always use 202503 data for now
        url = self.source_type.base_url.format(date="202503")
        return [url]

    @property
    def table_names(self) -> List[str]:
        """Get the table name for NHS prescriptions data."""
        return ["nhs_prescriptions_03_2025"]

    @property
    def schema_name(self) -> str:
        """Get the schema name for the NHS English Prescribing data."""
        return "prescribing_raw_data_2025"

    @property
    def db_template(self) -> dict:
        """
        Database template for NHS English Prescribing data.
        Based on the expected CSV structure.
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
            "BNF_CHEMICAL_SUBSTANCE_CODE": "VARCHAR",
            "BNF_CHEMICAL_SUBSTANCE": "VARCHAR",
            "BNF_PRESENTATION_CODE": "VARCHAR",
            "BNF_PRESENTATION_NAME": "VARCHAR",
            "BNF_CHAPTER_PLUS_CODE": "VARCHAR",
            "QUANTITY": "DOUBLE",
            "ITEMS": "BIGINT",
            "TOTAL_QUANTITY": "DOUBLE",
            "ADQ_USAGE": "DOUBLE",
            "NIC": "DOUBLE",
            "ACTUAL_COST": "DOUBLE",
            "UNIDENTIFIED": "VARCHAR",
            "SNOMED_CODE": "BIGINT",
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
        """Create a default NHS English Prescribing configuration."""
        return cls(
            processor_type=DataProcessorType.MOTHERDUCK,
            time_range=TimeRange.LATEST,
            batch_limit=200000,
        )


if __name__ == "__main__":
    config = NHSEnglishPrescriptions.create_default()
    print(config)
