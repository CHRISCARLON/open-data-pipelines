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
        """Create a default NHS English Prescribing configuration."""
        return cls(
            processor_type=DataProcessorType.MOTHERDUCK,
            time_range=TimeRange.LATEST,
            batch_limit=300000,
        )


if __name__ == "__main__":
    config = NHSEnglishPrescriptions.create_default()
    print(config)
