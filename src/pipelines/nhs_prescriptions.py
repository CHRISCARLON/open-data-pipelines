import os

from ..databases.motherduck import MotherDuckManager
from ..data_sources.nhs_english_prescriptions import NHSEnglishPrescriptions
from ..data_processors.utils.metadata_logger import ensure_metadata_schema_exists
from ..data_processors.nhs_english_prescriptions import process_nhs_prescriptions
from loguru import logger


def main():
    """
    Pipeline to set up NHS English Prescriptions database schema and process CSV data.
    This creates the prescribing schema, tables, and loads the aggregated data.
    """
    if not (token := os.getenv("MOTHERDUCK_TOKEN")) or not (
        database := os.getenv("MOTHERDB_2")
    ):
        raise ValueError("MOTHERDUCK_TOKEN and MOTHERDB must be set")

    config = NHSEnglishPrescriptions.create_default()

    with MotherDuckManager(token, database) as db_manager:
        logger.info("Setting up NHS English Prescriptions database schema...")

        db_manager.setup_for_data_source(config)

        ensure_metadata_schema_exists(config, db_manager)

        logger.success("NHS English Prescriptions schema setup complete!")
        logger.info(f"Created schema: {config.schema_name}")
        logger.info(f"Created tables: {', '.join(config.table_names)}")

        logger.info("Starting NHS English Prescriptions data processing...")

        process_nhs_prescriptions(
            download_links=config.download_links,
            table_names=config.table_names,
            batch_size=config.batch_limit or 200000,
            conn=db_manager.connection,
            schema_name=config.schema_name,
            expected_columns=config.db_template,
            create_aggregated=True,
            drop_staging=True,
            config=config,
        )

        logger.success("NHS English Prescriptions pipeline completed successfully!")


if __name__ == "__main__":
    main()
