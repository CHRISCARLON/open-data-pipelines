import os

from loguru import logger

from ..data_processors.nhs_english_prescriptions import process_nhs_prescriptions
from ..data_processors.utils.metadata_logger import ensure_metadata_schema_exists
from ..data_sources.nhs_english_prescriptions import NHSEnglishPrescriptions
from ..databases.motherduck import MotherDuckManager


def main():
    """
    Pipeline to process a specific date range of NHS English Prescriptions data.
    """
    if not (token := os.getenv("MOTHERDUCK_TOKEN")) or not (
        database := os.getenv("MOTHERDB_2")
    ):
        raise ValueError("MOTHERDUCK_TOKEN and MOTHERDB_2 must be set")

    START_MONTH = "202508"
    END_MONTH = "202510"

    config = NHSEnglishPrescriptions.create_date_range(START_MONTH, END_MONTH)

    logger.info(f"Processing NHS prescriptions from {START_MONTH} to {END_MONTH}")
    logger.info(f"Download links: {config.download_links}")
    logger.info(f"Schema: {config.schema_name}")
    logger.info(f"Tables to process: {len(config.table_names)}")
    logger.info(f"Table names: {', '.join(config.table_names)}")

    with MotherDuckManager(token, database) as db_manager:
        logger.info("Setting up NHS English Prescriptions database schema...")

        db_manager.setup_for_data_source(config)

        ensure_metadata_schema_exists(config, db_manager)

        logger.success("NHS English Prescriptions schema setup complete!")
        logger.info(f"Schema: {config.schema_name}")
        logger.info(f"Tables ready: {', '.join(config.table_names)}")

        logger.info(
            f"Starting NHS English Prescriptions data processing ({START_MONTH} to {END_MONTH})..."
        )

        process_nhs_prescriptions(
            download_links=config.download_links,
            table_names=config.table_names,
            batch_size=config.batch_limit or 200000,
            conn=db_manager.connection,
            schema_name=config.schema_name,
            expected_columns=config.db_template,
            config=config,
        )

        logger.success(
            f"NHS English Prescriptions ({START_MONTH} to {END_MONTH}) pipeline completed successfully!"
        )
        logger.info(f"Processed {len(config.table_names)} months of data")
        logger.info(f"All data stored in schema: {config.schema_name}")


if __name__ == "__main__":
    main()
