import os

from ..databases.motherduck import MotherDuckManager
from ..data_sources.nhs_english_prescriptions import NHSEnglishPrescriptions
from ..data_processors.utils.metadata_logger import ensure_metadata_schema_exists
from ..data_processors.nhs_english_prescriptions import process_nhs_prescriptions
from loguru import logger


def main():
    """
    Pipeline to process the last 6 months of NHS English Prescriptions data.
    This creates/updates the prescribing schema and loads data for the most recent 6 months.
    """
    if not (token := os.getenv("MOTHERDUCK_TOKEN")) or not (
        database := os.getenv("MOTHERDB_2")
    ):
        raise ValueError("MOTHERDUCK_TOKEN and MOTHERDB_2 must be set")

    # Configure for last 6 months
    config = NHSEnglishPrescriptions.create_last_n_months(6)

    logger.info("Processing last 6 months of NHS prescriptions data")
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
            "Starting NHS English Prescriptions data processing (last 6 months)..."
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
            "NHS English Prescriptions (last 6 months) pipeline completed successfully!"
        )
        logger.info(f"Processed {len(config.table_names)} months of data")
        logger.info(f"All data stored in schema: {config.schema_name}")


if __name__ == "__main__":
    main()
