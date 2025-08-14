import os

from ..databases.motherduck import MotherDuckManager
from ..data_sources.section_58 import Section58
from ..data_processors.section_58 import process_data as process_section_58
from ..data_processors.utils.metadata_logger import ensure_metadata_schema_exists
from loguru import logger


def main():
    """
    Pipeline to process Section 58 data for the latest month
    """
    if not (token := os.getenv("MOTHERDUCK_TOKEN")) or not (
        database := os.getenv("MOTHERDB")
    ):
        raise ValueError("MOTHERDUCK_TOKEN and MOTHERDB must be set")

    config = Section58.create_default_latest()

    with MotherDuckManager(token, database) as db_manager:
        logger.info("Setting up Section 58 database schema...")

        db_manager.setup_for_data_source(config)

        ensure_metadata_schema_exists(config, db_manager)

        logger.success("Section 58 schema setup complete!")
        logger.info(f"Created staging schema: {config.staging_schema}")
        logger.info(f"Created staging table: {config.staging_table}")
        logger.info(f"Dimension schema: {config.dimension_schema}")
        logger.info(f"Dimension table: {config.dimension_table}")

        logger.info("Starting Section 58 data processing...")

        for url in config.download_links:
            logger.info(f"Processing Section 58 data from: {url}")
            logger.info(
                f"Target staging table: {config.staging_schema}.{config.staging_table}"
            )
            logger.info(
                f"Target dimension table: {config.dimension_schema}.{config.dimension_table}"
            )

            process_section_58(
                url=url,
                batch_size=config.batch_limit or 300000,
                conn=db_manager.connection,
                config=config,
            )

            logger.success(f"Completed processing: {url}")

        logger.success("Section 58 pipeline completed successfully!")


if __name__ == "__main__":
    main()
