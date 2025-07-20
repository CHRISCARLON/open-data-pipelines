import os

from ..databases.motherduck import MotherDuckManager
from ..data_sources.bods_timetables import BODSTimetables
from ..data_processors.utils.metadata_logger import ensure_metadata_schema_exists
from ..data_processors import bods_timetables as bods_processor
from loguru import logger


def main():
    """
    Pipeline to set up BODS Timetables database schema and process GTFS data.
    This creates the bods_timetables schema, all 9 GTFS tables, and loads the data.
    """
    if not (token := os.getenv("MOTHERDUCK_TOKEN")) or not (
        database := os.getenv("MOTHERDB")
    ):
        raise ValueError("MOTHERDUCK_TOKEN and MOTHERDB must be set")

    config = BODSTimetables.create_default_latest()

    with MotherDuckManager(token, database) as db_manager:
        logger.info("Setting up BODS Timetables database schema...")

        db_manager.setup_for_data_source(config)

        ensure_metadata_schema_exists(config, db_manager)

        logger.success("BODS Timetables schema setup complete!")
        logger.info(f"Created schema: {config.schema_name}")
        logger.info(f"Created tables: {', '.join(config.table_names)}")

        logger.info("Starting BODS Timetables data processing...")

        for url in config.download_links:
            logger.info(f"Processing GTFS data from: {url}")

            bods_processor.process_data(
                url=url,
                conn=db_manager.connection,
                batch_limit=config.batch_limit or 300000,
                schema_name=config.schema_name,
                table_name="",
                config=config,
            )

        logger.success("BODS Timetables pipeline completed successfully!")


if __name__ == "__main__":
    main()
