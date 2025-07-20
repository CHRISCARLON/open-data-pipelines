import os
from typing import Optional

from ..databases.motherduck import MotherDuckManager
from ..data_sources.street_manager import StreetManager
from ..data_processors.street_manager import process_data as process_street_manager
from ..data_processors.utils.metadata_logger import ensure_metadata_schema_exists
from loguru import logger


def main():
    """
    Pipeline to process Street Manager data for the latest month
    """
    if not (token := os.getenv("MOTHERDUCK_TOKEN")) or not (
        database := os.getenv("MOTHERDB")
    ):
        raise ValueError("MOTHERDUCK_TOKEN and MOTHERDB must be set")

    config = StreetManager.create_default_latest()

    with MotherDuckManager(token, database) as db_manager:
        logger.info("Setting up Street Manager database schema...")
        
        # Setup schema and tables for the data source
        db_manager.setup_for_data_source(config)
        
        # Setup metadata logging
        ensure_metadata_schema_exists(config, db_manager)
        
        logger.success("Street Manager schema setup complete!")
        logger.info(f"Created schema: {config.schema_name}")
        logger.info(f"Created tables: {', '.join(config.table_names)}")

        logger.info("Starting Street Manager data processing...")
        
        url = config.download_links[0]
        table_name = config.table_names[0]
        
        logger.info(f"Processing Street Manager data from: {url}")
        logger.info(f"Target table: {config.schema_name}.{table_name}")

        process_street_manager(
            url=url,
            batch_size=config.batch_limit or 150000,
            conn=db_manager.connection,
            schema_name=config.schema_name,
            table_name=table_name,
            config=config,
        )

        logger.success("Street Manager pipeline completed successfully!")


if __name__ == "__main__":
    main()
