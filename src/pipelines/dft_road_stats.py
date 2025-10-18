import os

from loguru import logger

from ..databases.motherduck import MotherDuckManager
from ..data_sources.dft_road_stats import DftRoadStats
from ..data_processors.utils.metadata_logger import ensure_metadata_schema_exists
from ..data_processors.dft_road_stats import process_dft_road_stats


def main():
    """
    Pipeline to set up DFT Road Stats database schema and process ODS/ZIP data.
    This creates the dft_road_stats schema, tables, and loads the data.

    Processes only specified data.
"""
    if not (token := os.getenv("MOTHERDUCK_TOKEN")) or not (
        database := os.getenv("MOTHERDB")
    ):
        raise ValueError("MOTHERDUCK_TOKEN and MOTHERDB must be set")

    config = DftRoadStats.create_default_latest()

    with MotherDuckManager(token, database) as db_manager:
        logger.info("Setting up DFT Road Stats database schema...")

        db_manager.setup_for_data_source(config)
        ensure_metadata_schema_exists(config, db_manager)

        logger.success("DFT Road Stats schema setup complete!")
        logger.info(f"Created schema: {config.schema_name}")

        all_links = config.download_links
        filtered_links = {
            key: url
            for key, url in all_links.items()
            if key.lower() in ["rdl0101", "rdl0102", "rdl0201", "rdl0202"]
        }

        if not filtered_links:
            logger.warning("No files found in download links")
            logger.info(f"Available files: {list(all_links.keys())}")
            return

        logger.info(f"Processing {len(filtered_links)} files: {list(filtered_links.keys())}")

        # Define sheet names for each file code
        sheet_names = {
            "rdl0101": "RDL0101a",
            "rdl0102": "RDL0102a",
            "rdl0201": "RDL0201a",
            "rdl0202": "RDL0202a",
        }

        # Define header rows for each file code (0-indexed)
        # RDL0101 has headers at row 7 (index 6)
        # RDL0202 has headers at row 8 (index 7)
        header_rows = {
            "rdl0101": 6,
            "rdl0102": 7,
            "rdl201": 6,
            "rdl0202": 7,
        }

        logger.info("Starting DFT Road Stats data processing...")

        process_dft_road_stats(
            download_links=filtered_links,
            conn=db_manager.connection,
            schema_name=config.schema_name,
            batch_size=config.batch_limit or 250000,
            config=config,
            sheet_names=sheet_names,
            header_rows=header_rows,
        )

        logger.success("DFT Road Stats pipeline completed successfully!")


if __name__ == "__main__":
    main()
