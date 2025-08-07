import os

from ..databases.motherduck import MotherDuckManager
from ..data_sources.national_stat_postcode_lookup import NationalStatisticPostcodeLookup
from ..data_processors.national_stat_postcode_lookup import process_data
from ..data_processors.utils.metadata_logger import ensure_metadata_schema_exists


def main():
    if not (token := os.getenv("MOTHERDUCK_TOKEN")) or not (
        database := os.getenv("MOTHERDB")
    ):
        raise ValueError("MOTHERDUCK_TOKEN and MOTHERDB must be set")

    config = NationalStatisticPostcodeLookup.create_default()

    with MotherDuckManager(token, database) as db_manager:
        db_manager.setup_for_data_source(config)
        ensure_metadata_schema_exists(config, db_manager)

        url = config.download_links[0]
        process_data(
            url=url,
            conn=db_manager.connection,
            batch_limit=200000,
            schema_name=config.schema_name,
            table_name=config.table_names[0],
            processor_type=config.processor_type,
            config=config,
        )


if __name__ == "__main__":
    main()
