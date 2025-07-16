import os

from ..databases.motherduck import MotherDuckManager
from ..data_sources.geoplace_swa import GeoplaceSwa
from ..data_processors.geoplace_swa import process_data as process_geoplace_swa
from ..data_processors.utils.metadata_logger import ensure_metadata_schema_exists


def main():
    if not (token := os.getenv("MOTHERDUCK_TOKEN")) or not (database := os.getenv("MOTHERDB")):
        raise ValueError("MOTHERDUCK_TOKEN and MOTHERDB must be set")
        
    config = GeoplaceSwa.create_default_latest()

    with MotherDuckManager(token, database) as db_manager:
        db_manager.setup_for_data_source(config)
        ensure_metadata_schema_exists(config, db_manager)

        url = config.download_links[0]
        process_geoplace_swa(
            url=url,
            conn=db_manager.connection,
            schema_name=config.schema_name,
            table_name=config.table_names[0],
            processor_type=config.processor_type,
            config=config,
        )


if __name__ == "__main__":
    main()
