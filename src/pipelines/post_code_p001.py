import os

from ..databases.motherduck import MotherDuckManager
from ..data_sources.post_code_p001 import PostCodeP001
from ..data_processors.post_code_p001 import process_post_code_p001
from ..data_processors.utils.metadata_logger import ensure_metadata_schema_exists


def main():
    if not (token := os.getenv("MOTHERDUCK_TOKEN")) or not (
        database := os.getenv("MOTHERDB")
    ):
        raise ValueError("MOTHERDUCK_TOKEN and MOTHERDB must be set")

    config = PostCodeP001.create_default()

    with MotherDuckManager(token, database) as db_manager:
        db_manager.setup_for_data_source(config)
        ensure_metadata_schema_exists(config, db_manager)

        url = config.download_links[0]
        print(f"Processing Postcode P001 data from {url}")

        process_post_code_p001(
            download_links=config.download_links,
            table_names=config.table_names,
            batch_size=config.batch_limit or 250000,
            conn=db_manager.connection,
            schema_name=config.schema_name,
            config=config,
        )


if __name__ == "__main__":
    main()
