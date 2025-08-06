import os

from ..databases.motherduck import MotherDuckManager
from ..data_sources.code_point import CodePoint
from ..data_processors.code_point import process_data as process_code_point
from ..data_processors.utils.metadata_logger import ensure_metadata_schema_exists


def main():
    if not (token := os.getenv("MOTHERDUCK_TOKEN")) or not (
        database := os.getenv("MOTHERDB")
    ):
        raise ValueError("MOTHERDUCK_TOKEN and MOTHERDB must be set")

    config = CodePoint.create_default_latest()

    with MotherDuckManager(token, database) as db_manager:
        db_manager.setup_for_data_source(config)
        ensure_metadata_schema_exists(config, db_manager)

        url = config.download_links[0]
        print(f"Processing Code Point data from {url}")

        process_code_point(
            url=url,
            conn=db_manager.connection,
            batch_size=config.batch_limit or 250000,
            schema_name=config.schema_name,
            table_name=config.table_names[0],
            config=config,
        )


if __name__ == "__main__":
    main()
