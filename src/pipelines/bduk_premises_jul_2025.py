import os

from ..databases.motherduck import MotherDuckManager
from ..data_sources.bduk_premises_jul_2025 import BDUKPremises
from ..data_processors.bduk_premises import process_bduk
from ..data_processors.utils.metadata_logger import ensure_metadata_schema_exists


def main():
    if not (token := os.getenv("MOTHERDUCK_TOKEN")) or not (
        database := os.getenv("MOTHERDB")
    ):
        raise ValueError("MOTHERDUCK_TOKEN and MOTHERDB must be set")

    config = BDUKPremises.create_default_latest()

    with MotherDuckManager(token, database) as db_manager:
        db_manager.setup_for_data_source(config)
        ensure_metadata_schema_exists(config, db_manager)

        download_links = config.download_links
        table_names = config.table_names

        process_bduk(
            download_links=download_links,
            table_names=table_names,
            batch_size=config.batch_limit or 200000,
            conn=db_manager.connection,
            schema_name=config.schema_name,
            expected_columns=config.db_template,
            config=config,
        )


if __name__ == "__main__":
    main()
