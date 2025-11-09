import os

from ..databases.motherduck import MotherDuckManager
from ..data_sources.cadent_underground import CadentUndergroundPipes
from ..data_processors.cadent_underground import process_cadent_data


def main():
    if not (token := os.getenv("MOTHERDUCK_TOKEN")) or not (
        database := os.getenv("MOTHERDB")
    ):
        raise ValueError("MOTHERDUCK_TOKEN and MOTHERDB must be set")

    config = CadentUndergroundPipes.create_default_latest()

    with MotherDuckManager(token, database) as db_manager:
        db_manager.setup_for_data_source(config)

        download_url = config.download_links[0]
        table_name = config.table_names[0]

        process_cadent_data(
            url=download_url,
            batch_size=config.batch_limit or 150000,
            motherduck_conn=db_manager.connection,
            schema_name=config.schema_name,
            table_name=table_name,
            expected_columns=None,  # Disable validation - accept CSV as-is
        )


if __name__ == "__main__":
    main()
