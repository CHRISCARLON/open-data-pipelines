from database.motherduck import MotherDuckManager
from loguru import logger

from auth.get_credentials import get_secrets
from auth.creds import secret_name

from data_sources.street_manager import StreetManager
from data_processors.street_manager import process_data as process_street_manager


def main():
    # MotherDuck Credentials
    secrets = get_secrets(secret_name)
    token = secrets["motherduck_token"]
    database = "sm_permit"

    # Create Data Source Configs
    street_manager_config = StreetManager.create_default_historic_2025()

    logger.info(f"street_manager_config: {street_manager_config}")

    with MotherDuckManager(token, database) as motherduck_manager:
        motherduck_manager.setup_for_data_source(street_manager_config)
        
        # Process all download links and table names
        for url, table_name in zip(street_manager_config.download_links, street_manager_config.table_names):
            logger.info(f"Processing {table_name}")
            try:
                process_street_manager(
                    url=url,
                    batch_size=street_manager_config.batch_limit or 150000,
                    conn=motherduck_manager.connection,
                    schema_name=street_manager_config.schema_name,
                    table_name=table_name,
                )
            except Exception as e:
                logger.error(f"Failed to process {table_name}: {e}")
                # Continue with next file
                continue


if __name__ == "__main__":
    main()
