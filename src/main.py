from database.motherduck import MotherDuckManager
from loguru import logger

from auth.get_credentials import get_secrets
from auth.creds import secret_name

from data_sources.street_manager import StreetManager
from data_sources.data_source_config import DataProcessorType, TimeRange
from data_processors.street_manager import process_data


def main():
    # MotherDuck Credentials
    secrets = get_secrets(secret_name)
    token = secrets["motherduck_token"]
    database = "street_works_data"

    # Create Street Manager Data Source Config for 2025 (Jan-May)
    street_manager_config = StreetManager(
        processor_type=DataProcessorType.MOTHERDUCK,
        time_range=TimeRange.HISTORIC,
        batch_limit=200000,
        year=2025,
        start_month=1,
        end_month=6,  # Non-inclusive, so this covers Jan-May
    )
    
    logger.info(f"street_manager_config: {street_manager_config}")
    logger.info(f"Processing {len(street_manager_config.download_links)} months of data")
    logger.info(f"Download links: {street_manager_config.download_links}")

    with MotherDuckManager(token, database) as motherduck_manager:
        motherduck_manager.setup_for_data_source(street_manager_config)
        
        # Process each month of data
        for i, url in enumerate(street_manager_config.download_links):
            table_name = street_manager_config.table_names[i]
            logger.info(f"Processing {url} -> {table_name}")
            
            # Ensure batch_size is not None
            batch_size = street_manager_config.batch_limit or 200000
            
            process_data(
                url=url,
                batch_size=batch_size,
                conn=motherduck_manager.connection,
                schema_name=street_manager_config.schema_name,
                table_name=table_name,
            )


if __name__ == "__main__":
    main()
