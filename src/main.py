"""
Main entry point for the data pipeline.
"""
from databases.motherduck import MotherDuckManager
from loguru import logger

from auth.get_credentials import get_secrets
from auth.creds import secret_name

from data_sources.naptan import Naptan
from data_processors.naptan import process_data as process_naptan


def main():
    # MotherDuck Credentials
    secrets = get_secrets(secret_name)
    token = secrets["motherduck_token"]
    database = "sm_permit" 

    # Create Data Source Config
    naptan_config = Naptan.create_default_latest()

    logger.info(f"naptan_config: {naptan_config}")

    with MotherDuckManager(token, database) as motherduck_manager:
        motherduck_manager.setup_for_data_source(naptan_config)

        download_link = naptan_config.download_links[0] 
        table_name = naptan_config.table_names[0]  
        
        logger.info(f"Processing {table_name}")
        try:
            process_naptan(
                download_link=download_link,
                table_name=table_name,
                batch_size=naptan_config.batch_limit or 100000,
                conn=motherduck_manager.connection,
                schema_name=naptan_config.schema_name,
                processor_type=naptan_config.processor_type,
                expected_columns=naptan_config.db_template,
            )
            logger.success(f"Successfully completed processing {table_name}")
        except Exception as e:
            logger.error(f"Failed to process {table_name}: {e}")
            raise


if __name__ == "__main__":
    main()