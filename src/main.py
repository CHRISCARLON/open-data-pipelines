from database.motherduck import MotherDuckManager
from loguru import logger

from auth.get_credentials import get_secrets
from auth.creds import secret_name

from data_sources.built_up_areas import BuiltUpAreas
from data_processors.built_up_areas import process_built_up_areas

def main():
    # MotherDuck Credentials
    secrets = get_secrets(secret_name)
    token = secrets["motherduck_token"]
    database = "sm_permit"

    # Create Built Up Areas Data Source Config
    built_up_areas_config = BuiltUpAreas.create_default_latest()
    logger.info(f"built_up_areas_config: {built_up_areas_config}")

    with MotherDuckManager(token, database) as motherduck_manager:
        # Setup schema and table for Built Up Areas data
        motherduck_manager.setup_for_data_source(built_up_areas_config)
        
        # Process Built Up Areas data
        process_built_up_areas(
            url=built_up_areas_config.download_links[0], 
            conn=motherduck_manager.connection,
            batch_size=built_up_areas_config.batch_limit or 150000,
            schema_name=built_up_areas_config.schema_name,
            table_name=built_up_areas_config.table_names[0]
        )


if __name__ == "__main__":
    main()
