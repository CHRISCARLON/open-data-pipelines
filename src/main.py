from database.postgresql import PostgreSQLManager
from loguru import logger

from data_sources.geoplace_swa import GeoplaceSwa
from data_processors.geoplace_swa import process_data

def main():
    # PostgreSQL Credentials - test
    postgres_host = "localhost"
    postgres_port = 5432
    postgres_database = "street_works_data"
    postgres_user = "postgres"
    postgres_password = "postgres123"

    # Create Geoplace SWA Data Source Config
    geoplace_swa_config = GeoplaceSwa.create_postgresql_latest()
    logger.info(f"geoplace_swa_config: {geoplace_swa_config}")

    with PostgreSQLManager(
        host=postgres_host,
        port=postgres_port,
        database=postgres_database,
        user=postgres_user,
        password=postgres_password
    ) as postgres_manager:
        postgres_manager.setup_for_data_source(geoplace_swa_config)
    
        process_data(
            url=geoplace_swa_config.download_links[0], 
            conn=postgres_manager.connection,
            schema_name=geoplace_swa_config.schema_name,
            table_name=geoplace_swa_config.table_names[0],
            db_type="postgresql"
        )


if __name__ == "__main__":
    main()
