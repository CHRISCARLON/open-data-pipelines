from database.motherduck import MotherDuckManager
from loguru import logger

from auth.get_credentials import get_secrets
from auth.creds import secret_name

from data_sources.street_manager import StreetManager
from data_processors.street_manager import process_data as process_street_manager

from data_sources.bduk_premises import BDUKPremises
from data_processors.bduk_premises import process_bduk

def main():
    # MotherDuck Credentials
    secrets = get_secrets(secret_name)
    token = secrets["motherduck_token"]
    database = "sm_permit"

    # Create Data Source Configs
    bduk_premises_config = BDUKPremises.create_default_latest()
    logger.info(f"bduk_premises_config: {bduk_premises_config}")

    with MotherDuckManager(token, database) as motherduck_manager:
        motherduck_manager.setup_for_data_source(bduk_premises_config)
        process_bduk(
            download_links=bduk_premises_config.download_links,
            table_names=bduk_premises_config.table_names,
            batch_size=bduk_premises_config.batch_limit or 150000,
            conn=motherduck_manager.connection,
            schema_name=bduk_premises_config.schema_name,
            expected_columns=bduk_premises_config.db_template,
        )


if __name__ == "__main__":
    main()
