from database.motherduck import MotherDuckManager
from loguru import logger

from auth.get_credentials import get_secrets
from auth.creds import secret_name

from data_sources.nhs_english_prescriptions import NHSEnglishPrescriptions
from data_processors.nhs_english_prescriptions import process_nhs_prescriptions


def main():
    # MotherDuck Credentials
    secrets = get_secrets(secret_name)
    token = secrets["motherduck_token"]
    database = "health_data"

    # Create NHS English Prescriptions Data Source Config
    nhs_prescriptions_config = NHSEnglishPrescriptions.create_default()
    logger.info(f"nhs_prescriptions_config: {nhs_prescriptions_config}")

    with MotherDuckManager(token, database) as motherduck_manager:
        motherduck_manager.setup_for_data_source(nhs_prescriptions_config)
        process_nhs_prescriptions(
            download_links=nhs_prescriptions_config.download_links,
            table_names=nhs_prescriptions_config.table_names,
            batch_size=200000,
            conn=motherduck_manager.connection,
            schema_name=nhs_prescriptions_config.schema_name,
            expected_columns=nhs_prescriptions_config.db_template,
        )


if __name__ == "__main__":
    main()
