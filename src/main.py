from loguru import logger

from data_sources.geoplace_swa import GeoplaceSwa
from data_processors.new_processor_test import process_data


def main():
    # Create Geoplace SWA Data Source Config
    geoplace_swa_config = GeoplaceSwa.create_postgresql_latest()
    logger.info(f"geoplace_swa_config: {geoplace_swa_config}")
    process_data(geoplace_swa_config)


if __name__ == "__main__":
    main()
