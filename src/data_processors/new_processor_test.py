import pandas as pd
import requests
from io import BytesIO
from datetime import datetime
from msoffcrypto import OfficeFile
from typing import Optional
from loguru import logger
from data_sources.data_source_config import DataSourceConfig
from data_processors.data_processor_config import DataProcessor

# Custom functions defined in this module
def clean_name_geoplace(x: str):
    """Used to clean names columns ready for left join in the future"""
    x = x.replace("LONDON BOROUGH OF", "").strip()
    x = x.replace("COUNTY COUNCIL", "").strip()
    x = x.replace("BOROUGH COUNCIL", "").strip()
    x = x.replace("CITY COUNCIL", "").strip()
    x = x.replace("COUNCIL", "").strip()
    x = x.replace("ROYAL BOROUGH OF", "").strip()
    x = x.replace("COUNCIL OF THE", "").strip()
    x = x.replace("CITY OF", "").strip()
    x = x.replace("COUNTY", "").strip()
    x = x.replace("BOROUGH", "").strip()
    x = x.replace("CITY", "").strip()
    x = x.replace("METROPOLITAN", "").strip()
    x = x.replace("DISTRICT", "").strip()
    x = x.replace("CORPORATION", "").strip()
    x = x.replace("OF", "").strip()
    x = str(x).lower()
    return x

def fetch_swa_codes(data_stream: BytesIO) -> Optional[pd.DataFrame]:
    """
    Process SWA codes data from BytesIO stream.
    Data is an old xls file and needs extra steps to create dataframe.
    """
    try:
        # Reset stream position to beginning
        data_stream.seek(0)
        office_file = OfficeFile(data_stream)
        office_file.load_key("VelvetSweatshop")
        decrypted_file = BytesIO()
        office_file.decrypt(decrypted_file)
        decrypted_file.seek(0)

        # Read in and do some basic renames and transformation
        df = pd.read_excel(decrypted_file, header=1, engine="xlrd")
        df = df.astype(str).replace("nan", None)
        df.columns = df.columns.str.lower().str.replace(" ", "_").str.replace("/", "_")
        df.loc[:, "account_name"] = df.loc[:, "account_name"].apply(clean_name_geoplace)
        df.loc[df["account_name"] == "peter", "account_name"] = "peterborough"
        df.loc[
            df["account_name"] == "bournemouth, christchurch and poole", "account_name"
        ] = "bournemouth christchurch and poole"
        df.loc[df["account_name"] == "brighton & hove", "account_name"] = (
            "brighton and hove"
        )
        df.loc[df["account_name"] == "telford & wrekin", "account_name"] = (
            "telford and wrekin"
        )
        df.loc[df["account_name"] == "hammersmith & fulham", "account_name"] = (
            "hammersmith and fulham"
        )
        df.loc[df["account_name"] == "cheshire east", "account_name"] = "east cheshire"
        df.loc[df["account_name"] == "cheshire west and chester", "account_name"] = (
            "west cheshire"
        )
        df.loc[df["account_name"] == "east riding  yorkshire", "account_name"] = (
            "eastridingyorkshire"
        )

        # Add date time processed column
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df["date_time_processed"] = current_time
        return df

    except Exception as e:
        logger.error(f"Unexpected error in fetch_swa_codes: {e}")
    return None

# Dictionary of custom functions available in this module
CUSTOM_FUNCTIONS = {
    'fetch_swa_codes': fetch_swa_codes
}

def process_data(config: DataSourceConfig) -> None:
    """
    Main function to fetch and process data.

    Args:
        config: The data source configuration object
    """

    data_processor = DataProcessor(config)

    # Add custom functions to the processor
    for name, func in CUSTOM_FUNCTIONS.items():
        data_processor.add_custom_function(name, func)

    # Get the raw data first
    url = config.download_links[0]
    data_stream = data_processor.fetch_data(url)

    # Use the custom function to process the BytesIO data!!
    df = data_processor.custom_functions['fetch_swa_codes'](data_stream)

    if df is not None:
        logger.info(f"Data shape: {df.shape}")
        logger.info(f"Columns: {df.columns.tolist()}")
        logger.info(f"First few rows:\n{df.head()}")
        return df
    else:
        logger.error("Failed to process data")
