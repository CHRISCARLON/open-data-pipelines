import pandas as pd
import requests

from loguru import logger
from typing import Optional
from io import BytesIO
from datetime import datetime
from msoffcrypto import OfficeFile
from ..data_processors.utils.data_processor_utils import insert_table
from ..data_sources.data_source_config import DataProcessorType, DataSourceConfig
from ..data_processors.utils.metadata_logger import metadata_tracker


def clean_name_geoplace(x: str):
    """
    Used to clean names columns ready for left join in the future

    # usage: df.loc[:, "account_name"] = df.loc[:, "account_name"].apply(clean_name)
    """
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


def fetch_swa_codes(url: str) -> tuple[Optional[pd.DataFrame], int]:
    """
    Use download link to fetch data.
    Data is an old xls file and needs extra steps to create dataframe.

    Returns:
        tuple: (DataFrame containing the SWA codes data or None, file size in bytes)
    """
    try:
        assert isinstance(url, str), "URL must be a string"
    except (ValueError, AssertionError) as e:
        logger.error(f"Error getting download link: {e}")
        return None, 0

    try:
        response = requests.get(url)
        response.raise_for_status()

        file_size = len(response.content)
        logger.info(f"Downloaded file size: {file_size:,} bytes")

        result = BytesIO(response.content)
        office_file = OfficeFile(result)
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

        return df, file_size

    except requests.RequestException as e:
        logger.error(f"Error downloading file: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in fetch_swa_codes: {e}")

    return None, 0


def process_data(
    url: str,
    conn,
    schema_name: str,
    table_name: str,
    processor_type: DataProcessorType,
    config: Optional[DataSourceConfig] = None,
) -> None:
    """
    Args:
        url: The URL to fetch the data from.
        conn: The database connection (MotherDuck or PostgreSQL).
        schema_name: The schema of the table.
        table_name: The name of the table.
        processor_type: Type of database processor
        config: Data source configuration (enables metadata logging if provided)

    Returns:
        None
    """
    logger.info(f"Starting Geoplace SWA processing from {url} for {processor_type}")

    # If config is provided, use metadata tracking
    if config:
        with metadata_tracker(config, conn, url) as tracker:
            try:
                df, file_size = fetch_swa_codes(url)
                if df is not None:
                    tracker.set_rows_processed(len(df))
                    tracker.set_file_size(file_size)
                    tracker.add_info("columns_processed", len(df.columns))

                    # Add additional info to the tracker
                    active_accounts_count = df[df["account_status"] == "Active"][
                        "account_name"
                    ].nunique()
                    unactive_accounts_count = df[df["account_status"] == "Inactive"][
                        "account_name"
                    ].nunique()
                    tracker.add_info("active_accounts_count", active_accounts_count)
                    tracker.add_info("unactive_accounts_count", unactive_accounts_count)

                    insert_table(df, conn, schema_name, table_name, processor_type)
                    logger.success(f"Data inserted into {schema_name}.{table_name}")
                else:
                    raise ValueError("Failed to fetch SWA codes data")

            except Exception as e:
                logger.error(f"Error processing data: {e}")
                raise
    else:
        logger.warning("No config provided - metadata logging disabled")
        try:
            df, _ = fetch_swa_codes(url)
            if df is not None:
                insert_table(df, conn, schema_name, table_name, processor_type)
                logger.success(f"Data inserted into {schema_name}.{table_name}")
        except Exception as e:
            logger.error(f"Error processing data: {e}")
            raise
