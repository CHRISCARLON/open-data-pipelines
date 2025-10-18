from loguru import logger
import time
import requests
import os
import tempfile
import zipfile
import csv
import pandas as pd
from tqdm import tqdm
from typing import Optional, Dict
from ..data_sources.data_source_config import DataProcessorType, DataSourceConfig
from ..data_processors.utils.metadata_logger import metadata_tracker
from ..data_processors.utils.data_processor_utils import insert_table


def clean_dataframe_for_motherduck(
    df: pd.DataFrame, expected_columns: Dict[str, str]
) -> pd.DataFrame:
    """Clean DataFrame and handle empty strings for numeric columns."""
    df_clean = df.copy()

    numeric_columns = {
        col: dtype
        for col, dtype in expected_columns.items()
        if dtype in ["BIGINT", "DOUBLE", "INTEGER"]
    }

    for col, dtype in numeric_columns.items():
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].replace(["", "nan", "NaN", "null"], None)

            if dtype == "BIGINT":
                # Convert to numeric first, then to nullable integer
                numeric_series = pd.to_numeric(df_clean[col], errors="coerce")
                df_clean[col] = pd.Series(numeric_series).astype("Int64")
            elif dtype == "DOUBLE":
                df_clean[col] = pd.to_numeric(df_clean[col], errors="coerce")

    string_columns = [col for col in df_clean.columns if col not in numeric_columns]
    for col in string_columns:
        df_clean[col] = df_clean[col].astype(str).replace(["nan", "NaN", "null"], None)

    return df_clean


def fetch_redirect_url(url: str) -> str:
    """
    Call the redirect url and then fetch the actual download url.
    This is suboptimal and will change in future versions.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        redirect_url = response.url
    except (requests.exceptions.RequestException, ValueError, Exception):
        logger.error("An error retrieving the redirect URL")
        raise
    return redirect_url


def insert_into_motherduck(df, conn, schema: str, table: str):
    """
    Takes a connection object and a dataframe
    Processes dataframe into MotherDuck table with retry logic

    Args:
        df: Dataframe to process
        conn: Connection object
        schema: Database schema
        table: Table name
    """
    max_retries = 3
    base_delay = 3

    if not conn:
        logger.error("No connection provided")
        return None

    def attempt_insert(retry_count):
        """Closure for handling a single insert attempt with logging"""
        try:
            conn.register("temp_df", df)

            insert_sql = f"""INSERT INTO "{schema}"."{table}" SELECT * FROM temp_df"""
            conn.execute(insert_sql)

            if retry_count > 0:
                logger.success(
                    f"Successfully inserted data on attempt {retry_count + 1}"
                )
            return True
        except Exception as e:
            if retry_count < max_retries - 1:
                wait_time = (2**retry_count) * base_delay
                logger.warning(f"Attempt {retry_count + 1} failed: {e}")
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                return False
            else:
                logger.error(f"All {max_retries} attempts failed. Final error: {e}")
                raise

    for attempt in range(max_retries):
        if attempt_insert(attempt):
            return True

    return False


def load_csv_data(
    url: str,
    conn,
    batch_limit: int,
    schema: str,
    name: str,
    processor_type: DataProcessorType,
    expected_columns: Optional[Dict[str, str]] = None,
):
    """
    Function to stream and process CSV data in batches.

    Args:
        url (str): URL of the zipped CSV file
        conn: DuckDB connection object
        batch_limit (int): Number of rows to process in each batch
        schema (str): Database schema
        name (str): Table name
        processor_type: Type of database processor
        expected_columns: Optional dict of expected column names and types
    """
    if expected_columns:
        fieldnames = list(expected_columns.keys())
        logger.debug("Using the DB config")
    else:
        raise ValueError(
            "Expected columns must be provided for National Stat Post Code processing"
        )

    errors = []
    total_rows_processed = 0
    file_size = 0

    def handle_error(message, exception=None, row_num=None):
        """Closure for consistent error handling"""
        error_msg = f"{message}"
        if row_num:
            error_msg = f"Error processing row {row_num}: {exception}"
        elif exception:
            error_msg = f"{message}: {exception}"

        logger.warning(error_msg) if row_num else logger.error(error_msg)
        errors.append(error_msg)

    def process_batch(batch, is_final=False):
        """Closure for processing a batch of data"""
        nonlocal total_rows_processed

        if not batch:
            return

        try:
            df_chunk = pd.DataFrame(batch)

            # Clean the dataframe if expected_columns are available
            if expected_columns:
                df_chunk = clean_dataframe_for_motherduck(df_chunk, expected_columns)

            insert_table(df_chunk, conn, schema, name, processor_type)

            batch_size = len(batch)
            total_rows_processed += batch_size
            start_row = total_rows_processed - batch_size + 1

            batch_type = "final batch: rows" if is_final else "rows"
            logger.info(f"Processed {batch_type} {start_row} to {total_rows_processed}")
        except Exception as e:
            message = (
                "Error processing final batch" if is_final else "Error processing batch"
            )
            handle_error(message, e)

    try:
        actual_url = fetch_redirect_url(url)
        response = requests.get(actual_url, stream=True)
        response.raise_for_status()

        with tempfile.TemporaryDirectory() as temp_dir:
            zip_path = os.path.join(temp_dir, "temp.zip")
            total_size = int(response.headers.get("content-length", 0))
            file_size = total_size
            logger.info(f"Downloading {total_size / 1024 / 1024:.2f} MB")

            with open(zip_path, "wb") as zip_file:
                with tqdm(
                    total=total_size, unit="B", unit_scale=True, desc="Downloading"
                ) as pbar:
                    for chunk in response.iter_content(chunk_size=8192):
                        zip_file.write(chunk)
                        pbar.update(len(chunk))

            logger.info("Extracting zip file")
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(temp_dir)

            csv_file = None
            for root, dirs, files in os.walk(temp_dir):
                if "Data" in dirs:
                    data_dir = os.path.join(root, "Data")
                    for filename in os.listdir(data_dir):
                        if filename.endswith(".csv") and "NSPL" in filename:
                            csv_file = os.path.join(data_dir, filename)
                            print(csv_file)

            if not csv_file:
                handle_error("No CSV file found in the zip archive")
                raise FileNotFoundError("No CSV file found in the zip archive")

            total_lines = sum(1 for _ in open(csv_file))
            logger.info(f"Processing {total_lines - 1} rows from CSV")

            current_batch = []
            with open(csv_file, "r", newline="") as file:
                reader = csv.DictReader(file, fieldnames=fieldnames)
                next(reader)

                for i, row in enumerate(
                    tqdm(reader, total=total_lines - 1, desc="Processing rows"), 1
                ):
                    try:
                        current_batch.append(row)

                        if len(current_batch) >= batch_limit:
                            process_batch(current_batch)
                            current_batch = []

                    except Exception as e:
                        handle_error("Error processing row", e, i)
                        continue

                if current_batch:
                    process_batch(current_batch, is_final=True)

    except Exception as e:
        handle_error("Error processing the zip file", e)
        raise

    finally:
        if errors:
            logger.error(f"Total errors encountered: {len(errors)}")
            for error in errors:
                print(error)

        logger.info(
            f"Completed processing. Total rows processed: {total_rows_processed}"
        )

    return total_rows_processed, file_size, len(fieldnames)


def process_data(
    url: str,
    conn,
    batch_limit: int,
    schema_name: str,
    table_name: str,
    processor_type: Optional[DataProcessorType] = None,
    config: Optional[DataSourceConfig] = None,
):
    """
    Process the data from the url and insert it into the database.

    Args:
        url: URL to fetch data from
        conn: Database connection
        batch_limit: Number of rows per batch
        schema_name: Database schema
        table_name: Table name
        processor_type: Type of database processor (for backward compatibility)
        config: Data source configuration (enables metadata logging if provided)
    """
    if config:
        proc_type = config.processor_type
    elif processor_type:
        proc_type = processor_type
    else:
        proc_type = DataProcessorType.MOTHERDUCK

    logger.info(f"Starting OS USRN-UPRN processing from {url} for {proc_type}")

    if config:
        with metadata_tracker(config, conn, url) as tracker:
            try:
                expected_columns = config.db_template if config else None
                total_rows, file_size, fieldnames_count = load_csv_data(
                    url,
                    conn,
                    batch_limit,
                    schema_name,
                    table_name,
                    proc_type,
                    expected_columns,
                )

                tracker.set_rows_processed(total_rows)
                tracker.set_file_size(file_size)
                tracker.add_info("batch_limit", batch_limit)
                tracker.add_info("fieldnames_count", fieldnames_count)

                logger.success(f"Data inserted into {schema_name}.{table_name}")

            except Exception as e:
                logger.error(f"Error processing data: {e}")
                raise
    else:
        logger.warning("No config provided - metadata logging disabled")
        try:
            total_rows, file_size, fieldnames_count = load_csv_data(
                url, conn, batch_limit, schema_name, table_name, proc_type, None
            )
            logger.success(f"Data inserted into {schema_name}.{table_name}")
        except Exception as e:
            logger.error(f"Error processing data: {e}")
            raise
