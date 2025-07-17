from loguru import logger
import time
import requests
import os
import tempfile
import zipfile
import csv
import pandas as pd
from tqdm import tqdm
from typing import Optional
from ..data_sources.data_source_config import DataProcessorType, DataSourceConfig
from ..data_processors.utils.metadata_logger import metadata_tracker
from ..data_processors.utils.data_processor_utils import insert_table


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
            # Register the DataFrame with the connection
            conn.register("temp_df", df)

            # Now use the registered name in the SQL
            insert_sql = f"""INSERT INTO "{schema}"."{table}" SELECT * FROM temp_df"""
            conn.execute(insert_sql)

            if retry_count > 0:  # Log if it succeeded after retries
                logger.success(
                    f"Successfully inserted data on attempt {retry_count + 1}"
                )
            return True
        except Exception as e:
            if retry_count < max_retries - 1:  # Don't wait on the last attempt
                wait_time = (2**retry_count) * base_delay
                logger.warning(f"Attempt {retry_count + 1} failed: {e}")
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                return False
            else:
                logger.error(f"All {max_retries} attempts failed. Final error: {e}")
                raise

    # Execute attempts using the closure
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
    """
    fieldnames = [
        "CORRELATION_ID",
        "IDENTIFIER_1",
        "VERSION_NUMBER_1",
        "VERSION_DATE_1",
        "IDENTIFIER_2",
        "VERSION_NUMBER_2",
        "VERSION_DATE_2",
        "CONFIDENCE",
    ]

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
        # Get and process the data
        logger.info(f"Downloading file from {url}")
        response = requests.get(url, stream=True)
        response.raise_for_status()

        with tempfile.TemporaryDirectory() as temp_dir:
            # Download and extract zip file
            zip_path = os.path.join(temp_dir, "temp.zip")

            # Add progress bar for download
            total_size = int(response.headers.get("content-length", 0))
            file_size = total_size  # Store for metadata
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

            # Find CSV file
            csv_file = next(
                (
                    os.path.join(temp_dir, f)
                    for f in os.listdir(temp_dir)
                    if f.endswith(".csv")
                ),
                None,
            )

            if not csv_file:
                handle_error("No CSV file found in the zip archive")
                raise FileNotFoundError("No CSV file found in the zip archive")

            # Get total line count for progress bar
            total_lines = sum(1 for _ in open(csv_file))
            logger.info(f"Processing {total_lines - 1} rows from CSV")  # -1 for header

            # Process CSV data
            current_batch = []
            with open(csv_file, "r", newline="") as file:
                reader = csv.DictReader(file, fieldnames=fieldnames)
                next(reader)  # Skip header

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

                # Process remaining rows
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

    return total_rows_processed, file_size


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
        proc_type = DataProcessorType.MOTHERDUCK  # Default fallback

    logger.info(f"Starting OS USRN-UPRN processing from {url} for {proc_type}")

    # If config is provided, use metadata tracking if not, use the old method
    if config:
        with metadata_tracker(config, conn, url) as tracker:
            try:
                total_rows, file_size = load_csv_data(
                    url, conn, batch_limit, schema_name, table_name, proc_type
                )

                # Update tracker with processing stats
                tracker.set_rows_processed(total_rows)
                tracker.set_file_size(file_size)
                tracker.add_info("batch_limit", batch_limit)
                tracker.add_info("fieldnames_count", 8)

                logger.success(f"Data inserted into {schema_name}.{table_name}")

            except Exception as e:
                logger.error(f"Error processing data: {e}")
                raise
    else:
        # Backward compatibility - no metadata tracking - but you should always use the method above
        logger.warning("No config provided - metadata logging disabled")
        try:
            load_csv_data(url, conn, batch_limit, schema_name, table_name, proc_type)
            logger.success(f"Data inserted into {schema_name}.{table_name}")
        except Exception as e:
            logger.error(f"Error processing data: {e}")
            raise
