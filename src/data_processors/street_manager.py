import json
import pyarrow as pa
from typing import Iterator, Any, Optional
import requests
import time

from stream_unzip import stream_unzip
from loguru import logger
from tqdm import tqdm

from ..data_sources.data_source_config import DataSourceConfig
from ..data_processors.utils.metadata_logger import metadata_tracker


def rename_columns(column_names: list[str]) -> list[str]:
    """
    Replace 'object_data.' prefix in column names with empty string.

    Args:
        column_names: List of column names

    Returns:
        List of renamed column names
    """
    return [
        col.replace("object_data.", "") if "object_data." in col else col
        for col in column_names
    ]


def insert_table_to_motherduck(
    table: pa.Table, conn, schema: str, table_name: str
) -> None:
    """
    Inserts a PyArrow table into a MotherDuck table with retry logic.
    """
    max_retries = 3
    base_delay = 3

    for attempt in range(max_retries):
        try:
            conn.register("input_data", table)

            column_names = table.column_names
            columns_sql = ", ".join([f'"{name}"' for name in column_names])

            placeholders = ", ".join([f"input_data.{name}" for name in column_names])

            insert_sql = f"""INSERT INTO "{schema}"."{table_name}" ({columns_sql}) 
                            SELECT {placeholders} FROM input_data"""

            conn.execute(insert_sql)
            logger.success(f"Inserted {len(table)} rows into {schema}.{table_name}")
            return

        except Exception as e:
            try:
                conn.unregister("input_data")
            except Exception:
                pass

            if "lease expired" in str(e) and attempt < max_retries - 1:
                wait_time = (2**attempt) * base_delay
                logger.warning(f"Connection lease expired (attempt {attempt + 1}): {e}")
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logger.error(f"Error inserting PyArrow Table into DuckDB: {e}")
                raise


def flatten_json(json_data) -> dict:
    """
    Street manager archived open data comes in nested json files
    This function flattens the structure

    Args:
        json_data to flatten

    Returns:
        flattened data
    """
    flattened_data = {}

    def flatten(data, prefix=""):
        if isinstance(data, dict):
            for key in data:
                flatten(data[key], f"{prefix}{key}.")
        else:
            flattened_data[prefix[:-1]] = data

    flatten(json_data)
    return flattened_data


def process_json_chunk(chunk: bytes) -> dict[str, Any]:
    """
    Process a single JSON chunk by parsing it and flattening it.

    Args:
        chunk: Bytes containing JSON data

    Returns:
        Flattened JSON data as dictionary
    """
    json_data = json.loads(chunk.decode("utf-8"))
    return flatten_json(json_data)


def chunks_to_arrow_table(flattened_data: list[dict[str, Any]]) -> pa.Table:
    """
    Convert a list of flattened dictionaries to a PyArrow table.

    Args:
        flattened_data: List of dictionaries with flattened data

    Returns:
        PyArrow Table
    """
    # Create PyArrow arrays for each field
    fields = {}
    for key in flattened_data[0].keys():
        values = [item.get(key, None) for item in flattened_data]
        fields[key] = values

    # Create PyArrow table from dictionary
    table = pa.Table.from_pydict(fields)

    # Replace schema metadata
    table = table.replace_schema_metadata({})

    # Rename columns to remove 'object_data.' prefix
    new_names = rename_columns(table.column_names)
    table = table.rename_columns(new_names)

    return table


def batch_processor(
    zipped_chunks: Iterator, 
    batch_size: int, 
    conn, 
    schema_name: str, 
    table_name: str,
    tracker=None
) -> int:
    """
    Process data in batches and insert into MotherDuck.
    Returns total rows processed.
    """
    batch_count = 0
    total_rows_processed = 0
    flattened_data = []
    current_file = None
    current_item = None

    try:
        for file, size, unzipped_chunks in tqdm(stream_unzip(zipped_chunks)):
            current_file = file.decode("utf-8") if isinstance(file, bytes) else file

            try:
                bytes_obj = b"".join(unzipped_chunks)
                current_item = process_json_chunk(bytes_obj)
                flattened_data.append(current_item)
                batch_count += 1

                if batch_count >= batch_size:
                    table = chunks_to_arrow_table(flattened_data)
                    insert_table_to_motherduck(table, conn, schema_name, table_name)
                    logger.success(f"Processed batch of {batch_count} items")
                    
                    total_rows_processed += batch_count
                    flattened_data = []
                    batch_count = 0
                    current_item = None

            except Exception as e:
                logger.error(f"Error processing file {current_file}: {e}")
                if current_item:
                    logger.error("Last processed item:")
                    for k, v in current_item.items():
                        logger.error(f"{k}: {type(v)} = {v}")
                raise

        # Process remaining items
        if flattened_data:
            table = chunks_to_arrow_table(flattened_data)
            insert_table_to_motherduck(table, conn, schema_name, table_name)
            logger.success(f"Processed final batch of {len(flattened_data)} items")
            total_rows_processed += len(flattened_data)

        logger.success("Data processing complete - all batches have been processed")
        return total_rows_processed

    except Exception as e:
        logger.error(f"Error during batch processing: {e}")
        if flattened_data:
            logger.error(f"Number of items in current batch: {len(flattened_data)}")
            if flattened_data:
                last_item = flattened_data[-1]
                logger.error("Last processed item:")
                for k, v in last_item.items():
                    logger.error(f"{k}: {type(v)} = {v}")
        raise


def process_data(
    url: str, 
    batch_size: int, 
    conn, 
    schema_name: str, 
    table_name: str,
    config: Optional[DataSourceConfig] = None
) -> None:
    """
    Main function to fetch and process data stream with PyArrow and metadata tracking.
    """
    logger.info(f"Starting data stream processing from {url} with batch size {batch_size}")

    if config:
        with metadata_tracker(config, conn, url) as tracker:
            try:
                with requests.get(url, stream=True, timeout=15) as response:
                    if response.status_code != 200:
                        logger.error(f"Failed to fetch data: HTTP {response.status_code}")
                        raise Exception(f"HTTP error: {response.status_code}")

                    # Get file size from headers if available
                    file_size = int(response.headers.get('content-length', 0))
                    if file_size > 0:
                        tracker.set_file_size(file_size)
                        tracker.add_info("file_size_mb", round(file_size / 1024 / 1024, 2))

                    zipped_chunks = response.iter_content(chunk_size=1048576)
                    total_rows = batch_processor(zipped_chunks, batch_size, conn, schema_name, table_name, tracker)
                    
                    # Update tracker with processing stats
                    tracker.set_rows_processed(total_rows)
                    tracker.add_info("batch_size", batch_size)
                    tracker.add_info("table_name", table_name)

            except Exception as e:
                logger.error(f"Error processing data with metadata: {e}")
                raise
    else:
        logger.warning("No config provided - metadata logging disabled")
        try:
            with requests.get(url, stream=True, timeout=15) as response:
                if response.status_code != 200:
                    logger.error(f"Failed to fetch data: HTTP {response.status_code}")
                    raise Exception(f"HTTP error: {response.status_code}")

                zipped_chunks = response.iter_content(chunk_size=1048576)
                batch_processor(zipped_chunks, batch_size, conn, schema_name, table_name)

        except Exception as e:
            logger.error(f"Error processing data: {e}")
            raise
