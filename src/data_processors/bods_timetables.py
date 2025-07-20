import csv
import pandas as pd
from typing import Iterator, Dict, Optional
import requests
from loguru import logger
from tqdm import tqdm
from stream_unzip import stream_unzip
import time
from ..data_sources.data_source_config import DataSourceConfig
from ..data_processors.utils.metadata_logger import metadata_tracker
from ..data_processors.utils.data_processor_utils import insert_table


def insert_into_motherduck_with_retry(
    df: pd.DataFrame, conn, schema: str, table: str
) -> bool:
    """
    Insert DataFrame into MotherDuck with retry logic.
    """
    max_retries = 3
    base_delay = 3

    if not conn:
        logger.error("No connection provided")
        return False

    for attempt in range(max_retries):
        try:
            conn.register("temp_df", df)
            insert_sql = f"""INSERT INTO "{schema}"."{table}" SELECT * FROM temp_df"""
            conn.execute(insert_sql)

            if attempt > 0:
                logger.success(f"Successfully inserted data on attempt {attempt + 1}")

            return True

        except Exception as e:
            conn.unregister("temp_df")

            if attempt < max_retries - 1:
                wait_time = (2**attempt) * base_delay
                logger.warning(f"Attempt {attempt + 1} failed: {e}")
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logger.error(f"All {max_retries} attempts failed. Final error: {e}")
                raise
        finally:
            try:
                conn.unregister("temp_df")
            except Exception:
                pass

    return False


def get_table_name_from_filename(filename: str) -> Optional[str]:
    """
    Map GTFS filenames to table names.
    GTFS files are like agency.txt, routes.txt etc.
    """
    # Remove .txt extension and get base name
    base_name = filename.lower().replace(".txt", "")

    # Map of GTFS filenames to our table names
    gtfs_mapping = {
        "agency": "agency",
        "calendar": "calendar",
        "calendar_dates": "calendar_dates",
        "feed_info": "feed_info",
        "routes": "routes",
        "shapes": "shapes",
        "stops": "stops",
        "stop_times": "stop_times",
        "trips": "trips",
    }

    return gtfs_mapping.get(base_name)


def stream_gtfs_csv_from_zip(
    zip_url: str, batch_size: int, expected_tables: Dict[str, Dict[str, str]]
) -> Iterator[tuple[str, pd.DataFrame]]:
    """
    Stream GTFS CSV files from a ZIP archive.
    """
    try:
        logger.info(f"Starting GTFS ZIP stream from {zip_url}")
        response = requests.get(zip_url, stream=True, timeout=60)
        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0))

        with tqdm(
            total=total_size, unit="B", unit_scale=True, desc="Streaming GTFS ZIP"
        ) as pbar:

            def chunked_response():
                for chunk in response.iter_content(chunk_size=1048576):
                    pbar.update(len(chunk))
                    yield chunk

            # Process ZIP as it streams
            for file_name, file_size, unzipped_chunks in stream_unzip(
                chunked_response()
            ):
                # Handle different types from stream_unzip
                if isinstance(file_name, (bytes, bytearray)):
                    file_name_str = file_name.decode("utf-8", errors="ignore")
                elif isinstance(file_name, memoryview):
                    file_name_str = bytes(file_name).decode("utf-8", errors="ignore")
                else:
                    file_name_str = str(file_name)

                # Only process .txt files (GTFS format)
                if file_name_str.lower().endswith(".txt"):
                    table_name = get_table_name_from_filename(file_name_str)

                    if table_name and table_name in expected_tables:
                        logger.info(
                            f"Processing GTFS file: {file_name_str} -> {table_name}"
                        )

                        # Stream process this file
                        row_buffer = []
                        header = None
                        partial_line = ""

                        try:
                            for chunk in unzipped_chunks:
                                try:
                                    text_chunk = chunk.decode("utf-8")
                                except UnicodeDecodeError:
                                    text_chunk = chunk.decode("utf-8", errors="ignore")

                                text_chunk = partial_line + text_chunk
                                lines = text_chunk.split("\n")
                                partial_line = lines[-1]
                                lines = lines[:-1]

                                for line in lines:
                                    if not line.strip():
                                        continue

                                    if header is None:
                                        header = next(csv.reader([line]))
                                        logger.info(
                                            f"Found {len(header)} columns in {file_name_str}"
                                        )
                                    else:
                                        try:
                                            values = next(csv.reader([line]))
                                            if len(values) == len(header):
                                                row_dict = dict(zip(header, values))
                                                row_buffer.append(row_dict)

                                                if len(row_buffer) >= batch_size:
                                                    df_batch = pd.DataFrame(row_buffer)
                                                    df_batch = df_batch.astype(
                                                        str
                                                    ).replace("nan", None)
                                                    yield table_name, df_batch
                                                    row_buffer = []
                                                    logger.debug(
                                                        f"Yielded batch of {batch_size} rows for {table_name}"
                                                    )
                                            else:
                                                logger.warning(
                                                    f"Row has {len(values)} values but header has {len(header)} columns in {file_name_str}"
                                                )
                                        except csv.Error as e:
                                            logger.warning(
                                                f"Error parsing CSV line in {file_name_str}: {e}"
                                            )
                                            continue

                            # Process final partial line
                            if partial_line.strip() and header:
                                try:
                                    values = next(csv.reader([partial_line]))
                                    if len(values) == len(header):
                                        row_dict = dict(zip(header, values))
                                        row_buffer.append(row_dict)
                                except csv.Error:
                                    pass

                            # Yield remaining rows for this file
                            if row_buffer:
                                df_batch = pd.DataFrame(row_buffer)
                                df_batch = df_batch.astype(str).replace("nan", None)
                                yield table_name, df_batch
                                logger.debug(
                                    f"Yielded final batch of {len(row_buffer)} rows for {table_name}"
                                )

                        except Exception as e:
                            logger.error(f"Error processing {file_name_str}: {e}")
                            try:
                                for _ in unzipped_chunks:
                                    pass
                            except Exception as e:
                                logger.error(f"Error consuming chunks: {e}")
                                pass

                    else:
                        logger.info(
                            f"Skipping file {file_name_str} (not in expected GTFS tables)"
                        )
                        try:
                            for _ in unzipped_chunks:
                                pass
                        except Exception as e:
                            logger.error(f"Error consuming chunks: {e}")
                            pass
                else:
                    logger.debug(f"Skipping non-txt file: {file_name_str}")
                    try:
                        for _ in unzipped_chunks:
                            pass
                    except Exception as e:
                        logger.error(f"Error consuming chunks: {e}")
                        pass

    except Exception as e:
        logger.error(f"Error streaming GTFS ZIP file {zip_url}: {e}")
        raise


def process_gtfs_streaming_data(
    url: str,
    batch_size: int,
    conn,
    config: DataSourceConfig,
) -> Dict[str, int]:
    """
    Process GTFS data from ZIP URL using streaming.

    Args:
        url: URL of the GTFS ZIP file
        batch_size: Batch size for processing
        conn: Database connection
        config: BODS timetables configuration

    Returns:
        Dict mapping table names to row counts processed
    """
    table_row_counts = {}
    total_batches = 0
    errors = []

    try:
        # Process streamed batches
        for table_name, df_batch in stream_gtfs_csv_from_zip(
            url, batch_size, config.db_template
        ):
            total_batches += 1
            batch_rows = len(df_batch)

            try:
                insert_table(
                    df_batch,
                    conn,
                    config.schema_name,
                    table_name,
                    config.processor_type,
                )

                # Track row counts per table
                if table_name not in table_row_counts:
                    table_row_counts[table_name] = 0
                table_row_counts[table_name] += batch_rows

                logger.info(
                    f"Processed batch for {table_name}: {batch_rows} rows "
                    f"(total for {table_name}: {table_row_counts[table_name]})"
                )

            except Exception as e:
                error_msg = f"Error processing batch for {table_name}: {e}"
                logger.error(error_msg)
                errors.append(error_msg)
                # Continue processing other batches

        if not table_row_counts:
            logger.warning(f"No GTFS data found in {url}")
        else:
            logger.success("Completed processing GTFS data:")
            for table_name, row_count in table_row_counts.items():
                logger.success(f"  {table_name}: {row_count:,} rows")

    except Exception as e:
        error_msg = f"Error processing GTFS ZIP {url}: {e}"
        logger.error(error_msg)
        errors.append(error_msg)
        raise
    finally:
        if errors:
            logger.error(f"Total errors encountered: {len(errors)}")
            for error in errors[:5]:
                logger.error(error)
            if len(errors) > 5:
                logger.error(f"... and {len(errors) - 5} more errors")

    return table_row_counts


def process_data(
    url: str,
    conn,
    batch_limit: int,
    schema_name: str,
    table_name: str,
    config: DataSourceConfig,
):
    """
    Process BODS Timetables GTFS data with metadata tracking.

    Args:
        url: URL to fetch GTFS ZIP from
        conn: Database connection
        batch_limit: Number of rows per batch
        schema_name: Database schema (from config)
        table_name: Not used for GTFS (multiple tables)
        config: BODS timetables configuration
    """
    logger.info(f"Starting BODS Timetables processing from {url}")

    with metadata_tracker(config, conn, url) as tracker:
        try:
            table_row_counts = process_gtfs_streaming_data(
                url, batch_limit, conn, config
            )

            # Calculate total rows processed across all tables
            total_rows = sum(table_row_counts.values())

            # Update tracker with processing stats
            tracker.set_rows_processed(total_rows)
            tracker.add_info("batch_limit", batch_limit)
            tracker.add_info("tables_processed", len(table_row_counts))
            tracker.add_info("table_row_counts", table_row_counts)

            logger.success(f"GTFS data inserted into {schema_name} schema")
            logger.success(f"Total rows processed: {total_rows:,}")

        except Exception as e:
            logger.error(f"Error processing BODS Timetables data: {e}")
            raise
