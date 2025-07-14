import csv
import pandas as pd
from typing import Iterator, List, Dict, Tuple, Optional
import requests
from loguru import logger
from tqdm import tqdm
from stream_unzip import stream_unzip
import time


def insert_into_motherduck(df: pd.DataFrame, conn, schema: str, table: str) -> bool:
    """
    Insert DataFrame into MotherDuck with retry logic.

    Args:
        df: DataFrame to insert
        conn: Database connection
        schema: Database schema name
        table: Table name

    Returns:
        True if successful, False otherwise
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


def validate_column_names(
    header: List[str], expected_columns: Dict[str, str]
) -> Tuple[bool, List[str]]:
    """
    Validate that CSV header matches expected columns

    Args:
        header: List of column names from CSV
        expected_columns: Dict of expected column names and types

    Returns:
        (is_valid, list_of_issues)
    """
    expected_names = set(expected_columns.keys())
    actual_names = set(header)

    issues = []

    # Check for missing columns
    missing = expected_names - actual_names
    if missing:
        issues.append(f"Missing columns: {', '.join(sorted(missing))}")

    # Check for extra columns
    extra = actual_names - expected_names
    if extra:
        issues.append(f"Unexpected columns: {', '.join(sorted(extra))}")

    return len(issues) == 0, issues


def stream_csv_from_zip(
    zip_url: str, batch_size: int, expected_columns: Optional[Dict[str, str]] = None
) -> Iterator[pd.DataFrame]:
    """
    Stream CSV data from a ZIP file with optional column validation.

    Args:
        zip_url: URL of the ZIP file
        batch_size: Number of rows per batch
        expected_columns: Dict of expected column names and types for validation

    Yields:
        DataFrames containing batch_size rows
    """
    try:
        # Start streaming
        logger.info(f"Starting stream from {zip_url}")
        response = requests.get(zip_url, stream=True, timeout=30)
        response.raise_for_status()

        # Get file size for progress tracking
        total_size = int(response.headers.get("content-length", 0))

        # Stream data
        with tqdm(
            total=total_size, unit="B", unit_scale=True, desc="Streaming"
        ) as pbar:

            def chunked_response():
                for chunk in response.iter_content(chunk_size=1048576):
                    pbar.update(len(chunk))
                    yield chunk

            # Process ZIP as it streams
            for file_name, file_size, unzipped_chunks in stream_unzip(
                chunked_response()
            ):
                file_name_str = (
                    file_name.decode("utf-8")
                    if isinstance(file_name, bytes)
                    else file_name
                )

                # Only process CSV files
                if str(file_name_str).lower().endswith(".csv"):
                    logger.info(f"Processing CSV: {file_name_str}")

                    # Stream CSV processing
                    row_buffer = []
                    header = None
                    partial_line = ""

                    for chunk in unzipped_chunks:
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

                                # Validate columns if expected_columns provided
                                if expected_columns:
                                    is_valid, issues = validate_column_names(
                                        header, expected_columns
                                    )

                                    if not is_valid:
                                        logger.error(
                                            f"Column validation failed for {file_name_str}:"
                                        )
                                        for issue in issues:
                                            logger.error(f"  - {issue}")

                                        # Raise error to skip this file
                                        raise ValueError(
                                            f"Invalid columns in {file_name_str}"
                                        )
                                    else:
                                        logger.info(
                                            f"✓ Column validation passed for {file_name_str}"
                                        )

                            else:
                                try:
                                    values = next(csv.reader([line]))
                                    if len(values) == len(header):
                                        row_dict = dict(zip(header, values))
                                        row_buffer.append(row_dict)

                                        # Yield batch when full
                                        if len(row_buffer) >= batch_size:
                                            df_batch = pd.DataFrame(row_buffer)
                                            yield df_batch
                                            row_buffer = []
                                            logger.debug(
                                                f"Yielded batch of {batch_size} rows"
                                            )
                                except csv.Error as e:
                                    logger.warning(f"Error parsing CSV line: {e}")
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

                    # Yield remaining rows
                    if row_buffer:
                        df_batch = pd.DataFrame(row_buffer)
                        yield df_batch
                        logger.debug(f"Yielded final batch of {len(row_buffer)} rows")

    except Exception as e:
        logger.error(f"Error streaming ZIP file {zip_url}: {e}")
        raise


def process_streaming_data(
    url: str,
    batch_size: int,
    conn,
    schema_name: str,
    table_name: str,
    expected_columns: Optional[Dict[str, str]] = None,
) -> None:
    """
    Process CSV data from ZIP URL using true streaming.

    Args:
        url: URL of the ZIP file
        batch_size: Batch size for processing
        conn: Database connection
        schema_name: Schema name
        table_name: Table name
        expected_columns: Dict of expected column names and types for validation
    """
    total_rows = 0
    batch_count = 0
    errors = []

    try:
        logger.info(f"Starting streaming process for {url}")

        # Process streamed batches
        for df_batch in stream_csv_from_zip(url, batch_size, expected_columns):
            batch_count += 1
            batch_rows = len(df_batch)

            try:
                # Convert all to strings
                df_batch = df_batch.astype(str).replace("nan", None)

                insert_into_motherduck(df_batch, conn, schema_name, table_name)

                total_rows += batch_rows
                logger.info(
                    f"Processed batch {batch_count} ({batch_rows} rows, {total_rows} total)"
                )

            except Exception as e:
                error_msg = f"Error processing batch {batch_count}: {e}"
                logger.error(error_msg)
                errors.append(error_msg)
                # Continue processing other batches

        if total_rows == 0:
            logger.warning(f"No data found in {url}")
        else:
            logger.success(
                f"Completed processing {table_name} with {total_rows} rows in {batch_count} batches"
            )

    except Exception as e:
        error_msg = f"Error processing {url}: {e}"
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


def process_bduk(
    download_links: List[str],
    table_names: List[str],
    batch_size: int,
    conn,
    schema_name: str,
    expected_columns: Optional[Dict[str, str]] = None,
) -> None:
    """
    Process multiple BDUK files.

    Args:
        download_links: List of ZIP URLs
        table_names: List of table names
        batch_size: Batch size for processing
        conn: Database connection
        schema_name: Schema name
        expected_columns: Dict of expected column names and types for validation
    """
    if len(download_links) != len(table_names):
        raise ValueError("Number of download links must match number of table names")

    # Process each file
    for zip_url, table_name in zip(download_links, table_names):
        logger.info(f"Processing {table_name}")

        try:
            process_streaming_data(
                url=zip_url,
                batch_size=batch_size,
                conn=conn,
                schema_name=schema_name,
                table_name=table_name,
                expected_columns=expected_columns,
            )
        except Exception as e:
            logger.error(f"Failed to process {table_name}: {e}")
            continue
