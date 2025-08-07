import csv
import pandas as pd
from typing import Iterator, List, Dict, Tuple, Optional
import requests
from loguru import logger
from tqdm import tqdm
import time
from ..data_processors.utils.metadata_logger import metadata_tracker
from ..data_sources.data_source_config import DataSourceConfig


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

    Returns Tuple[bool, List[str]]:
        (is_valid, list_of_issues)
    """
    expected_names = set(expected_columns.keys())
    actual_names = set(header)

    issues = []

    missing = expected_names - actual_names
    if missing:
        issues.append(f"Missing columns: {', '.join(sorted(missing))}")

    extra = actual_names - expected_names
    if extra:
        issues.append(f"Unexpected columns: {', '.join(sorted(extra))}")

    return len(issues) == 0, issues


def clean_dataframe_for_motherduck(
    df: pd.DataFrame, expected_columns: Dict[str, str]
) -> pd.DataFrame:
    """Clean DataFrame and rename columns to SQL-safe names."""
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
                df_clean[col] = pd.to_numeric(df_clean[col], errors="coerce").astype(
                    "Int64"
                )
            elif dtype == "DOUBLE":
                df_clean[col] = pd.to_numeric(df_clean[col], errors="coerce")

    string_columns = [col for col in df_clean.columns if col not in numeric_columns]
    for col in string_columns:
        df_clean[col] = df_clean[col].astype(str).replace(["nan", "NaN", "null"], None)

    return df_clean


def stream_csv_from_url(
    csv_url: str,
    batch_size: int,
    expected_columns: Optional[Dict[str, str]] = None,
    tracker=None,
) -> Iterator[pd.DataFrame]:
    """
    Stream CSV data directly from a URL with optional column validation.

    Args:
        csv_url: URL of the CSV file
        batch_size: Number of rows per batch
        expected_columns: Dict of expected column names and types for validation

    Yields:
        DataFrames containing batch_size rows
    """
    try:
        logger.info(f"Starting CSV stream from {csv_url}")
        response = requests.get(csv_url, stream=True, timeout=30)
        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0))

        row_buffer = []
        header = None
        partial_line = ""

        if tracker:
            tracker.set_file_size(total_size)

        with tqdm(
            total=total_size, unit="B", unit_scale=True, desc="Streaming CSV"
        ) as pbar:
            for chunk in response.iter_content(chunk_size=1048576):
                if chunk:
                    pbar.update(len(chunk))

                    try:
                        text_chunk = chunk.decode("utf-8", errors="ignore")
                    except UnicodeDecodeError:
                        logger.warning("Failed to decode chunk as UTF-8, skipping")
                        continue

                    text_chunk = partial_line + text_chunk
                    lines = text_chunk.split("\n")
                    partial_line = lines[-1]
                    lines = lines[:-1]

                    for line in lines:
                        if not line.strip():
                            continue

                        if header is None:
                            # Parse header
                            header = next(csv.reader([line]))
                            logger.info(f"Found {len(header)} columns: {header[:5]}...")

                            # Validate columns if expected_columns provided
                            if expected_columns:
                                is_valid, issues = validate_column_names(
                                    header, expected_columns
                                )

                                if not is_valid:
                                    logger.error("Column validation failed:")
                                    for issue in issues:
                                        logger.error(f"  - {issue}")
                                    raise ValueError("Invalid columns in CSV")
                                else:
                                    logger.info("✓ Column validation passed")

                        else:
                            try:
                                values = next(csv.reader([line]))
                                if len(values) == len(header):
                                    row_dict = dict(zip(header, values))
                                    row_buffer.append(row_dict)

                                    if len(row_buffer) >= batch_size:
                                        df_batch = pd.DataFrame(row_buffer)
                                        if expected_columns:
                                            df_batch = clean_dataframe_for_motherduck(
                                                df_batch, expected_columns
                                            )
                                        yield df_batch
                                        row_buffer = []
                                        logger.debug(
                                            f"Yielded batch of {batch_size} rows"
                                        )
                                else:
                                    logger.warning(
                                        f"Row has {len(values)} values but header has {len(header)} columns"
                                    )
                            except csv.Error as e:
                                logger.warning(f"Error parsing CSV line: {e}")
                                continue

            if partial_line.strip() and header:
                try:
                    values = next(csv.reader([partial_line]))
                    if len(values) == len(header):
                        row_dict = dict(zip(header, values))
                        row_buffer.append(row_dict)
                except csv.Error:
                    pass

            if row_buffer:
                df_batch = pd.DataFrame(row_buffer)
                if expected_columns:
                    df_batch = clean_dataframe_for_motherduck(
                        df_batch, expected_columns
                    )
                yield df_batch
                logger.debug(f"Yielded final batch of {len(row_buffer)} rows")

    except Exception as e:
        logger.error(f"Error streaming CSV file {csv_url}: {e}")
        raise


def process_streaming_csv(
    url: str,
    batch_size: int,
    conn,
    schema_name: str,
    table_name: str,
    expected_columns: Optional[Dict[str, str]] = None,
    tracker=None,
) -> Tuple[int, int]:
    """
    Process CSV data directly from URL using streaming.

    Args:
        url: URL of the CSV file
        batch_size: Batch size for processing
        conn: Database connection
        schema_name: Schema name
        table_name: Table name
        expected_columns: Dict of expected column names and types for validation
        tracker: Optional metadata tracker

    Returns:
        Tuple of (total_rows_processed, file_size_bytes)
    """
    total_rows = 0
    batch_count = 0
    errors = []
    file_size = 0

    if tracker:
        tracker.add_info("url", url)
        tracker.add_info("batch_size", batch_size)
        tracker.add_info("schema_name", schema_name)
        tracker.add_info("table_name", table_name)
        tracker.add_info("expected_columns", expected_columns)
        tracker.add_info("errors", errors)

    try:
        logger.info(f"Starting streaming process for {url}")

        # Process streamed batches
        for df_batch in stream_csv_from_url(url, batch_size, expected_columns, tracker):
            batch_count += 1
            batch_rows = len(df_batch)

            try:
                insert_into_motherduck(df_batch, conn, schema_name, table_name)

                total_rows += batch_rows
                logger.info(
                    f"Processed batch {batch_count} ({batch_rows} rows, {total_rows} total)"
                )

                if tracker and batch_count % 10 == 0:
                    tracker.add_info("batches_processed", batch_count)
                    tracker.add_info("current_total_rows", total_rows)

            except Exception as e:
                error_msg = f"Error processing batch {batch_count}: {e}"
                logger.error(error_msg)
                errors.append(error_msg)

        if total_rows == 0:
            logger.warning(f"No data found in {url}")
        else:
            logger.success(
                f"Completed processing {table_name} with {total_rows} rows in {batch_count} batches"
            )

        if tracker:
            tracker.add_info("total_batches", batch_count)
            tracker.add_info("errors_count", len(errors))

        return total_rows, file_size

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


def process_post_code_p002(
    download_links: List[str],
    table_names: List[str],
    batch_size: int,
    conn,
    schema_name: str,
    expected_columns: Optional[Dict[str, str]] = None,
    config: Optional[DataSourceConfig] = None,
) -> None:
    """
    Process Postcode P002 CSV files with metadata tracking.

    Args:
        download_links: List of CSV URLs
        table_names: List of table names
        batch_size: Batch size for processing
        conn: Database connection
        schema_name: Schema name
        expected_columns: Dict of expected column names and types for validation
        config: Data source configuration (enables metadata logging if provided)
    """
    if len(download_links) != len(table_names):
        raise ValueError("Number of download links must match number of table names")

    for csv_url, table_name in zip(download_links, table_names):
        logger.info(f"Processing {table_name} from {csv_url}")

        if config:
            with metadata_tracker(config, conn, csv_url) as tracker:
                try:
                    total_rows, file_size = process_streaming_csv(
                        url=csv_url,
                        batch_size=batch_size,
                        conn=conn,
                        schema_name=schema_name,
                        table_name=table_name,
                        expected_columns=expected_columns,
                        tracker=tracker,
                    )

                    tracker.set_rows_processed(total_rows)
                    tracker.set_file_size(file_size)
                    tracker.add_info("batch_size", batch_size)
                    tracker.add_info("table_name", table_name)
                    tracker.add_info("file_format", "csv")

                    logger.success(f"Completed processing table: {table_name}")

                except Exception as e:
                    logger.error(f"Failed to process {table_name}: {e}")
                    raise
        else:
            logger.warning("No config provided - metadata logging disabled")
            try:
                total_rows, file_size = process_streaming_csv(
                    url=csv_url,
                    batch_size=batch_size,
                    conn=conn,
                    schema_name=schema_name,
                    table_name=table_name,
                    expected_columns=expected_columns,
                )

                logger.success(f"Completed processing table: {table_name}")

            except Exception as e:
                logger.error(f"Failed to process {table_name}: {e}")
                raise
