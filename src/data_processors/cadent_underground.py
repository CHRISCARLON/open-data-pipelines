import csv
import os
import time
import json
import requests
import pandas as pd
from typing import Iterator, Optional, Dict, List, Tuple
from loguru import logger
from tqdm import tqdm
from shapely.geometry import shape, Point


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

    missing = expected_names - actual_names
    if missing:
        issues.append(f"Missing columns: {', '.join(sorted(missing))}")

    extra = actual_names - expected_names
    if extra:
        issues.append(f"Unexpected columns: {', '.join(sorted(extra))}")

    return len(issues) == 0, issues


def stream_csv_from_url(
    csv_url: str,
    batch_size: int,
    expected_columns: Optional[Dict[str, str]] = None,
    api_key_param: str = "apikey",
) -> Iterator[pd.DataFrame]:
    """
    Stream CSV data directly from URL with optional column validation.
    Adds API key from CADENT_API_KEY environment variable.

    Args:
        csv_url: URL of the CSV file
        batch_size: Number of rows per batch
        expected_columns: Dict of expected column names and types for validation
        api_key_param: Query parameter name for API key

    Yields:
        DataFrames containing batch_size rows
    """
    try:
        api_key = os.getenv("CADENT_API_KEY")
        if not api_key:
            raise ValueError("CADENT_API_KEY environment variable not set")

        separator = "&" if "?" in csv_url else "?"
        url_with_key = f"{csv_url}{separator}{api_key_param}={api_key}"

        logger.info(f"Starting stream from {csv_url}")
        response = requests.get(url_with_key, stream=True, timeout=30)
        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0))
        logger.info(f"File size: {total_size:,} bytes")

        row_buffer = []
        header = None
        partial_line = ""

        with tqdm(
            total=total_size, unit="B", unit_scale=True, desc="Streaming"
        ) as pbar:
            for chunk in response.iter_content(chunk_size=1048576):
                pbar.update(len(chunk))

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

                        if header and header[0].startswith("\ufeff"):
                            header[0] = header[0].replace("\ufeff", "")
                            logger.info("Stripped BOM from first column")

                        logger.info(f"CSV Headers: {header}")
                        logger.debug(f"Header repr: {[repr(h) for h in header]}")

                        if expected_columns:
                            logger.info(
                                f"Expected columns: {list(expected_columns.keys())}"
                            )
                            is_valid, issues = validate_column_names(
                                header, expected_columns
                            )

                            if not is_valid:
                                logger.error("Column validation failed:")
                                for issue in issues:
                                    logger.error(f"  - {issue}")
                                logger.warning("Proceeding despite column mismatch")
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
                                    yield df_batch
                                    row_buffer = []
                                    logger.debug(f"Yielded batch of {batch_size} rows")
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
) -> int:
    """
    Process CSV data from URL using true streaming.

    Args:
        url: URL of the CSV file
        batch_size: Batch size for processing
        conn: Database connection
        schema_name: Schema name
        table_name: Table name
        expected_columns: Dict of expected column names and types for validation

    Returns:
        Total rows processed
    """
    total_rows = 0
    batch_count = 0
    errors = []

    try:
        logger.info(f"Starting streaming process for {url}")

        for df_batch in stream_csv_from_url(url, batch_size, expected_columns):
            batch_count += 1
            batch_rows = len(df_batch)

            try:
                if "Geo Point" in df_batch.columns:

                    def point_to_wkt(point_str):
                        if pd.isna(point_str) or not point_str:
                            return None
                        try:
                            parts = point_str.split(",")
                            if len(parts) == 2:
                                lat = float(parts[0].strip())
                                lon = float(parts[1].strip())
                                return Point(lon, lat).wkt
                        except Exception:
                            pass
                        return None

                    df_batch["geo_point_wkt"] = df_batch["Geo Point"].apply(
                        point_to_wkt
                    )

                if "Geo Shape" in df_batch.columns:

                    def geojson_to_wkt(geojson_str):
                        if pd.isna(geojson_str) or not geojson_str:
                            return None
                        try:
                            geojson_obj = json.loads(geojson_str)
                            geom = shape(geojson_obj)
                            return geom.wkt
                        except Exception:
                            pass
                        return None

                    df_batch["geo_shape_wkt"] = df_batch["Geo Shape"].apply(
                        geojson_to_wkt
                    )

                # Log sample geometry data after first conversion - just to check it works
                if batch_count == 1 and len(df_batch) > 0:
                    logger.info("Sample geometry conversion:")
                    logger.info(
                        f"DataFrame columns after WKT conversion: {list(df_batch.columns)}"
                    )
                    sample_row = df_batch.iloc[0]
                    if (
                        "Geo Point" in df_batch.columns
                        and "geo_point_wkt" in df_batch.columns
                    ):
                        logger.info(
                            f"  Geo Point (original): {sample_row['Geo Point'][:100]}..."
                        )
                        logger.info(f"  geo_point_wkt: {sample_row['geo_point_wkt']}")
                    if (
                        "Geo Shape" in df_batch.columns
                        and "geo_shape_wkt" in df_batch.columns
                    ):
                        logger.info(
                            f"  Geo Shape (original): {sample_row['Geo Shape'][:150]}..."
                        )
                        logger.info(
                            f"  geo_shape_wkt: {sample_row['geo_shape_wkt'][:200]}..."
                        )

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

        return total_rows

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


def process_cadent_data(
    url: str,
    batch_size: int,
    motherduck_conn,
    schema_name: str,
    table_name: str,
    expected_columns: Optional[Dict[str, str]] = None,
) -> None:
    """
    Process Cadent CSV data from URL.

    Args:
        url: URL to the CSV file
        batch_size: Number of rows per batch
        motherduck_conn: Database connection
        schema_name: Database schema name
        table_name: Table name
        expected_columns: Dict of expected column names and types for validation
    """
    logger.info(f"Processing {table_name} from {url}")

    try:
        total_rows = process_streaming_csv(
            url=url,
            batch_size=batch_size,
            conn=motherduck_conn,
            schema_name=schema_name,
            table_name=table_name,
            expected_columns=expected_columns,
        )

        logger.success(f"Completed processing {table_name}")
        logger.info(f"Total rows: {total_rows:,}")

    except Exception as e:
        logger.error(f"Failed to process {table_name}: {e}")
        raise
