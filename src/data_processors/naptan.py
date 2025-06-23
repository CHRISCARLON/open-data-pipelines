import csv
import pandas as pd
from typing import Iterator, List, Dict, Tuple, Optional
import requests
from loguru import logger
from tqdm import tqdm
from data_processors.utils.data_processor_utils import insert_table
from data_sources.data_source_config import DataProcessorType


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


def clean_naptan_data(df: pd.DataFrame, expected_columns: Dict[str, str]) -> pd.DataFrame:
    """
    Clean NAPTAN data - set problematic values to NULL.
    
    Args:
        df: DataFrame to clean
        expected_columns: Dict of column names and their expected types
        
    Returns:
        Cleaned DataFrame
    """
    df_cleaned = df.copy()
    
    # Handle different data types - when in doubt, set to NULL
    for col, dtype in expected_columns.items():
        if col in df_cleaned.columns:
            if dtype in ['DOUBLE', 'BIGINT']:
                
                df_cleaned[col] = df_cleaned[col].replace(['', 'nan', 'NaN'], None)
                try:
                    df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors='coerce')
                except Exception:
                    logger.warning(f"Setting numeric column {col} to NULL due to conversion issues")
                    df_cleaned[col] = None
                    
            elif dtype == 'TIMESTAMP':
                logger.debug(f"Setting timestamp column {col} to NULL")
                df_cleaned[col] = None
                    
            else:  
                df_cleaned[col] = df_cleaned[col].astype(str).replace(['nan', 'NaN', 'None'], None)
    
    logger.debug(f"Cleaned DataFrame shape: {df_cleaned.shape}")
    return df_cleaned


def stream_csv_from_url(
    csv_url: str, batch_size: int, expected_columns: Optional[Dict[str, str]] = None
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
        logger.info(f"Starting stream from {csv_url}")
        
        # Start streaming with a larger timeout for NAPTAN
        response = requests.get(csv_url, stream=True, timeout=60)
        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0))

        # Stream data
        with tqdm(
            total=total_size, unit="B", unit_scale=True, desc="Streaming CSV"
        ) as pbar:
            
            row_buffer = []
            header = None
            partial_line = ""
            
            for chunk in response.iter_content(chunk_size=1048576):
                if chunk:
                    pbar.update(len(chunk))
                    
                    try:
                        text_chunk = chunk.decode('utf-8')
                    except UnicodeDecodeError:
                        text_chunk = chunk.decode('utf-8', errors='ignore')
                    
                    text_chunk = partial_line + text_chunk
                    lines = text_chunk.split('\n')
                    partial_line = lines[-1]
                    lines = lines[:-1]
                    
                    for line in lines:
                        if not line.strip():
                            continue
                            
                        if header is None:
                            header = next(csv.reader([line]))
                            
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
                                    
                                    # Yield batch when full
                                    if len(row_buffer) >= batch_size:
                                        df_batch = pd.DataFrame(row_buffer)
                                        
                                        # Clean the data if expected_columns provided
                                        if expected_columns:
                                            df_batch = clean_naptan_data(df_batch, expected_columns)
                                        
                                        yield df_batch
                                        row_buffer = []
                                        logger.debug(f"Yielded batch of {batch_size} rows")
                                        
                            except csv.Error as e:
                                logger.warning(f"Error parsing CSV line: {e}")
                                continue
            
            # Process final partial line if exists
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
                
                # Clean the data if expected_columns provided
                if expected_columns:
                    df_batch = clean_naptan_data(df_batch, expected_columns)
                    
                yield df_batch
                logger.debug(f"Yielded final batch of {len(row_buffer)} rows")

    except Exception as e:
        logger.error(f"Error streaming CSV file {csv_url}: {e}")
        raise


def process_streaming_data(
    url: str,
    batch_size: int,
    conn,
    schema_name: str,
    table_name: str,
    processor_type: DataProcessorType,
    expected_columns: Optional[Dict[str, str]] = None,
) -> None:
    """
    Process CSV data from URL using true streaming.

    Args:
        url: URL of the CSV file
        batch_size: Batch size for processing
        conn: Database connection
        schema_name: Schema name
        table_name: Table name
        processor_type: Type of processor (MotherDuck or PostgreSQL)
        expected_columns: Dict of expected column names and types for validation
    """
    total_rows = 0
    batch_count = 0
    errors = []

    try:
        logger.info(f"Starting streaming process for {url}")

        # Process streamed batches
        for df_batch in stream_csv_from_url(url, batch_size, expected_columns):
            batch_count += 1
            batch_rows = len(df_batch)

            try:
                insert_table(df_batch, conn, schema_name, table_name, processor_type)

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


def process_naptan(
    download_link: str,
    table_name: str,
    batch_size: int,
    conn,
    schema_name: str,
    processor_type: DataProcessorType,
    expected_columns: Optional[Dict[str, str]] = None,
) -> None:
    """
    Process NAPTAN CSV file.

    Args:
        download_link: CSV URL
        table_name: Table name
        batch_size: Batch size for processing
        conn: Database connection
        schema_name: Schema name
        processor_type: Type of processor (MotherDuck or PostgreSQL)
        expected_columns: Dict of expected column names and types for validation
    """
    logger.info(f"Processing NAPTAN data: {table_name}")

    try:
        process_streaming_data(
            url=download_link,
            batch_size=batch_size,
            conn=conn,
            schema_name=schema_name,
            table_name=table_name,
            processor_type=processor_type,
            expected_columns=expected_columns,
        )
    except Exception as e:
        logger.error(f"Failed to process {table_name}: {e}")
        raise


def process_data(
    download_link: str,
    table_name: str,
    batch_size: int,
    conn,
    schema_name: str,
    processor_type: DataProcessorType,
    expected_columns: Optional[Dict[str, str]] = None,
) -> None:
    """
    Main function to process NAPTAN data with streaming.

    Args:
        download_link: The URL to fetch the CSV data from
        table_name: The name of the table
        batch_size: Batch size for processing
        conn: The database connection (MotherDuck or PostgreSQL)
        schema_name: The schema of the table
        processor_type: Type of processor (MotherDuck or PostgreSQL)
        expected_columns: Dict of expected column names and types for validation
    """
    logger.info(f"Starting NAPTAN data processing from {download_link} for {processor_type}")
    
    try:
        process_naptan(
            download_link=download_link,
            table_name=table_name,
            batch_size=batch_size,
            conn=conn,
            schema_name=schema_name,
            processor_type=processor_type,
            expected_columns=expected_columns,
        )
        logger.success("NAPTAN data processing completed successfully")
    except Exception as e:
        logger.error(f"Error processing NAPTAN data: {e}")
        raise
