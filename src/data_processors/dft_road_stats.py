import requests
import os
import tempfile
import pandas as pd

from loguru import logger
from tqdm import tqdm
from typing import Optional, Dict, Iterator
from stream_unzip import stream_unzip

from ..data_processors.utils.metadata_logger import metadata_tracker
from ..data_processors.utils.data_processor_utils import insert_into_motherduck
from ..data_sources.data_source_config import DataSourceConfig


def stream_file_from_url(url: str) -> Iterator[bytes]:
    """
    Stream file from URL without loading entire file into memory.

    Args:
        url: URL to stream from

    Yields:
        Chunks of bytes from the file
    """
    try:
        logger.info(f"Starting stream from {url}")
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()

        for chunk in response.iter_content(chunk_size=1048576):  # 1MB chunks
            if chunk:
                yield chunk

    except Exception as e:
        logger.error(f"Error streaming file from {url}: {e}")
        raise


def read_ods_file(file_path: str, sheet_name: Optional[str] = None, header_row: int = 6) -> pd.DataFrame:
    """
    Read ODS file into a pandas DataFrame.

    Args:
        file_path: Path to ODS file
        sheet_name: Optional sheet name to read. Defaults to first sheet (0) if not provided.
        header_row: Row index (0-based) where the column headers are located. Default is 6 (row 7).

    Returns:
        DataFrame containing the data
    """
    try:
        logger.info(f"Reading ODS file: {file_path}")

        selected_sheet: str | int = sheet_name if sheet_name else 0
        logger.info(f"Using sheet: {selected_sheet}, header row: {header_row}")

        # DFT Road Stats files have headers at different rows depending on the file
        result = pd.read_excel(file_path, sheet_name=selected_sheet, engine="odf", header=header_row)

        if not isinstance(result, pd.DataFrame):
            raise TypeError(f"Expected DataFrame, got {type(result)}")

        logger.success(f"Read {len(result):,} rows and {len(result.columns)} columns from ODS file")
        return result

    except Exception as e:
        logger.error(f"Error reading ODS file {file_path}: {e}")
        raise


def clean_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean DataFrame column names and data for database insertion.

    Args:
        df: DataFrame to clean

    Returns:
        Cleaned DataFrame
    """
    df_clean = df.copy()

    # Clean column names: lowercase, replace spaces with underscores
    df_clean.columns = (
        df_clean.columns.str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace("-", "_", regex=False)
        .str.replace("(", "", regex=False)
        .str.replace(")", "", regex=False)
        .str.replace("/", "_", regex=False)
        .str.replace("'", "", regex=False)  # Remove quotes around letters
        .str.rstrip("_")  # Remove trailing underscores
    )

    # Convert all columns to string type
    for col in df_clean.columns:
        df_clean[col] = df_clean[col].astype(str).replace(["nan", "None", "NaN"], None)

    return df_clean


def process_ods_file(
    file_path: str,
    conn,
    schema_name: str,
    table_name: str,
    batch_size: int = 10000,
    tracker=None,
    sheet_name: Optional[str] = None,
    header_row: int = 6,
) -> int:
    """
    Process a single ODS file and insert into MotherDuck.

    Args:
        file_path: Path to the ODS file
        conn: Database connection
        schema_name: Schema name
        table_name: Table name
        batch_size: Number of rows per batch
        tracker: Optional metadata tracker
        sheet_name: Optional sheet name to read from the ODS file
        header_row: Row index (0-based) where the column headers are located

    Returns:
        Number of rows processed
    """
    try:
        df = read_ods_file(file_path, sheet_name=sheet_name, header_row=header_row)
        df_clean = clean_dataframe_columns(df)

        total_rows = len(df_clean)
        logger.info(f"Processing {total_rows:,} rows in batches of {batch_size}")
        logger.info(f"DataFrame columns ({len(df_clean.columns)}): {list(df_clean.columns)}")

        if tracker:
            tracker.set_rows_processed(total_rows)
            tracker.add_info("total_rows", total_rows)
            tracker.add_info("batch_size", batch_size)
            tracker.add_info("columns", list(df_clean.columns))

        rows_processed = 0
        for start_idx in range(0, total_rows, batch_size):
            end_idx = min(start_idx + batch_size, total_rows)
            batch = df_clean.iloc[start_idx:end_idx]

            insert_into_motherduck(batch, conn, schema_name, table_name)

            rows_processed += len(batch)
            logger.info(f"Processed {rows_processed:,}/{total_rows:,} rows")

        logger.success(f"Successfully processed {table_name} with {rows_processed:,} rows")
        return rows_processed

    except Exception as e:
        logger.error(f"Error processing ODS file {file_path}: {e}")
        raise


def process_single_ods_file(
    url: str,
    conn,
    schema_name: str,
    table_name: str,
    batch_size: int,
    config: Optional[DataSourceConfig] = None,
    sheet_name: Optional[str] = None,
    header_row: int = 6,
) -> int:
    """
    Stream and process a single ODS file.

    Args:
        url: URL of the ODS file
        conn: Database connection
        schema_name: Schema name
        table_name: Table name
        batch_size: Batch size for processing
        config: Data source configuration (enables metadata logging if provided)
        sheet_name: Optional sheet name to read from the ODS file
        header_row: Row index (0-based) where the column headers are located

    Returns:
        Number of rows processed
    """
    if config:
        with metadata_tracker(config, conn, url) as tracker:
            try:
                tracker.add_info("file_format", "ods")
                tracker.add_info("url", url)

                with tempfile.NamedTemporaryFile(suffix=".ods", delete=False) as tmp_file:
                    total_bytes = 0
                    for chunk in stream_file_from_url(url):
                        tmp_file.write(chunk)
                        total_bytes += len(chunk)
                    tmp_path = tmp_file.name

                tracker.set_file_size(total_bytes)
                logger.info(f"Streamed {total_bytes:,} bytes to temp file")

                try:
                    rows_processed = process_ods_file(
                        tmp_path, conn, schema_name, table_name, batch_size, tracker, sheet_name, header_row
                    )
                    return rows_processed
                finally:
                    # Clean up temp file
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)

            except Exception as e:
                logger.error(f"Error processing ODS file from {url}: {e}")
                raise
    else:
        with tempfile.NamedTemporaryFile(suffix=".ods", delete=False) as tmp_file:
            for chunk in stream_file_from_url(url):
                tmp_file.write(chunk)
            tmp_path = tmp_file.name

        try:
            return process_ods_file(tmp_path, conn, schema_name, table_name, batch_size, None, sheet_name, header_row)
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)


def process_zip_with_ods_files(
    url: str,
    conn,
    schema_name: str,
    table_prefix: str,
    batch_size: int,
    config: Optional[DataSourceConfig] = None,
) -> Dict[str, int]:
    """
    Stream and process a ZIP file containing ODS files using stream_unzip.

    Args:
        url: URL of the ZIP file
        conn: Database connection
        schema_name: Schema name
        table_prefix: Prefix for table names (file name will be appended)
        batch_size: Batch size for processing
        config: Data source configuration (enables metadata logging if provided)

    Returns:
        Dictionary mapping table names to rows processed
    """
    results = {}

    if config:
        with metadata_tracker(config, conn, url) as tracker:
            try:
                tracker.add_info("file_format", "zip")
                tracker.add_info("url", url)

                logger.info(f"Starting ZIP stream from {url}")
                response = requests.get(url, stream=True, timeout=30)
                response.raise_for_status()

                file_size = int(response.headers.get("content-length", 0))
                if file_size > 0:
                    tracker.set_file_size(file_size)
                    logger.info(f"ZIP file size: {file_size:,} bytes")

                zipped_chunks = response.iter_content(chunk_size=1048576)
                ods_files_found = 0

                with tempfile.TemporaryDirectory() as temp_dir:
                    for file_name, file_size, unzipped_chunks in tqdm(
                        stream_unzip(zipped_chunks), desc="Extracting ZIP"
                    ):
                        file_name_str = (
                            file_name.decode("utf-8")
                            if isinstance(file_name, bytes)
                            else file_name
                        )

                        if not file_name_str.endswith(".ods"):
                            continue

                        ods_files_found += 1
                        logger.info(f"Found ODS file in ZIP: {file_name_str}")

                        temp_ods_path = os.path.join(temp_dir, file_name_str)
                        os.makedirs(os.path.dirname(temp_ods_path), exist_ok=True)

                        with open(temp_ods_path, "wb") as f:
                            for chunk in unzipped_chunks:
                                f.write(chunk)

                        base_name = os.path.splitext(os.path.basename(file_name_str))[0]
                        table_name = f"{table_prefix}_{base_name}".lower()

                        logger.info(f"Processing {base_name} -> {table_name}")

                        rows = process_ods_file(
                            temp_ods_path, conn, schema_name, table_name, batch_size, tracker
                        )
                        results[table_name] = rows

                tracker.add_info("ods_files_found", ods_files_found)
                tracker.add_info("tables_created", list(results.keys()))
                tracker.set_rows_processed(sum(results.values()))

                logger.success(
                    f"Processed {ods_files_found} ODS files from ZIP with {sum(results.values()):,} total rows"
                )

            except Exception as e:
                logger.error(f"Error processing ZIP file from {url}: {e}")
                raise
    else:
        logger.info(f"Starting ZIP stream from {url}")
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()

        zipped_chunks = response.iter_content(chunk_size=1048576)

        with tempfile.TemporaryDirectory() as temp_dir:
            for file_name, file_size, unzipped_chunks in tqdm(
                stream_unzip(zipped_chunks), desc="Extracting ZIP"
            ):
                file_name_str = (
                    file_name.decode("utf-8") if isinstance(file_name, bytes) else file_name
                )

                if not file_name_str.endswith(".ods"):
                    continue

                temp_ods_path = os.path.join(temp_dir, file_name_str)
                os.makedirs(os.path.dirname(temp_ods_path), exist_ok=True)

                with open(temp_ods_path, "wb") as f:
                    for chunk in unzipped_chunks:
                        f.write(chunk)

                base_name = os.path.splitext(os.path.basename(file_name_str))[0]
                table_name = f"{table_prefix}_{base_name}".lower()

                rows = process_ods_file(temp_ods_path, conn, schema_name, table_name, batch_size)
                results[table_name] = rows

    return results


def process_dft_road_stats(
    download_links: Dict[str, str],
    conn,
    schema_name: str,
    batch_size: int,
    config: Optional[DataSourceConfig] = None,
    sheet_names: Optional[Dict[str, str]] = None,
    header_rows: Optional[Dict[str, int]] = None,
) -> None:
    """
    Process DFT Road Stats data files (ODS or ZIP files).

    Args:
        download_links: Dictionary mapping file codes to URLs
        conn: Database connection
        schema_name: Schema name
        batch_size: Batch size for processing
        config: Data source configuration (enables metadata logging if provided)
        sheet_names: Optional dictionary mapping file codes to sheet names
        header_rows: Optional dictionary mapping file codes to header row indices (0-based)
    """
    for file_code, url in download_links.items():
        logger.info(f"Processing {file_code} from {url}")

        sheet_name = sheet_names.get(file_code) if sheet_names else None
        if sheet_name:
            logger.info(f"Using sheet name: {sheet_name}")

        header_row = header_rows.get(file_code, 6) if header_rows else 6

        try:
            if url.endswith(".zip"):
                results = process_zip_with_ods_files(
                    url, conn, schema_name, file_code, batch_size, config
                )
                logger.success(
                    f"Completed {file_code}: processed {len(results)} tables with {sum(results.values()):,} total rows"
                )
            elif url.endswith(".ods"):
                rows = process_single_ods_file(
                    url, conn, schema_name, file_code, batch_size, config, sheet_name, header_row
                )
                logger.success(f"Completed {file_code}: {rows:,} rows")
            else:
                logger.warning(f"Unsupported file type for {url}, skipping")

        except Exception as e:
            logger.error(f"Failed to process {file_code}: {e}")
            # Continue with next file
            continue
