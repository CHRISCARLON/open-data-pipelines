import time
import requests
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from loguru import logger
from tqdm import tqdm


def insert_batch_to_motherduck(
    batch_table: pa.Table, conn, schema: str, table: str, batch_num: int
) -> bool:
    """
    Insert Arrow table batch into MotherDuck with retry logic.

    Args:
        batch_table: Arrow table to insert
        conn: Database connection
        schema: Database schema name
        table: Table name
        batch_num: Batch number for logging

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
            conn.register("cadent_batch", batch_table)
            insert_sql = f'INSERT INTO "{schema}"."{table}" SELECT * FROM cadent_batch'
            conn.execute(insert_sql)

            if attempt > 0:
                logger.success(
                    f"Successfully inserted batch {batch_num} on attempt {attempt + 1}"
                )

            return True

        except Exception as e:
            try:
                conn.unregister("cadent_batch")
            except Exception:
                pass

            if attempt < max_retries - 1:
                wait_time = (2**attempt) * base_delay
                logger.warning(f"Batch {batch_num} attempt {attempt + 1} failed: {e}")
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logger.error(
                    f"All {max_retries} attempts failed for batch {batch_num}. Final error: {e}"
                )
                raise
        finally:
            try:
                conn.unregister("cadent_batch")
            except Exception:
                pass

    return False


def process_cadent_data(
    url: str, batch_size: int, motherduck_conn, schema_name: str, table_name: str
) -> None:
    """
    Download parquet in chunks, read complete file, then send to MotherDuck in batches with retry logic!
    """
    logger.info(f"Processing Cadent data from {url}")

    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0))

        buffer = BytesIO()
        downloaded = 0

        if total_size > 0:
            logger.info(f"File size: {total_size:,} bytes")
            pbar = tqdm(total=total_size, unit="B", unit_scale=True, desc="Downloading")
        else:
            logger.info("File size unknown, showing progress...")
            pbar = tqdm(unit="B", unit_scale=True, desc="Downloading")

        try:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    buffer.write(chunk)
                    chunk_size = len(chunk)
                    downloaded += chunk_size
                    pbar.update(chunk_size)
        finally:
            pbar.close()

        logger.success(f"Download complete: {downloaded:,} bytes")

        buffer.seek(0)
        logger.info("Reading parquet data...")

        arrow_buffer = pa.py_buffer(buffer.getvalue())
        buffer_reader = pa.BufferReader(arrow_buffer)
        table = pq.read_table(buffer_reader)

        total_rows = len(table)
        logger.success(
            f"Successfully read {total_rows:,} rows and {len(table.columns)} columns"
        )
        logger.info(f"Columns: {table.column_names[:5]}...")

        logger.info(f"Inserting data in batches of {batch_size:,} rows...")

        rows_inserted = 0
        batch_num = 1

        for start in range(0, total_rows, batch_size):
            end = min(start + batch_size, total_rows)

            batch_table = table.slice(start, end - start)

            success = insert_batch_to_motherduck(
                batch_table, motherduck_conn, schema_name, table_name, batch_num
            )

            if success:
                rows_inserted += len(batch_table)
                logger.info(
                    f"Inserted batch {batch_num}: {rows_inserted:,} / {total_rows:,} rows"
                )
            else:
                logger.error(f"Failed to insert batch {batch_num} after all retries")
                raise Exception(f"Batch {batch_num} insertion failed")

            batch_num += 1

        logger.success(
            f"Completed! Inserted {rows_inserted:,} total rows into MotherDuck"
        )

    except Exception as e:
        logger.error(f"Error processing Cadent data: {e}")
        raise
