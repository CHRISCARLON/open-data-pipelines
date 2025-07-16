import pandas as pd
import json
import uuid
from contextlib import contextmanager
from datetime import datetime
from typing import Any
from loguru import logger
from ...data_sources.data_source_config import DataSourceConfig
from ...data_processors.utils.data_processor_utils import insert_table


class MetadataTracker:
    """
    Tracks processing metadata during data pipeline execution.
    """

    def __init__(self):
        self.rows_processed = 0
        self.file_size_bytes = 0
        self.additional_info = {}

    def set_rows_processed(self, count: int):
        """Record how many rows were processed."""
        self.rows_processed = count

    def set_file_size(self, size: int):
        """Record the size of the processed file."""
        self.file_size_bytes = size

    def add_info(self, key: str, value: Any):
        """Add any additional information to track."""
        self.additional_info[key] = value


@contextmanager
def metadata_tracker(config: DataSourceConfig, conn, url: str):
    """
    Context manager that automatically logs processing metadata.

    Usage:
        with metadata_tracker(config, conn, url) as tracker:
            # Do your data processing
            df = fetch_swa_codes(url)
            tracker.set_rows_processed(len(df))
            # Processing continues...
    """

    start_time = datetime.now()

    log_data = {
        "log_id": str(uuid.uuid4()),
        "data_source": config.source_type.code,
        "schema_name": config.schema_name,
        "table_name": config.table_names[0] if config.table_names else "unknown",
        "processor_type": config.processor_type.value,
        "url": url,
        "start_time": start_time,
        "end_time": None,
        "duration_seconds": None,
        "rows_processed": 0,
        "file_size_bytes": 0,
        "status": "STARTED",
        "error_message": None,
        "additional_info": {},
        "created_at": start_time,
    }

    tracker = MetadataTracker()

    try:
        logger.info(f"Starting metadata tracking for {config.source_type.code}")
        yield tracker

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        log_data.update(
            {
                "end_time": end_time,
                "duration_seconds": duration,
                "rows_processed": tracker.rows_processed,
                "file_size_bytes": tracker.file_size_bytes,
                "status": "SUCCESS",
                "error_message": None,
                "additional_info": json.dumps(tracker.additional_info),
            }
        )

        logger.success(
            f"Processing completed: {tracker.rows_processed} rows in {duration:.2f}s"
        )

    except Exception as e:
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        log_data.update(
            {
                "end_time": end_time,
                "duration_seconds": duration,
                "rows_processed": tracker.rows_processed,
                "file_size_bytes": tracker.file_size_bytes,
                "status": "FAILED",
                "error_message": str(e)[:1000],  # Truncate long error messages
            }
        )

        logger.error(f"Processing failed after {duration:.2f}s: {str(e)}")
        raise

    finally:
        _insert_metadata_log(log_data, config, conn)


def _insert_metadata_log(log_data: dict, config: DataSourceConfig, conn):
    """
    Insert the metadata log into the database.
    """
    try:
        df_metadata = pd.DataFrame([log_data])

        insert_table(
            df_metadata,
            conn,
            config.metadata_schema_name,
            config.metadata_table_name,
            config.processor_type,
        )

        logger.info(
            f"Metadata logged: {log_data['status']} - {log_data['rows_processed']} rows"
        )

    except Exception as e:
        logger.warning(
            f"Failed to log metadata (but main processing status unchanged): {e}"
        )


def ensure_metadata_schema_exists(config: DataSourceConfig, db_manager):
    """
    Ensure the metadata schema and table exist before processing.
    """
    try:
        db_manager.create_schema_if_not_exists(config.metadata_schema_name)

        db_manager.create_metadata_table(
            config.metadata_schema_name,
            config.metadata_table_name,
            config.metadata_db_template,
        )

        logger.success(
            f"Metadata schema ready: {config.metadata_schema_name}.{config.metadata_table_name}"
        )
        return True

    except Exception as e:
        logger.error(f"Failed to create metadata schema: {e}")
        return False
