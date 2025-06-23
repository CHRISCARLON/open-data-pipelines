import pandas as pd
import time
import psycopg2

from loguru import logger
from data_sources.data_source_config import DataProcessorType


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


def insert_table_to_postgresql(df, conn, schema, table):
    """
    Inserts a DataFrame into a PostgreSQL table.

    Args:
        df: The DataFrame to be inserted.
        conn: The PostgreSQL connection.
        schema: The schema of the table.
        table: The name of the table.
    """
    try:
        # Clear existing data
        cursor = conn.cursor()
        cursor.execute(f'DELETE FROM "{schema}"."{table}"')

        # Insert new data row by row
        for _, row in df.iterrows():
            columns = ", ".join([f'"{col}"' for col in df.columns])
            placeholders = ", ".join(["%s"] * len(df.columns))
            insert_sql = (
                f'INSERT INTO "{schema}"."{table}" ({columns}) VALUES ({placeholders})'
            )
            cursor.execute(insert_sql, tuple(row.values))

        conn.commit()
        cursor.close()
        logger.success(
            f"Inserted {len(df)} rows into PostgreSQL table {schema}.{table}"
        )

    except psycopg2.Error as e:
        logger.error(f"Error inserting DataFrame into PostgreSQL: {e}")
        conn.rollback()
        raise
    except Exception as e:
        logger.error(f"Unexpected error inserting into PostgreSQL: {e}")
        conn.rollback()
        raise


def insert_table(df, conn, schema, table, processor_type: DataProcessorType):
    """
    Pass
    """
    logger.info(f"Processor Type is:{processor_type}")
    match processor_type:
        case DataProcessorType.MOTHERDUCK:
            return insert_into_motherduck(df, conn, schema, table)
        case DataProcessorType.POSTGRESQL:
            return insert_table_to_postgresql(df, conn, schema, table)
        case _:
            raise ValueError(f"No insert handler for processor type '{processor_type}'")
