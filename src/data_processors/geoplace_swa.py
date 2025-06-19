import duckdb
import psycopg2
from loguru import logger
from typing import Optional
import pandas as pd
import requests
from io import BytesIO
from datetime import datetime
from msoffcrypto import OfficeFile


def insert_table_to_motherduck(df, conn, schema, table):
    """
    Inserts a DataFrame into a DuckDB table.

    This function also ensures that the order of the columns is always the same.

    Args:
        df: The DataFrame to be inserted.
        conn: The MotherDuck connection.
        schema: The schema of the table.
        table: The name of the table.
    """
    try:
        insert_sql = f"""INSERT INTO "{schema}"."{table}" SELECT * FROM df"""
        conn.execute(insert_sql)
    except (duckdb.DataError, duckdb.Error, Exception) as e:
        logger.error(f"Error inserting DataFrame into DuckDB: {e}")
        raise


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
            columns = ', '.join([f'"{col}"' for col in df.columns])
            placeholders = ', '.join(['%s'] * len(df.columns))
            insert_sql = f'INSERT INTO "{schema}"."{table}" ({columns}) VALUES ({placeholders})'
            cursor.execute(insert_sql, tuple(row.values))
        
        conn.commit()
        cursor.close()
        logger.success(f"Inserted {len(df)} rows into PostgreSQL table {schema}.{table}")
        
    except psycopg2.Error as e:
        logger.error(f"Error inserting DataFrame into PostgreSQL: {e}")
        conn.rollback()
        raise
    except Exception as e:
        logger.error(f"Unexpected error inserting into PostgreSQL: {e}")
        conn.rollback()
        raise


def clean_name_geoplace(x: str):
    """
    Used to clean names columns ready for left join in the future

    # usage: df.loc[:, "account_name"] = df.loc[:, "account_name"].apply(clean_name)
    """
    x = x.replace("LONDON BOROUGH OF", "").strip()
    x = x.replace("COUNTY COUNCIL", "").strip()
    x = x.replace("BOROUGH COUNCIL", "").strip()
    x = x.replace("CITY COUNCIL", "").strip()
    x = x.replace("COUNCIL", "").strip()
    x = x.replace("ROYAL BOROUGH OF", "").strip()
    x = x.replace("COUNCIL OF THE", "").strip()
    x = x.replace("CITY OF", "").strip()
    x = x.replace("COUNTY", "").strip()
    x = x.replace("BOROUGH", "").strip()
    x = x.replace("CITY", "").strip()
    x = x.replace("METROPOLITAN", "").strip()
    x = x.replace("DISTRICT", "").strip()
    x = x.replace("CORPORATION", "").strip()
    x = x.replace("OF", "").strip()
    x = str(x).lower()
    return x


def fetch_swa_codes(url: str) -> Optional[pd.DataFrame]:
    """
    Use download link to fetch data.
    Data is an old xls file and needs extra steps to create dataframe.
    Calls the get_link function.

    Returns:
        Optional[pd.DataFrame]: DataFrame containing the SWA codes data, or None if an error occurs.
    """
    try:
        assert isinstance(url, str), "URL must be a string"
    except (ValueError, AssertionError) as e:
        logger.error(f"Error getting download link: {e}")
        return None

    try:
        response = requests.get(url)
        response.raise_for_status()

        result = BytesIO(response.content)
        office_file = OfficeFile(result)
        office_file.load_key("VelvetSweatshop")

        decrypted_file = BytesIO()
        office_file.decrypt(decrypted_file)
        decrypted_file.seek(0)

        # Read in and do some basic renames and transformation
        df = pd.read_excel(decrypted_file, header=1, engine="xlrd")
        df = df.astype(str).replace("nan", None)
        df.columns = df.columns.str.lower().str.replace(" ", "_").str.replace("/", "_")
        df.loc[:, "account_name"] = df.loc[:, "account_name"].apply(clean_name_geoplace)
        df.loc[df["account_name"] == "peter", "account_name"] = "peterborough"
        df.loc[
            df["account_name"] == "bournemouth, christchurch and poole", "account_name"
        ] = "bournemouth christchurch and poole"
        df.loc[df["account_name"] == "brighton & hove", "account_name"] = (
            "brighton and hove"
        )
        df.loc[df["account_name"] == "telford & wrekin", "account_name"] = (
            "telford and wrekin"
        )
        df.loc[df["account_name"] == "hammersmith & fulham", "account_name"] = (
            "hammersmith and fulham"
        )
        df.loc[df["account_name"] == "cheshire east", "account_name"] = "east cheshire"
        df.loc[df["account_name"] == "cheshire west and chester", "account_name"] = (
            "west cheshire"
        )
        df.loc[df["account_name"] == "east riding  yorkshire", "account_name"] = (
            "eastridingyorkshire"
        )

        # Add date time processed column
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df["date_time_processed"] = current_time
        return df

    except requests.RequestException as e:
        logger.error(f"Error downloading file: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in fetch_swa_codes: {e}")

    return None


def process_data(url: str, conn, schema_name: str, table_name: str, db_type: str = "motherduck") -> None:
    """
    Main function to fetch and process data stream with Pandas.

    Args:
        url: The URL to fetch the data from.
        conn: The database connection (MotherDuck or PostgreSQL).
        schema_name: The schema of the table.
        table_name: The name of the table.
        db_type: Type of database - "motherduck" or "postgresql"
    """
    logger.info(f"Starting data stream processing from {url} for {db_type}")
    try:
        df = fetch_swa_codes(url)
        if df is not None:
            match db_type.lower():
                case "postgresql":
                    insert_table_to_postgresql(df, conn, schema_name, table_name)
                case "motherduck":
                    insert_table_to_motherduck(df, conn, schema_name, table_name)
                case _:
                    insert_table_to_motherduck(df, conn, schema_name, table_name)
            logger.success(f"Data inserted into {schema_name}.{table_name}")
    except Exception as e:
        logger.error(f"Error processing data: {e}")
        raise
