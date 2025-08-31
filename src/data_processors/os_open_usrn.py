import requests
import os
import tempfile
import zipfile
import pandas as pd
import fiona
from shapely import wkt
from shapely.geometry import shape
from loguru import logger
from tqdm import tqdm
import time
from typing import Optional

from ..data_processors.utils.metadata_logger import metadata_tracker
from ..data_sources.data_source_config import DataSourceConfig


def insert_into_motherduck(df, conn, schema: str, table: str):
    """
    Processes dataframe into MotherDuck table with retry logic

    Args:
        df: DataFrame to insert
        conn: Connection object
        schema: Schema name
        table: Table name
    """
    max_retries = 3
    base_delay = 3

    if not conn:
        logger.error("No connection provided")
        return None

    for attempt in range(max_retries):
        try:
            logger.info(f"Attempting to insert into schema: {schema}, table: {table}")

            conn.register("df_temp", df)

            insert_sql = f"""INSERT INTO "{schema}"."{table}" SELECT * FROM df_temp"""
            conn.execute(insert_sql)

            if attempt > 0:
                logger.success(f"Successfully inserted data on attempt {attempt + 1}")

            logger.success(f"Inserted {len(df)} rows into {schema}.{table}")
            return None

        except Exception as e:
            try:
                conn.unregister("df_temp")
            except Exception:
                pass

            if attempt < max_retries - 1:
                wait_time = (2**attempt) * base_delay
                logger.warning(f"Attempt {attempt + 1} failed: {e}")
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logger.error(f"All {max_retries} attempts failed. Final error: {e}")
                raise

    return None


def fetch_redirect_url(url: str) -> str:
    """
    Call the redirect url and then fetch the actual download url.
    This is suboptimal and will change in future versions.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        redirect_url = response.url
        logger.success(f"The Redirect URL is: {redirect_url}")
    except (requests.exceptions.RequestException, ValueError, Exception) as e:
        logger.error(f"An error retrieving the redirect URL: {e}")
        raise
    return redirect_url


def load_geopackage_open_usrns(
    url: str, conn, batch_size: int, schema: str, table: str, tracker=None
):
    """
    Function to load OS open usrn data in batches of 50,000 rows.

    It taskes a duckdb connection object and the download url required.

    Args:
        Url for data
        Connection object
    """
    chunk_size = batch_size
    errors = []

    try:
        logger.info("Downloading zip file...")
        response = requests.get(url, stream=True)
        response.raise_for_status()

        file_size = len(response.content)
        if tracker:
            tracker.set_file_size(file_size)

        with tempfile.TemporaryDirectory() as temp_dir:
            zip_path = os.path.join(temp_dir, "temp.zip")
            with open(zip_path, "wb") as zip_file:
                zip_file.write(response.content)

            logger.info("Extracting zip file...")
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(temp_dir)

            gpkg_file = None
            for root, dirs, files in os.walk(temp_dir):
                logger.info(f"Searching in directory: {root}")
                logger.info(f"Found files: {files}")
                for file_name in files:
                    if file_name.endswith(".gpkg"):
                        gpkg_file = os.path.join(root, file_name)
                        logger.success(f"Found GeoPackage file: {gpkg_file}")
                        break
                if gpkg_file:
                    break

            if gpkg_file:
                try:
                    with fiona.open(gpkg_file, "r") as src:
                        crs = src.crs
                        data_schema = src.schema

                        logger.info(f"The CRS is: {crs}")
                        logger.info(f"The Data Schema is: {data_schema}")

                        total_features = len(src)
                        if tracker:
                            tracker.set_rows_processed(total_features)
                            tracker.add_info("total_features", total_features)
                            tracker.add_info("batch_size", batch_size)
                            tracker.add_info("file_format", "geopackage")
                            tracker.add_info("crs", str(crs))

                        features = []

                        for i, feature in enumerate(
                            tqdm(src, total=total_features, desc="Processing features")
                        ):
                            try:
                                geom = shape(feature["geometry"])
                                feature["properties"]["geometry"] = wkt.dumps(geom)
                            except Exception as e:
                                feature["properties"]["geometry"] = None
                                error_msg = (
                                    f"Error converting geometry for feature {i}: {e}"
                                )
                                logger.warning(error_msg)
                                errors.append(error_msg)

                            features.append(feature["properties"])

                            if len(features) == chunk_size:
                                df_chunk = pd.DataFrame(features)
                                insert_into_motherduck(df_chunk, conn, schema, table)
                                logger.info(
                                    f"Processed features {i - chunk_size + 1} to {i}"
                                )

                                features = []

                        if features:
                            df_chunk = pd.DataFrame(features)
                            insert_into_motherduck(df_chunk, conn, schema, table)
                            logger.info(
                                f"Processed remaining features: {len(features)}"
                            )
                            features = []

                except Exception as e:
                    error_msg = f"Error processing GeoPackage: {e}"
                    logger.error(error_msg)
                    errors.append(error_msg)
                    raise
            else:
                error_msg = "No GeoPackage file found in the zip archive"
                logger.error(error_msg)
                errors.append(error_msg)
                raise FileNotFoundError(error_msg)
    except Exception as e:
        error_msg = f"Error processing the zip file: {e}"
        logger.error(error_msg)
        errors.append(error_msg)
        raise
    finally:
        if errors:
            logger.info(f"Total errors encountered: {len(errors)}")
            if tracker:
                tracker.add_info("errors", len(errors))
                tracker.add_info("errors", errors)
    return None


def process_data(
    url: str,
    conn,
    batch_size: int,
    schema_name: str,
    table_name: str,
    config: Optional[DataSourceConfig] = None,
):
    """
    Process the data from the url and insert it into the motherduck table.
    """
    logger.info(
        f"Starting data stream processing from {url} with batch size {batch_size}"
    )

    if config:
        with metadata_tracker(config, conn, url) as tracker:
            try:
                redirect_url = fetch_redirect_url(url)
                load_geopackage_open_usrns(
                    redirect_url, conn, batch_size, schema_name, table_name, tracker
                )
                logger.success("USRN data processing completed successfully")
            except Exception as e:
                logger.error(f"Error processing USRN data: {e}")
                raise
    else:
        redirect_url = fetch_redirect_url(url)
        load_geopackage_open_usrns(
            redirect_url, conn, batch_size, schema_name, table_name
        )
