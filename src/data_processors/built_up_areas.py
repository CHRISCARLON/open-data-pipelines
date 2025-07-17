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

            insert_sql = f'INSERT INTO "{schema}"."{table}" SELECT * FROM df_temp'

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


def load_geopackage_built_up_areas(
    url: str, conn, batch_size: int, schema: str, table: str
):
    """
    Function to load OS Open Built Up Areas data in batches.

    Args:
        url: URL for data
        conn: Connection object
        batch_size: Number of rows per batch
        schema: Schema name
        table: Table name
    """

    chunk_size = batch_size
    errors = []

    try:
        # Download the zip file
        logger.info("Downloading Built Up Areas zip file...")
        response = requests.get(url, stream=True)
        response.raise_for_status()

        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Write the zip file to the temporary directory
            zip_path = os.path.join(temp_dir, "built_up_areas.zip")
            with open(zip_path, "wb") as zip_file:
                zip_file.write(response.content)

            logger.info("Extracting Built Up Areas zip file...")
            # Extract the contents of the zip file
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(temp_dir)

            # Find the GeoPackage file for Built Up Areas
            gpkg_file = None
            for root, dirs, files in os.walk(temp_dir):
                for file_name in files:
                    if (
                        file_name.endswith(".gpkg")
                        and "built_up_areas" in file_name.lower()
                    ):
                        gpkg_file = os.path.join(root, file_name)
                        logger.success(f"Found Built Up Areas GeoPackage: {gpkg_file}")
                        break
                if gpkg_file:
                    break

            if not gpkg_file:
                # Fallback: look for any gpkg file
                for root, dirs, files in os.walk(temp_dir):
                    for file_name in files:
                        if file_name.endswith(".gpkg"):
                            gpkg_file = os.path.join(root, file_name)
                            logger.info(f"Found GeoPackage file: {gpkg_file}")
                            break
                    if gpkg_file:
                        break

            if gpkg_file:
                try:
                    # Open the GeoPackage file
                    with fiona.open(gpkg_file, "r") as src:
                        # Print metadata
                        crs = src.crs
                        data_schema = src.schema

                        logger.info(f"CRS: {crs}")
                        logger.info(f"Schema: {data_schema}")

                        # Get total number of features
                        total_features = len(src)
                        logger.info(f"Total Built Up Areas features: {total_features}")

                        features = []

                        for i, feature in enumerate(
                            tqdm(
                                src,
                                total=total_features,
                                desc="Processing Built Up Areas",
                            )
                        ):
                            try:
                                geom_wkt = None
                                if feature.get("geometry"):
                                    try:
                                        geom = shape(feature["geometry"])

                                        if geom and geom.is_valid:
                                            geom_wkt = wkt.dumps(geom)
                                        else:
                                            try:
                                                geom = geom.buffer(0)
                                                geom_wkt = (
                                                    wkt.dumps(geom)
                                                    if geom.is_valid
                                                    else None
                                                )
                                            except Exception:
                                                geom_wkt = None
                                    except Exception:
                                        geom_wkt = None

                                # Create record - convert everything to string
                                built_up_area_record = {
                                    "gsscode": str(
                                        feature["properties"].get("gsscode", "")
                                    )
                                    if feature["properties"].get("gsscode") is not None
                                    else None,
                                    "name1_text": str(
                                        feature["properties"].get("name1_text", "")
                                    )
                                    if feature["properties"].get("name1_text")
                                    is not None
                                    else None,
                                    "name1_language": str(
                                        feature["properties"].get("name1_language", "")
                                    )
                                    if feature["properties"].get("name1_language")
                                    is not None
                                    else None,
                                    "name2_text": str(
                                        feature["properties"].get("name2_text", "")
                                    )
                                    if feature["properties"].get("name2_text")
                                    is not None
                                    else None,
                                    "name2_language": str(
                                        feature["properties"].get("name2_language", "")
                                    )
                                    if feature["properties"].get("name2_language")
                                    is not None
                                    else None,
                                    "areahectares": str(
                                        feature["properties"].get("areahectares", "")
                                    )
                                    if feature["properties"].get("areahectares")
                                    is not None
                                    else None,
                                    "geometry_area_m": str(
                                        feature["properties"].get("geometry_area_m", "")
                                    )
                                    if feature["properties"].get("geometry_area_m")
                                    is not None
                                    else None,
                                    "geometry": geom_wkt,
                                }

                            except Exception as e:
                                # Fallback with nulls grrrr
                                built_up_area_record = {
                                    "gsscode": None,
                                    "name1_text": None,
                                    "name1_language": None,
                                    "name2_text": None,
                                    "name2_language": None,
                                    "areahectares": None,
                                    "geometry_area_m": None,
                                    "geometry": None,
                                }
                                error_msg = f"Error processing feature {i}: {e}"
                                logger.warning(error_msg)
                                errors.append(error_msg)

                            features.append(built_up_area_record)

                            # Process batch when it reaches chunk_size
                            if len(features) == chunk_size:
                                df_chunk = pd.DataFrame(features)
                                insert_into_motherduck(df_chunk, conn, schema, table)
                                logger.info(
                                    f"Processed Built Up Areas batch: {i - chunk_size + 1} to {i}"
                                )
                                features = []

                        # Process any remaining features
                        if features:
                            df_chunk = pd.DataFrame(features)
                            insert_into_motherduck(df_chunk, conn, schema, table)
                            logger.info(
                                f"Processed remaining Built Up Areas features: {len(features)}"
                            )

                except Exception as e:
                    error_msg = f"Error processing Built Up Areas GeoPackage: {e}"
                    logger.error(error_msg)
                    errors.append(error_msg)
                    raise
            else:
                error_msg = "No Built Up Areas GeoPackage file found in the zip archive"
                logger.error(error_msg)
                errors.append(error_msg)
                raise FileNotFoundError(error_msg)

    except Exception as e:
        error_msg = f"Error processing Built Up Areas zip file: {e}"
        logger.error(error_msg)
        errors.append(error_msg)
        raise
    finally:
        # Print all errors
        if errors:
            logger.error(f"Total errors encountered: {len(errors)}")
            for error in errors:
                logger.error(error)

    return None


def process_built_up_areas(
    url: str, conn, batch_size: int, schema_name: str, table_name: str
):
    """
    Process the Built Up Areas data from the url and insert it into the motherduck table.
    """
    logger.info(
        f"Starting Built Up Areas processing from {url} with batch size {batch_size}"
    )

    try:
        conn.execute("INSTALL spatial;")
        conn.execute("LOAD spatial;")
        logger.info("Spatial extension loaded for Built Up Areas")
    except Exception as e:
        logger.warning(f"Could not load spatial extension: {e}")

    redirect_url = fetch_redirect_url(url)
    load_geopackage_built_up_areas(
        redirect_url, conn, batch_size, schema_name, table_name
    )
