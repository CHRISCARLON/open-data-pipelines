import os
import HerdingCats as hc

from ..databases.motherduck import MotherDuckManager
from ..data_sources.ons_uprn_directory import ONSUprnDirectory
from ..data_processors.ons_uprn_directory import process_data
from ..data_processors.utils.metadata_logger import ensure_metadata_schema_exists


def ons_geo_portal():
    """Fetch the ONS UPRN Directory download URL from the ONS Geo Portal."""
    with hc.CatSession(hc.ONSGeoPortal.ONS_GEO) as session:
        explorer = hc.ONSGeoExplorer(session)

        print("Searching for ONSUD datasets...")
        summary = explorer.get_datasets_summary(
            q="ONSUD",
            sort="Date Created|created|desc",
            description=True
        )

        target_title = ["ONS UPRN Directory", "July 2025"]
        exclude_phrase = "user guide"
        target_dataset = None

        for dataset in summary:
            title = dataset['title'].lower()
            if (all(phrase.lower() in title for phrase in target_title) and
                exclude_phrase.lower() not in title):
                target_dataset = dataset
                break

        if target_dataset:
            download_info = explorer.get_download_info(target_dataset['id'])
            return download_info['download_url']
        else:
            raise ValueError(f"Target dataset '{target_title}' not found in search results.")


def main():
    if not (token := os.getenv("MOTHERDUCK_TOKEN")) or not (
        database := os.getenv("MOTHERDB")
    ):
        raise ValueError("MOTHERDUCK_TOKEN and MOTHERDB must be set")

    config = ONSUprnDirectory.create_default()

    with MotherDuckManager(token, database) as db_manager:
        db_manager.setup_for_data_source(config)
        ensure_metadata_schema_exists(config, db_manager)

        url = ons_geo_portal()
        print(f"Download URL: {url}")

        process_data(
            url=url,
            conn=db_manager.connection,
            batch_limit=config.batch_limit or 250000,
            schema_name=config.schema_name,
            table_name=config.table_names[0],
            processor_type=config.processor_type,
            config=config,
        )


if __name__ == "__main__":
    main()
