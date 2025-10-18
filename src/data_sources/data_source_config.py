from typing import Protocol, runtime_checkable
from enum import Enum


class DataProcessorType(Enum):
    """Enum for different types of data processors"""

    MOTHERDUCK = "motherduck"
    POSTGRESQL = "postgresql"
    # Add other data processors as needed


class TimeRange(Enum):
    """Enum for different time ranges"""

    LATEST = "latest"
    HISTORIC = "historic"


class DataSourceType(Enum):
    """Enum for different types of data sources with their associated base URLs"""

    STREET_MANAGER = (
        "street_manager",
        "https://opendata.manage-roadworks.service.gov.uk/permit/",
    )
    SECTION_58 = (
        "section_58",
        "https://opendata.manage-roadworks.service.gov.uk/section_58/",
    )
    GEOPLACE_SWA = (
        "geoplace_swa",
        "https://www.geoplace.co.uk/local-authority-resources/street-works-managers/view-swa-codes",
    )
    OS_OPEN_USRN = (
        "os_open_usrn",
        "https://api.os.uk/downloads/v1/products/OpenUSRN/downloads?area=GB&format=GeoPackage&redirect",
    )
    OS_USRN_UPRN = (
        "os_usrn_uprn",
        "https://api.os.uk/downloads/v1/products/LIDS/downloads",
    )
    BDUK_PREMISES_SEPT_2024 = (
        "bduk_premises_sept_2024",
        "https://www.gov.uk/government/publications/premises-in-bduk-plans-england-and-wales",
    )
    BDUK_PREMISES_JUL_2025 = (
        "bduk_premises_jul_2025",
        "https://www.gov.uk/government/publications/january-2025-omr-and-premises-in-bduk-plans-england-and-wales",
    )
    BDUK_PREMISES_SEPT_2025 = (
        "bduk_premises_sept_2025",
        "https://www.gov.uk/government/publications/may-2025-omr-and-premises-in-bduk-plans-england-and-wales",
    )
    CADENT_GAS = (
        "cadent_gas_underground_pipes",
        "https://cadentgas.opendatasoft.com/api/explore/v2.1/catalog/datasets/gpi-pipe-infrastructure-open/exports/parquet",
    )
    BUILT_UP_AREAS = (
        "built_up_areas",
        "https://api.os.uk/downloads/v1/products/BuiltUpAreas/downloads?area=GB&format=GeoPackage&redirect",
    )
    NHS_ENGLISH_PRESCRIBING_DATA = (
        "nhs_english_prescribing_data",
        "https://opendata.nhsbsa.net/dataset/906115a6-4155-44be-8b81-f8e83cebfb84/resource/ea287041-1027-4062-9db9-040f48223b13/download/epd_snomed_202503.csv",
    )
    NAPTAN = (
        "naptan_data",
        "https://beta-naptan.dft.gov.uk/Download/National/csv",
    )
    BODS_TIMETABLES = (
        "bods_timetables",
        "https://data.bus-data.dft.gov.uk/timetable/download/gtfs-file/north_west/",
    )
    CODE_POINT = (
        "code_point",
        "https://api.os.uk/downloads/v1/products/CodePointOpen/downloads?area=GB&format=GeoPackage&redirect",
    )
    POSTCODE_p001 = (
        "postcode_p001",
        "https://www.nomisweb.co.uk/output/census/2021/pcd_p001.csv",
    )
    POSTCODE_p002 = (
        "postcode_p002",
        "https://www.nomisweb.co.uk/output/census/2021/pcd_p002.csv",
    )
    NATIONAL_STATISTIC_POSTCODE_LOOKUP = (
        "national_statistic_postcode_lookup",
        "https://www.arcgis.com/sharing/rest/content/items/2410f94375674cd2a6182b4f5e531bb8/data",
    )
    ONSUprnDirectory = ("ons_uprn_directory", "https://geoportal.statistics.gov.uk")

    # Add other data sources as needed

    def __init__(self, code: str, base_url: str):
        self._code = code
        self._base_url = base_url

    @property
    def code(self) -> str:
        """Get the code for the data source type"""
        return self._code

    @property
    def base_url(self) -> str:
        """Get the base URL for the data source type"""
        return self._base_url


@runtime_checkable
class DataSourceConfig(Protocol):
    """Protocol defining the interface for data source configurations"""

    @property
    def processor_type(self) -> DataProcessorType:
        """Get the processor type"""
        ...

    @property
    def source_type(self) -> DataSourceType:
        """Get the source type"""
        ...

    @property
    def time_range(self) -> TimeRange:
        """Get the time range"""
        ...

    @property
    def base_url(self) -> str:
        """Get the base URL for the configured data source"""
        ...

    @property
    def download_links(self) -> list[str] | dict:
        """Get the download links for the configured data source"""
        ...

    @property
    def schema_name(self) -> str:
        """Get the schema name for the configured data source"""
        ...

    @property
    def table_names(self) -> list[str] | dict:
        """Get the table names for the configured data source"""
        ...

    @property
    def db_template(self) -> dict:
        """Get the database template for the configured data source"""
        ...

    @property
    def metadata_schema_name(self) -> str:
        """Get the metadata schema name for tracking processing information"""
        ...

    @property
    def metadata_table_name(self) -> str:
        """Get the metadata table name for logging processing runs"""
        ...

    @property
    def metadata_db_template(self) -> dict:
        """Get the database template for metadata logging table"""
        ...

    def __str__(self) -> str:
        """String representation of the configuration"""
        ...
