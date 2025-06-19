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
    BDUK_PREMISES = (
        "bduk_premises",
        "https://www.gov.uk/government/publications/premises-in-bduk-plans-england-and-wales",
    )
    CADENT_GAS = (
        "cadent_gas_underground_pipes", 
        "https://cadentgas.opendatasoft.com/api/explore/v2.1/catalog/datasets/gpi-pipe-infrastructure-open/exports/parquet"
        )
    BUILT_UP_AREAS = (
        "built_up_areas",
        "https://api.os.uk/downloads/v1/products/BuiltUpAreas/downloads?area=GB&format=GeoPackage&redirect",
    )
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

    def __str__(self) -> str:
        """String representation of the configuration"""
        ...
