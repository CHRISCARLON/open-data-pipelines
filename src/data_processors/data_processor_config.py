import requests
from io import BytesIO
from data_sources.data_source_config import DataSourceConfig
from typing import Protocol, Callable, Any, Dict
from loguru import logger

# ALL OF THIS NEEDS TO BE ABLE TO ACCOUNT FOR ALL GENERIC PROCESSING NEEDS!

class DataProcessorTrait(Protocol):
    def fetch_redirect_url(self, url: str) -> str: ...
    def fetch_data(self, url: str) -> BytesIO: ...
    def add_custom_function(self, function: Callable[..., Any]):...
    custom_functions: Dict[str, Callable[..., Any]]

class DataProcessor:
    def __init__(self, config: DataSourceConfig):
        """
        Initialise DataProcessor with a config object.
        Args:
            config: A DataSourceConfig object
        """
        self.config = config
        self.custom_functions: Dict[str, Callable[..., Any]] = {}
        logger.info(
            f"DataProcessor initialised with config: {config.__class__.__name__}"
        )

    def fetch_redirect_url(self, url: str) -> str:
        response = requests.get(url)
        response.raise_for_status()
        redirect_url = response.url
        logger.success(f"The Redirect URL is: {redirect_url}")
        return redirect_url

    def fetch_data(self, url: str) -> BytesIO:
        response = requests.get(url)
        response.raise_for_status()
        logger.info(f"Successfully fetched data from: {url}")
        return BytesIO(response.content)

    def add_custom_function(self, name: str, function: Callable[..., Any]):
        """
        Add a custom function to the processor.

        Args:
            name: Name for the custom function
            function: The function to add
        """
        self.custom_functions[name] = function
        logger.info(f"Added custom function: {name}")
