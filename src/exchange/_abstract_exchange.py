"""Abstract exchange class module"""

from abc import ABC, abstractmethod
from typing import NamedTuple
import httpx
import pandas as pd

from provider import DataEngine
from provider import AbstractProvider
from exchange.exchange_exception import APIException, RequestException


class BookData(NamedTuple):
    """Book data snapshot"""
    option: pd.DataFrame | None
    future: pd.DataFrame | None
    spot: pd.DataFrame | None


class RequestClass:
    """Request implementation for Exchanges"""
    api_url: str
    version_url: str | None = None
    HEADERS: dict = {
        'Accept': 'application/json',
        'User-Agent': 'Option Library Client',
    }

    def __init__(self, api_url, http_params: dict | None = None):
        self.api_url = api_url[:-1] if api_url[-1] == '/' else api_url
        if not isinstance(http_params, dict):
            http_params = {'headers': self.HEADERS}
        elif 'headers' not in http_params:
            http_params['headers'] = self.HEADERS
        self.session = httpx.Client(**http_params)
        self.timestamp_offset = 0

    def request_api(self, endpoint_path: str, signed: bool = False, **kwargs):
        """Main request method """
        api_url = self._create_api_uri(endpoint_path)
        return self._request(api_url, signed, **kwargs)

    def _request(self, request_url, signed: bool, **kwargs):
        response = self.session.get(request_url, **kwargs)
        return self._handle_response(response)

    @staticmethod
    def _handle_response(response: httpx.Response):
        """Internal helper for handling API responses from the Binance server.
        Raises the appropriate exceptions when necessary; otherwise, returns the
        response.
        """
        if not str(response.status_code).startswith('2'):
            raise APIException(response, response.status_code, response.text)
        try:
            return response.json()
        except ValueError as err:
            txt = response.text
            raise RequestException(f'Invalid Response: {err}\n{txt}: ')

    def _create_api_uri(self, endpoint_path: str) -> str:
        endpoint_path = endpoint_path if endpoint_path[0] != '/' else endpoint_path[1:]
        return f'{self.api_url}/{endpoint_path}'


class AbstractExchange(AbstractProvider, ABC):
    """Abstract exchange class"""
    SOURCE_PREFIX = 'source'

    @abstractmethod
    def __init__(self, engine: DataEngine, exchange_code: str, api_url: str, http_params: dict | None = None, **kwargs):
        """"""
        self.client = RequestClass(api_url, http_params)
        super().__init__(exchange_code, **kwargs)

    @abstractmethod
    def get_options_assets_books_snapshot(self, asset_codes: list[str] | str | None = None) -> pd.DataFrame:
        """Get symbols books snapshot"""
