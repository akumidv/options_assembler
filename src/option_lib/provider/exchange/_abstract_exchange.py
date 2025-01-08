"""Abstract exchange class module"""

from abc import ABC, abstractmethod

import httpx
from option_lib.provider._provider_entities import DataEngine
from option_lib.provider._abstract_provider_class import AbstractProvider
from option_lib.provider.exchange.exchange_exception import APIException, RequestException


class RequestClass:
    api_url: str
    version_url: str | None = None
    HEADERS: dict = {
        'Accept': 'application/json',
        'User-Agent': 'Option Library Client',
    }
    def __init__(self, api_url):
        self.api_url = api_url[:-1] if api_url[-1] == '/' else api_url
        self.session = httpx.Client(headers=self.HEADERS)
        self.timestamp_offset = 0

    def request_api(self, endpoint_path: str, signed: bool = False, **kwargs):
        api_url = self._create_api_uri(endpoint_path)
        return self._request(api_url, signed, **kwargs)

    def _request(self, request_url, signed: bool, **kwargs):
        print('###', request_url)
        print('###', kwargs)
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
        except ValueError:
            txt = response.text
            raise RequestException(f'Invalid Response: {txt}')

    def _create_api_uri(self, endpoint_path: str) -> str:
        return f'{self.api_url}/{endpoint_path}'



class AbstractExchange(AbstractProvider, ABC):
    """Abstract exchange class"""


    @abstractmethod
    def __init__(self, engine: DataEngine, exchange_code: str, api_url: str, **kwargs):
        """"""
        self.client = RequestClass(api_url)
        super().__init__(exchange_code, **kwargs)

    @abstractmethod
    def get_symbols_books_snapshot(self, symbols: list[str] | None = None):
        """Get symbols books snapshot"""
