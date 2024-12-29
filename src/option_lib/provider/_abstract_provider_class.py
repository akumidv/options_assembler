"""Abstract class with mandatory interfaces for every provider """
from abc import ABC, abstractmethod

import pandas as pd

from option_lib.provider import RequestParameters

class AbstractProvider(ABC):
    """ Provider interfaces """
    exchange_code: str
    option_columns = ['datetime', 'expiration_date', 'strike', 'type', 'premium', 'future_expiration_date']
    future_columns = ['datetime', 'expiration_date', 'price']

    def __init__(self, exchange_code: str, **kwargs):
        self.exchange_code = exchange_code

    @abstractmethod
    def load_option(self, symbol: str, params: RequestParameters) -> pd.DataFrame:
        """Provide option by period, timeframe"""

    @abstractmethod
    def load_future(self, symbol: str, params: RequestParameters) -> pd.DataFrame:
        """Provide future by period, timeframe"""
