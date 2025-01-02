"""Abstract class with mandatory interfaces for every provider """
from abc import ABC, abstractmethod

import pandas as pd

from option_lib.provider import RequestParameters
from option_lib.provider import ProviderOptionColumns, ProviderFutureColumns


class AbstractProvider(ABC):
    """ Provider interfaces """
    exchange_code: str
    option_columns = [column.col for column in ProviderOptionColumns]
    future_columns = [column.col for column in ProviderFutureColumns]

    def __init__(self, exchange_code: str, **kwargs):
        self.exchange_code = exchange_code
        super().__init__(**kwargs)

    @abstractmethod
    def load_option_history(self, symbol: str, params: RequestParameters) -> pd.DataFrame:
        """Provide option by period, timeframe"""

    @abstractmethod
    def load_future_history(self, symbol: str, params: RequestParameters) -> pd.DataFrame:
        """Provide future by period, timeframe"""
