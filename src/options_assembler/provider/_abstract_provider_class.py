"""
Abstract class with mandatory interfaces for every provider

Provider limits:
- provide data if it has it. For example if exchange provider do not support history it will not return option_history
- if local files do not contain chain it will not return data
- if request is wrong - for example there is not option for expiration_date it throw an error
It is not planned that provider level will be contained any data logic. It should be in option data class
"""
from abc import ABC, abstractmethod
import datetime
import pandas as pd

from options_assembler.entities import OptionColumns, FuturesColumns, Timeframe, AssetKind
from options_assembler.provider._provider_entities import RequestParameters


class AbstractProvider(ABC):
    """ Provider interfaces """
    exchange_code: str
    option_columns = [OptionColumns.TIMESTAMP.nm, OptionColumns.STRIKE.nm, OptionColumns.EXPIRATION_DATE.nm,
                      OptionColumns.OPTION_TYPE.nm, OptionColumns.PRICE.nm, OptionColumns.UNDERLYING_EXPIRATION_DATE.nm,
                      OptionColumns.UNDERLYING_PRICE.nm]
    future_columns = [FuturesColumns.TIMESTAMP.nm, FuturesColumns.EXPIRATION_DATE.nm, FuturesColumns.PRICE.nm]

    def __init__(self, exchange_code: str, **kwargs):
        self.exchange_code = exchange_code
        super().__init__(**kwargs)

    @abstractmethod
    def get_symbols_list(self, asset_kind: AssetKind) -> list[str]:
        """List of symbols"""

    def get_symbol_history_years(self, symbol: str, asset_kind: AssetKind, timeframe: Timeframe):
        """List of history years"""

    @abstractmethod
    def load_option_history(self, symbol: str, params: RequestParameters, columns: list | None = None) -> pd.DataFrame:
        """Provide option by period, timeframe"""

    @abstractmethod
    def load_option_book(self, symbol: str, settlement_datetime: datetime.datetime | None = None,
                         timeframe: Timeframe = Timeframe.EOD, columns: list | None = None) -> pd.DataFrame:
        """Provide option for datetime, timeframe"""

    @abstractmethod
    def load_future_history(self, symbol: str, params: RequestParameters, columns: list | None = None) -> pd.DataFrame:
        """Provide future by period, timeframe"""

    @abstractmethod
    def load_future_book(self, symbol: str, settlement_datetime: datetime.datetime | None = None,
                         timeframe: Timeframe = Timeframe.EOD, columns: list | None = None) -> pd.DataFrame:
        """Provide future for datetime, timeframe"""

    @abstractmethod
    def load_option_chain(self, symbol: str, settlement_datetime: datetime.datetime | None = None,
                          expiration_date: datetime.datetime | None = None,
                          timeframe: Timeframe = Timeframe.EOD,
                          columns: list | None = None) -> pd.DataFrame | None:
        """Provide option chain by request to api if supported. Otherwise, return None"""
