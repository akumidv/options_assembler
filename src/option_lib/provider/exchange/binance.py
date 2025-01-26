"""
Binance api provider
"""
import datetime
import pandas as pd

from option_lib.entities import AssetKind, Timeframe
from option_lib.provider._provider_entities import DataEngine, RequestParameters
from option_lib.provider.exchange.exchange_entities import ExchangeCode
from option_lib.provider.exchange._abstract_exchange import AbstractExchange, BookData


class BinanceExchange(AbstractExchange):
    """Binance exchange api"""
    API_URL = 'https://api.binance.com'

    def load_future_book(self, symbol: str, settlement_datetime: datetime.datetime | None = None,
                         timeframe: Timeframe = Timeframe.EOD, columns: list | None = None) -> pd.DataFrame:
        raise NotImplementedError

    def load_option_book(self, symbol: str, settlement_datetime: datetime.datetime | None = None,
                         timeframe: Timeframe = Timeframe.EOD, columns: list | None = None) -> pd.DataFrame:
        raise NotImplementedError

    def get_symbols_list(self, asset_kind: AssetKind) -> list[str]:
        raise NotImplementedError

    def get_symbols_books_snapshot(self, symbols: list[str] | str | None = None) -> pd.DataFrame:
        pass

    def __init__(self, engine: DataEngine = DataEngine.PANDAS):
        """"""
        super().__init__(engine, ExchangeCode.BINANCE.name, self.API_URL)

    def load_option_history(self, symbol: str, params: RequestParameters | None = None,
                            columns: list | None = None) -> pd.DataFrame:
        """load options history"""
        raise NotImplementedError

    def load_future_history(self, symbol: str, params: RequestParameters | None = None,
                            columns: list | None = None) -> pd.DataFrame:
        """load futures history"""

    def load_option_chain(self, symbol: str, settlement_datetime: datetime.datetime | None = None,
                          expiration_date: datetime.datetime | None = None,
                          timeframe: Timeframe = Timeframe.EOD,
                          columns: list | None = None) -> pd.DataFrame | None:
        """Providing option chain by local file system is not supported return None"""
