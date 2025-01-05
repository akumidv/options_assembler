"""
Binance api provider
"""
import datetime
import pandas as pd

from option_lib.provider._provider_entities import DataEngine, RequestParameters
from option_lib.provider.exchange.exchange_entities import ExchangeCode
from option_lib.provider.exchange._abstract_exchange import AbstractExchange


class BinanceExchange(AbstractExchange):
    """Binance exchange api"""

    def __init__(self, engine: DataEngine = DataEngine.PANDAS):
        """"""
        super().__init__(engine, ExchangeCode.BINANCE.value)

    def load_option_history(self, symbol: str, params: RequestParameters | None = None,
                            columns: list | None = None) -> pd.DataFrame:
        """load options history"""

    def load_future_history(self, symbol: str, params: RequestParameters | None = None,
                            columns: list | None = None) -> pd.DataFrame:
        """load futures history"""

    def load_option_chain(self, settlement_date: datetime.datetime | None = None,
                          expiration_date: datetime.datetime | None = None) -> pd.DataFrame | None:
        """Providing option chain by local file system is not supported return None"""
