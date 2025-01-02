"""
Binance api provider
"""
import pandas as pd

from option_lib.provider import DataEngine, RequestParameters
from option_lib.provider.exchange import AbstractExchange
from option_lib.provider.exchange.exchange_entities import ExchangeCode


class BinanceExchange(AbstractExchange):
    """Binance exchange api"""

    def __init__(self, engine: DataEngine = DataEngine.PANDAS):
        """"""
        super().__init__(ExchangeCode.BINANCE.value)

    def load_option_history(self, symbol: str, params: RequestParameters | None = None,
                            columns: list | None = None) -> pd.DataFrame:
        """load options history"""

    def load_future_history(self, symbol: str, params: RequestParameters | None = None,
                            columns: list | None = None) -> pd.DataFrame:
        """load futures history"""
