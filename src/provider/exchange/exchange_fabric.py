import enum
from typing import Dict, Type
from option_lib.provider import DataEngine
from option_lib.provider.exchange import AbstractExchange
from option_lib.provider.exchange.binance import BinanceExchange
from option_lib.provider.exchange.deribit import DeribitExchange
from option_lib.provider.exchange.exchange_entities import ExchangeCode


_EXCHANGES: Dict[ExchangeCode, Type[AbstractExchange]] = {
    ExchangeCode.BINANCE: BinanceExchange,
    ExchangeCode.DERIBIT: DeribitExchange
}


def get_exchange(exchange_code: str, engine: DataEngine=DataEngine.PANDAS, **kwargs) -> AbstractExchange:
    """Fabric"""
    exchange = ExchangeCode(exchange_code)

    return _EXCHANGES[exchange](engine, **kwargs)
