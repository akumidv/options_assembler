"""Fabric to choose exchange by it name"""

from typing import Dict, Type
from provider import DataEngine
from exchange._abstract_exchange import AbstractExchange
from exchange.exchange_entities import ExchangeCode
from exchange.binance import BinanceExchange
from exchange.deribit import DeribitExchange
from exchange.moex import MoexExchange


_EXCHANGES: Dict[ExchangeCode, Type[AbstractExchange]] = {
    ExchangeCode.BINANCE: BinanceExchange,
    ExchangeCode.DERIBIT: DeribitExchange,
    ExchangeCode.MOEX: MoexExchange
}


def get_exchange(exchange_code: str, engine: DataEngine=DataEngine.PANDAS, **kwargs) -> AbstractExchange:
    """Fabric"""
    exchange = ExchangeCode(exchange_code)

    return _EXCHANGES[exchange](engine, **kwargs)
