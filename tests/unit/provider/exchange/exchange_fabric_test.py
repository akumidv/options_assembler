"""Exchange fabric tests"""
from option_lib.provider.exchange.exchange_entities import ExchangeCode
from option_lib.provider import DataEngine
from option_lib.provider.exchange import (
    get_exchange, DeribitExchange, BinanceExchange
)


def test_get_exchange_binance():
    exchange_provider = get_exchange(ExchangeCode.BINANCE.value)
    assert isinstance(exchange_provider, BinanceExchange)
    assert not isinstance(exchange_provider, DeribitExchange)


def test_get_exchange_deribit():
    exchange_provider = get_exchange(ExchangeCode.DERIBIT.value, engine=DataEngine.PANDAS)
    assert isinstance(exchange_provider, DeribitExchange)
    assert not isinstance(exchange_provider, BinanceExchange)
