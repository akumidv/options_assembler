"""Binance exchange provider"""

from option_lib.provider import AbstractProvider
from exchange import AbstractExchange
from exchange.binance import BinanceExchange


def test_binance_exchange_init():
    binance = BinanceExchange()
    assert isinstance(binance, AbstractExchange)
    assert isinstance(binance, AbstractProvider)
