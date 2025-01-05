"""Deribit exchange provider"""
from option_lib.provider import AbstractProvider
from option_lib.provider.exchange import AbstractExchange
from option_lib.provider.exchange.deribit import DeribitExchange


def test_binance_exchange_init():
    deribit = DeribitExchange()
    assert isinstance(deribit, AbstractExchange)
    assert isinstance(deribit, AbstractProvider)
