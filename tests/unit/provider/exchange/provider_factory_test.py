"""Provider fabric tests"""
from option_lib.provider import get_provider, DataSource, DataEngine
from option_lib.provider.exchange import ExchangeCode
from option_lib.provider import (
    PandasLocalFileProvider
)
from option_lib.provider.exchange import BinanceExchange, DeribitExchange


def test_get_provider_local(exchange_code, data_path):
    provider = get_provider(exchange_code, storage=DataSource.LOCAL, data_path=data_path)
    assert isinstance(provider, PandasLocalFileProvider)


def test_get_provider_exchange_binance():
    provider = get_provider(ExchangeCode.BINANCE.value, storage=DataSource.API, engine=DataEngine.PANDAS)
    assert isinstance(provider, BinanceExchange)


def test_get_provider_exchange_deribit():
    provider = get_provider(ExchangeCode.DERIBIT.value, storage=DataSource.API, engine=DataEngine.PANDAS)
    assert isinstance(provider, DeribitExchange)
