"""Deribit exchange provider"""
import pandas as pd
import pytest

from option_lib.provider.exchange import RequestClass
from option_lib.provider.exchange.deribit import DeribitMarket, DeribitExchange


@pytest.fixture(name='deribit_market')
def deribit_market_fixture():
    """Deribit market data client"""
    client = RequestClass(api_url=DeribitExchange.TEST_API_URL)
    deribit_market = DeribitMarket(client)
    return deribit_market


def test_deribit_market_init():
    client = RequestClass(api_url=DeribitExchange.TEST_API_URL)
    deribit_market = DeribitMarket(client)
    assert isinstance(deribit_market, DeribitMarket)


def test_get_instruments(deribit_market):
    symbols_df = deribit_market.get_instruments()
    assert isinstance(symbols_df, pd.DataFrame)
    assert len(symbols_df) > 0
    assert 'price_index' in symbols_df.columns
    assert not symbols_df[symbols_df['price_index'] == 'btc_usd'].empty


def test_get_book_summary_by_currency(deribit_market):
    book_summary_df = deribit_market.get_book_summary_by_currency(currency=DeribitExchange.CURRENCIES[0])
    assert isinstance(book_summary_df, pd.DataFrame)
    assert len(book_summary_df) > 0
    assert 'base_currency' in book_summary_df.columns
    assert not book_summary_df[book_summary_df['base_currency'] == DeribitExchange.CURRENCIES[0]].empty
