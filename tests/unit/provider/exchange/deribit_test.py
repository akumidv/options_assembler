"""Deribit exchange provider"""
import pytest
import pandas as pd
from option_lib.entities import AssetKind
from option_lib.provider import AbstractProvider
from option_lib.provider.exchange import AbstractExchange, BookData
from option_lib.provider.exchange.deribit import DeribitExchange


def test_deribit_exchange_init():
    deribit = DeribitExchange()
    assert isinstance(deribit, AbstractExchange)
    assert isinstance(deribit, AbstractProvider)


def test_get_symbols_list_future(deribit_client):
    asset_kind = AssetKind.FUTURE
    symbols = deribit_client.get_symbols_list(asset_kind)
    assert isinstance(symbols, list)
    assert len(symbols) > 0
    assert 'BTC_USD' in symbols


def test_get_symbols_books_snapshot(deribit_client):
    book_summary_df = deribit_client.get_symbols_books_snapshot()
    assert isinstance(book_summary_df, pd.DataFrame)
    assert len(book_summary_df) > 0
    assert 'base_currency' in book_summary_df.columns
    assert not book_summary_df[book_summary_df['base_currency'] == DeribitExchange.CURRENCIES[0]].empty
