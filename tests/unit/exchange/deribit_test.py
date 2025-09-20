"""Deribit exchange provider"""
import pytest
import pandas as pd
from options_lib.entities import AssetKind
from provider import AbstractProvider
from exchange import AbstractExchange
from exchange.deribit import DeribitExchange


def test_deribit_exchange_init():
    deribit = DeribitExchange()
    assert isinstance(deribit, AbstractExchange)
    assert isinstance(deribit, AbstractProvider)


def test_get_symbols_list_future(deribit_client):
    asset_kind = AssetKind.FUTURES
    symbols = deribit_client.get_assets_list(asset_kind)
    assert isinstance(symbols, list)
    assert len(symbols) > 0
    assert 'BTC_USD' in symbols


def test_get_symbols_books_snapshot(deribit_client):
    book_summary_df = deribit_client.get_options_assets_books_snapshot()
    print(book_summary_df)
    assert isinstance(book_summary_df, pd.DataFrame)
    assert len(book_summary_df) > 0
    assert 'base_currency' in book_summary_df.columns
    assert not book_summary_df[book_summary_df['base_currency'] == DeribitExchange.CURRENCIES[0]].empty
