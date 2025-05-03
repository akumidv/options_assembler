"""Deribit exchange provider"""
import pytest
import pandas as pd
from option_lib.entities import AssetKind
from options_assembler.provider import AbstractProvider
from exchange import AbstractExchange
from exchange.moex import MoexExchange


def test_moex_exchange_init():
    moex = MoexExchange()
    assert isinstance(moex, AbstractExchange)
    assert isinstance(moex, AbstractProvider)


def test_get_assets_list_future(moex_client, moex_asset_code):
    asset_kind = AssetKind.FUTURES
    assets = moex_client.get_assets_list(asset_kind)
    print('##asstets', assets)
    assert isinstance(assets, list)
    assert len(assets) > 0
    assert moex_asset_code in assets


def test_get_symbols_books_snapshot(moex_client):
    book_summary_df = moex_client.get_symbols_books_snapshot()
    assert isinstance(book_summary_df, pd.DataFrame)
    assert len(book_summary_df) > 0
    assert 'base_currency' in book_summary_df.columns
    assert not book_summary_df[book_summary_df['base_currency'] == MoexExchange.CURRENCIES[0]].empty
