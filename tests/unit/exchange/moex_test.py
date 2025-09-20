"""Deribit exchange provider"""
import pandas as pd
from options_lib.entities import AssetKind, OptionsColumns as OCl
from provider import AbstractProvider
from exchange import AbstractExchange
from exchange.moex import MoexExchange


def test_moex_exchange_init():
    moex = MoexExchange()
    assert isinstance(moex, AbstractExchange)
    assert isinstance(moex, AbstractProvider)


def test_get_assets_list_future(moex_exchange, moex_asset_code):
    asset_kind = AssetKind.FUTURES
    assets = moex_exchange.get_assets_list(asset_kind)
    assert isinstance(assets, list)
    assert len(assets) > 0
    assert moex_asset_code in assets


def test_get_assets_list_options(moex_exchange, moex_asset_code):
    asset_kind = AssetKind.OPTIONS
    assets = moex_exchange.get_assets_list(asset_kind)
    assert isinstance(assets, list)
    assert len(assets) > 0
    assert moex_asset_code in assets


def test_get_options_assets_books_snapshot(moex_exchange, moex_asset_code):
    book_summary_df = moex_exchange.get_options_assets_books_snapshot(moex_asset_code)
    assert isinstance(book_summary_df, pd.DataFrame)
    assert len(book_summary_df) > 0
    assert OCl.BASE_CODE.nm in book_summary_df.columns
    assert not book_summary_df[book_summary_df[OCl.BASE_CODE.nm] == moex_asset_code].empty
