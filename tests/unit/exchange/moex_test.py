"""Deribit exchange provider"""
import pytest
import pandas as pd
from option_lib.entities import AssetType, OptionColumns as OCl
from provider import AbstractProvider
from exchange import AbstractExchange
from exchange.moex import MoexExchange


def test_moex_exchange_init():
    moex = MoexExchange()
    assert isinstance(moex, AbstractExchange)
    assert isinstance(moex, AbstractProvider)


def test_get_assets_list_future(moex_exchange, moex_asset_code):
    asset_type = AssetType.FUTURE
    assets = moex_exchange.get_assets_list(asset_type)
    assert isinstance(assets, list)
    assert len(assets) > 0
    assert moex_asset_code in assets


def test_get_assets_list_options(moex_exchange, moex_asset_code):
    asset_type = AssetType.OPTION
    assets = moex_exchange.get_assets_list(asset_type)
    assert isinstance(assets, list)
    assert len(assets) > 0
    assert moex_asset_code in assets


def test_get_options_assets_books_snapshot(moex_exchange, moex_asset_code):
    book_summary_df = moex_exchange.get_options_assets_books_snapshot(moex_asset_code)
    assert isinstance(book_summary_df, pd.DataFrame)
    assert len(book_summary_df) > 0
    assert OCl.BASE_CODE.nm in book_summary_df.columns
    assert not book_summary_df[book_summary_df[OCl.BASE_CODE.nm] == moex_asset_code].empty
