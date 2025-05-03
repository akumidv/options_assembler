"""Deribit exchange provider"""
import datetime
from pydantic import ValidationError
import pandas as pd
import pytest

from option_lib.entities import (
    Timeframe, AssetKind, OptionType, AssetType,
    OptionColumns as OCl,
    FuturesColumns as FCl,
    SpotColumns as SCl,
    ALL_COLUMN_NAMES
)
from exchange import RequestClass
from exchange import AbstractExchange
from exchange.moex import MoexOptions, MoexExchange, MoexAssetType
from functools import lru_cache

@pytest.fixture(name='moex_client')
def moex_options_fixture():
    """Moex options data client"""
    client = RequestClass(api_url=MoexExchange.TEST_API_URL)
    moex_market = MoexOptions(client)
    return moex_market


@pytest.fixture(name='moex_asset_code')
def moex_asset_code_fixture():
    return 'SI'


@pytest.fixture(name='moex_option_series_code')
@lru_cache
def moex_option_series_code_fixture(moex_client, moex_asset_code):
    opt_df = moex_client.get_option_series(asset_code=moex_asset_code)
    series_code = opt_df.iloc[0][OCl.SERIES_CODE.nm]
    print('##series_code', series_code)
    return series_code


def test_moex_market_init():
    client = RequestClass(api_url=MoexExchange.TEST_API_URL)
    deribit_market = MoexOptions(client)
    assert isinstance(deribit_market, MoexOptions)


def test_get_assets(moex_client, moex_asset_code):
    symbols_df = moex_client.get_assets()
    assert isinstance(symbols_df, pd.DataFrame)
    assert len(symbols_df) > 0
    assert OCl.BASE_CODE.nm in symbols_df.columns
    assert OCl.ASSET_TYPE.nm in symbols_df.columns
    assert not symbols_df[symbols_df[OCl.ASSET_TYPE.nm] == MoexAssetType.SHARE.code].empty
    assert not symbols_df[symbols_df[OCl.BASE_CODE.nm] == moex_asset_code].empty


def test_get_asset_info(moex_client, moex_asset_code):
    asset_data = moex_client.get_asset_info(asset_code=moex_asset_code, asset_type=MoexAssetType.FUTURES)
    assert isinstance(asset_data, pd.Series)
    assert OCl.BASE_CODE.nm in asset_data
    assert OCl.ASSET_TYPE.nm in asset_data
    assert asset_data[OCl.BASE_CODE.nm] == moex_asset_code


def test_get_asset_info_for_wrong_parameters_should_raise_error(moex_client, moex_asset_code):
    with pytest.raises(ValidationError):
        _ = moex_client.get_asset_info(asset_code=None)
    with pytest.raises(ValueError):
        _ = moex_client.get_asset_info(asset_code=moex_asset_code, asset_type='123')


def test_get_asset_futures(moex_client, moex_asset_code):
    asset_futures_df = moex_client.get_asset_futures(asset_code=moex_asset_code)
    assert isinstance(asset_futures_df, pd.DataFrame)
    assert len(asset_futures_df) > 0
    assert OCl.ASSET_CODE.nm in asset_futures_df.columns
    assert OCl.ASSET_TYPE.nm in asset_futures_df.columns
    assert OCl.BASE_CODE.nm in asset_futures_df.columns
    assert OCl.EXPIRATION_DATE.nm in asset_futures_df.columns
    assert not asset_futures_df[asset_futures_df[OCl.ASSET_TYPE.nm] == MoexAssetType.FUTURES.code].empty
    assert not asset_futures_df[asset_futures_df[OCl.BASE_CODE.nm] == moex_asset_code].empty


def test_get_asset_options(moex_client, moex_asset_code):
    opt_df = moex_client.get_asset_options(asset_code=moex_asset_code)
    assert isinstance(opt_df, pd.DataFrame)
    assert len(opt_df) > 0
    assert OCl.ASSET_CODE.nm in opt_df.columns
    assert OCl.UNDERLYING_CODE.nm in opt_df.columns
    assert OCl.UNDERLYING_TYPE.nm in opt_df.columns
    assert OCl.BASE_CODE.nm in opt_df.columns
    assert OCl.EXPIRATION_DATE.nm in opt_df.columns
    assert not opt_df[opt_df[OCl.UNDERLYING_TYPE.nm] == MoexAssetType.FUTURES.code].empty
    assert not opt_df[opt_df[OCl.BASE_CODE.nm] == moex_asset_code].empty


def test_get_asset_options_for_asset_wo_option(moex_client):
    asset_options_df = moex_client.get_asset_options(asset_code='AFLT')
    assert asset_options_df is None


def test_get_asset_option_series(moex_client, moex_asset_code):
    opt_df = moex_client.get_option_series(asset_code=moex_asset_code)
    assert isinstance(opt_df, pd.DataFrame)
    assert len(opt_df) > 0
    assert OCl.ASSET_CODE.nm in opt_df.columns
    assert OCl.UNDERLYING_CODE.nm in opt_df.columns
    assert OCl.UNDERLYING_TYPE.nm in opt_df.columns
    assert OCl.BASE_CODE.nm in opt_df.columns
    assert OCl.EXPIRATION_DATE.nm in opt_df.columns
    assert OCl.OPEN_INTEREST.nm in opt_df.columns
    assert OCl.VOLUME.nm in opt_df.columns
    assert not opt_df[opt_df[OCl.UNDERLYING_TYPE.nm] == MoexAssetType.FUTURES.code].empty
    assert not opt_df[opt_df[OCl.BASE_CODE.nm] == moex_asset_code].empty



def test_get_asset_option_series_list(moex_client, moex_asset_code, moex_option_series_code):
    opt_df = moex_client.get_option_series_list(asset_code=moex_asset_code, series_code=moex_option_series_code)
    assert isinstance(opt_df, pd.DataFrame)
    assert len(opt_df) > 0
    assert OCl.ASSET_CODE.nm in opt_df.columns
    assert OCl.UNDERLYING_CODE.nm in opt_df.columns
    assert OCl.UNDERLYING_TYPE.nm in opt_df.columns
    assert OCl.BASE_CODE.nm in opt_df.columns
    assert OCl.EXPIRATION_DATE.nm in opt_df.columns
    assert not opt_df[opt_df[OCl.UNDERLYING_TYPE.nm] == MoexAssetType.FUTURES.code].empty
    assert not opt_df[opt_df[OCl.BASE_CODE.nm] == moex_asset_code].empty


def test_get_option_series_desk(moex_client, moex_asset_code, moex_option_series_code):
    opt_df = moex_client.get_option_series_desk(asset_code=moex_asset_code, series_code=moex_option_series_code)
    assert isinstance(opt_df, pd.DataFrame)
    assert len(opt_df) > 0
    assert OCl.ASSET_CODE.nm in opt_df.columns
    assert OCl.BASE_CODE.nm in opt_df.columns
    assert OCl.EXPIRATION_DATE.nm in opt_df.columns
    assert not opt_df[opt_df[OCl.BASE_CODE.nm] == moex_asset_code].empty
