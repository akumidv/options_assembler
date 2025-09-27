"""Test module for timeframe conversion function"""
import time

import pandas as pd
import pytest

from options_lib.dictionary import OptionsColumns as OCl, Timeframe, OptionsType
from options_lib.normalization.timeframe_resample import _get_group_columns_by_type, convert_to_timeframe


def test__get_group_columns_by_type_spot():
    df = pd.DataFrame({OCl.PRICE.nm: [123]})
    group_columns = _get_group_columns_by_type(df)
    assert group_columns == []
    df = pd.DataFrame({OCl.PRICE.nm: [123], OCl.EXPIRATION_DATE.nm: [pd.NA]})
    group_columns = _get_group_columns_by_type(df)
    assert group_columns == []
    df = pd.DataFrame({OCl.PRICE.nm: [123, 234], OCl.BASE_CODE.nm: ['BTC', 'ETH']})
    group_columns = _get_group_columns_by_type(df)
    assert group_columns == [OCl.BASE_CODE.nm]


def test__get_group_columns_by_type_future():
    fut_dict = {OCl.PRICE.nm: [123, 234], OCl.EXPIRATION_DATE.nm: [pd.Timestamp.now(), pd.Timestamp.now()]}
    df = pd.DataFrame(fut_dict)
    group_columns = _get_group_columns_by_type(df)
    assert group_columns == [OCl.EXPIRATION_DATE.nm]
    fut_dict.update({OCl.STRIKE.nm: [pd.NA, None]})
    df = pd.DataFrame.from_dict(fut_dict)
    group_columns = _get_group_columns_by_type(df)
    assert group_columns == [OCl.EXPIRATION_DATE.nm]
    fut_dict.update({OCl.BASE_CODE.nm: ['BTC', 'ETH']})
    df = pd.DataFrame(fut_dict)
    group_columns = _get_group_columns_by_type(df)
    assert group_columns == [OCl.BASE_CODE.nm, OCl.EXPIRATION_DATE.nm]


def test__get_group_columns_by_type_option():
    opt_dict = {OCl.PRICE.nm: [123, 234], OCl.EXPIRATION_DATE.nm: [pd.Timestamp.now(), pd.Timestamp.now()],
                OCl.OPTION_TYPE.nm: [OptionsType.CALL.code, OptionsType.PUT.code],
                OCl.STRIKE.nm: [1000, 1200]}
    df = pd.DataFrame(opt_dict)
    group_columns = _get_group_columns_by_type(df)
    assert group_columns == [OCl.EXPIRATION_DATE.nm, OCl.OPTION_TYPE.nm, OCl.STRIKE.nm]
    opt_dict.update({OCl.BASE_CODE.nm: ['BTC', 'ETH']})
    df = pd.DataFrame(opt_dict)
    group_columns = _get_group_columns_by_type(df)
    assert group_columns == [OCl.BASE_CODE.nm, OCl.EXPIRATION_DATE.nm, OCl.OPTION_TYPE.nm, OCl.STRIKE.nm]


def test__get_group_columns_by_type_wrong_option():
    opt_dict = {OCl.PRICE.nm: [123, 234], OCl.EXPIRATION_DATE.nm: [pd.Timestamp.now(), pd.Timestamp.now()],
                OCl.OPTION_TYPE.nm: [OptionsType.CALL.code, pd.NA],
                OCl.STRIKE.nm: [1000, 1200]}
    df = pd.DataFrame(opt_dict)
    with pytest.raises(ValueError):
        _ = _get_group_columns_by_type(df)
    opt_dict = {OCl.PRICE.nm: [123, 234], OCl.EXPIRATION_DATE.nm: [pd.Timestamp.now(), pd.Timestamp.now()],
                OCl.OPTION_TYPE.nm: [OptionsType.CALL.code, OptionsType.PUT.code],
                OCl.STRIKE.nm: [1000, None]}
    df = pd.DataFrame(opt_dict)
    with pytest.raises(ValueError):
        _ = _get_group_columns_by_type(df)


def test_convert_to_timeframe_future(future_update_files):
    dfs = []
    for fn in future_update_files[:5]:
        dfs.append(pd.read_parquet(fn))
    df = pd.concat(dfs)
    df_new_tf = convert_to_timeframe(df, Timeframe.EOD)
    assert len(df_new_tf) != len(df)
    assert len(df_new_tf[OCl.EXPIRATION_DATE.nm].unique()) == len(df[OCl.EXPIRATION_DATE.nm].unique())
    assert len(df_new_tf[OCl.ASSET_CODE.nm].unique()) == len(df[OCl.ASSET_CODE.nm].unique())


def test_convert_to_timeframe_option(option_update_files):
    dfs = []
    for fn in option_update_files[:5]:
        dfs.append(pd.read_parquet(fn))
    df = pd.concat(dfs, ignore_index=True)
    df_new_tf = convert_to_timeframe(df, Timeframe.EOD)
    assert len(df_new_tf) != len(df)
    assert len(df_new_tf[OCl.EXPIRATION_DATE.nm].unique()) == len(df[OCl.EXPIRATION_DATE.nm].unique())
    assert len(df_new_tf[OCl.ASSET_CODE.nm].unique()) == len(df[OCl.ASSET_CODE.nm].unique())


def test_convert_to_timeframe_option_by_type(option_update_files):
    dfs = []
    for fn in option_update_files[:5]:
        dfs.append(pd.read_parquet(fn))
    df = pd.concat(dfs)
    df_new_tf = convert_to_timeframe(df, Timeframe.EOD, by_exchange_symbol=False)
    assert len(df_new_tf) != len(df)
    assert len(df_new_tf[OCl.EXPIRATION_DATE.nm].unique()) == len(df[OCl.EXPIRATION_DATE.nm].unique())
    assert len(df_new_tf[OCl.ASSET_CODE.nm].unique()) == len(df[OCl.ASSET_CODE.nm].unique())
