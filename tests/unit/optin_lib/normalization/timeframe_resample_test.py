"""Test module for timeframe conversion function"""
import pandas as pd
import pytest

from option_lib.entities import OptionColumns as OCl, Timeframe, OptionType
from option_lib.normalization.timeframe_resample import _get_group_columns_by_type


def test__get_group_columns_by_type_spot():
    df = pd.DataFrame({OCl.PRICE.nm: [123]})
    group_columns = _get_group_columns_by_type(df)
    assert group_columns == []
    df = pd.DataFrame({OCl.PRICE.nm: [123], OCl.EXPIRATION_DATE.nm: [pd.NA]})
    group_columns = _get_group_columns_by_type(df)
    assert group_columns == []
    df = pd.DataFrame({OCl.PRICE.nm: [123, 234], OCl.SYMBOL.nm: ['BTC', 'ETH']})
    group_columns = _get_group_columns_by_type(df)
    assert group_columns == [OCl.SYMBOL.nm]


def test__get_group_columns_by_type_future():
    fut_dict = {OCl.PRICE.nm: [123, 234], OCl.EXPIRATION_DATE.nm: [pd.Timestamp.now(), pd.Timestamp.now()]}
    df = pd.DataFrame(fut_dict)
    group_columns = _get_group_columns_by_type(df)
    assert group_columns == [OCl.EXPIRATION_DATE.nm]
    fut_dict.update({OCl.STRIKE.nm: [pd.NA, None]})
    df = pd.DataFrame.from_dict(fut_dict)
    group_columns = _get_group_columns_by_type(df)
    assert group_columns == [OCl.EXPIRATION_DATE.nm]
    fut_dict.update({OCl.SYMBOL.nm: ['BTC', 'ETH']})
    df = pd.DataFrame(fut_dict)
    group_columns = _get_group_columns_by_type(df)
    assert group_columns == [OCl.SYMBOL.nm, OCl.EXPIRATION_DATE.nm]


def test__get_group_columns_by_type_option():
    opt_dict = {OCl.PRICE.nm: [123, 234], OCl.EXPIRATION_DATE.nm: [pd.Timestamp.now(), pd.Timestamp.now()],
                OCl.OPTION_TYPE.nm: [OptionType.CALL.code, OptionType.PUT.code],
                OCl.STRIKE.nm: [1000, 1200]}
    df = pd.DataFrame(opt_dict)
    group_columns = _get_group_columns_by_type(df)
    assert group_columns == [OCl.EXPIRATION_DATE.nm, OCl.OPTION_TYPE, OCl.STRIKE.nm]
    opt_dict.update({OCl.SYMBOL.nm: ['BTC', 'ETH']})
    df = pd.DataFrame(opt_dict)
    group_columns = _get_group_columns_by_type(df)
    assert group_columns == [OCl.SYMBOL.nm, OCl.EXPIRATION_DATE.nm, OCl.OPTION_TYPE, OCl.STRIKE.nm]


def test__get_group_columns_by_type_wrong_option():
    opt_dict = {OCl.PRICE.nm: [123, 234], OCl.EXPIRATION_DATE.nm: [pd.Timestamp.now(), pd.Timestamp.now()],
                OCl.OPTION_TYPE.nm: [OptionType.CALL.code, pd.NA],
                OCl.STRIKE.nm: [1000, 1200]}
    df = pd.DataFrame(opt_dict)
    with pytest.raises(ValueError):
        _ = _get_group_columns_by_type(df)
    opt_dict = {OCl.PRICE.nm: [123, 234], OCl.EXPIRATION_DATE.nm: [pd.Timestamp.now(), pd.Timestamp.now()],
                OCl.OPTION_TYPE.nm: [OptionType.CALL.code, OptionType.PUT.code],
                OCl.STRIKE.nm: [1000, None]}
    df = pd.DataFrame(opt_dict)
    with pytest.raises(ValueError):
        _ = _get_group_columns_by_type(df)
