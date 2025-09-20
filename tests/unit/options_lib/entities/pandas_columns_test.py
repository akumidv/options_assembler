"""Pandas columns test"""
import datetime
import pandas as pd
from options_lib.entities._dataframe_columns import OptionsColumns, FuturesColumns


def test_opt_columns():
    assert OptionsColumns('timestamp')
    assert OptionsColumns('expiration_date')
    column = OptionsColumns('timestamp')
    assert hasattr(column, 'value')
    assert hasattr(column, 'nm')
    assert hasattr(column, 'type')
    assert column.type, pd.Timestamp


def test_prov_fut_columns():
    assert FuturesColumns('timestamp')
    assert FuturesColumns('expiration_date')
    column = FuturesColumns('timestamp')
    assert hasattr(column, 'value')
    assert hasattr(column, 'nm')
    assert hasattr(column, 'type')
    assert column.type, pd.Timestamp
