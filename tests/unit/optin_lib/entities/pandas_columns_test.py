"""Pandas columns test"""
import datetime
import pandas as pd
from option_lib.entities._dataframe_columns import OptionColumns, FuturesColumns


def test_opt_columns():
    assert OptionColumns('timestamp')
    assert OptionColumns('expiration_date')
    column = OptionColumns('timestamp')
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
