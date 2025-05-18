"""Pandas columns test"""
import datetime
import pandas as pd
from option_lib.entities._dataframe_columns import OptionColumns, FutureColumns


def test_opt_columns():
    assert OptionColumns('timestamp')
    assert OptionColumns('expiration_date')
    column = OptionColumns('timestamp')
    assert hasattr(column, 'value')
    assert hasattr(column, 'nm')
    assert hasattr(column, 'type')
    assert column.type, pd.Timestamp


def test_prov_fut_columns():
    assert FutureColumns('timestamp')
    assert FutureColumns('expiration_date')
    column = FutureColumns('timestamp')
    assert hasattr(column, 'value')
    assert hasattr(column, 'nm')
    assert hasattr(column, 'type')
    assert column.type, pd.Timestamp
