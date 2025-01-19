"""Pandas columns test"""
import datetime
from option_lib.entities._pandas_columns import OptionColumns, FuturesColumns


def test_opt_columns():
    assert OptionColumns('datetime')
    assert OptionColumns('expiration_date')
    column = OptionColumns('datetime')
    assert hasattr(column, 'value')
    assert hasattr(column, 'nm')
    assert hasattr(column, 'type')
    assert column.type, datetime.date


def test_prov_fut_columns():
    assert FuturesColumns('datetime')
    assert FuturesColumns('expiration_date')
    column = FuturesColumns('datetime')
    assert hasattr(column, 'value')
    assert hasattr(column, 'nm')
    assert hasattr(column, 'type')
    assert column.type, datetime.date
