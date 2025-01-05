"""Pandas columns test"""
import datetime
from option_lib.entities._pandas_columns import OptionColumns, FuturesColumns
from option_lib.entities._provider_pandas_columns import ProviderOptionColumns, ProviderFuturesColumns


def test_opt_columns():
    for opt_col in ProviderOptionColumns:
        assert opt_col.value in OptionColumns

    assert OptionColumns('datetime')
    assert OptionColumns('expiration_date')
    column = OptionColumns('datetime')
    assert hasattr(column, 'value')
    assert hasattr(column, 'nm')
    assert hasattr(column, 'type')
    assert column.type, datetime.date


def test_prov_fut_columns():
    for fut_col in ProviderFuturesColumns:
        assert fut_col.value in FuturesColumns
    assert FuturesColumns('datetime')
    assert FuturesColumns('expiration_date')
    column = FuturesColumns('datetime')
    assert hasattr(column, 'value')
    assert hasattr(column, 'nm')
    assert hasattr(column, 'type')
    assert column.type, datetime.date
