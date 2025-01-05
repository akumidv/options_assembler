"""Provider column type test"""
import datetime

from option_lib.entities import ProviderOptionColumns, ProviderFuturesColumns


def test_prov_opt_columns():
    assert ProviderOptionColumns('datetime')
    assert ProviderOptionColumns('expiration_date')
    column = ProviderOptionColumns('datetime')
    assert hasattr(column, 'value')
    assert hasattr(column, 'nm')
    assert hasattr(column, 'type')
    assert column.type, datetime.date


def test_prov_fut_columns():
    assert ProviderFuturesColumns('datetime')
    assert ProviderFuturesColumns('expiration_date')
    column = ProviderFuturesColumns('datetime')
    assert hasattr(column, 'value')
    assert hasattr(column, 'nm')
    assert hasattr(column, 'type')
    assert column.type, datetime.date
