"""Tests for option class"""
import pandas as pd
import pytest

from option_lib import Option
from option_lib.entities import OptionColumns as OCl
from option_lib.provider import PandasLocalFileProvider


@pytest.fixture(name='option_instance')
def option_instance_fixture(exchange_provider, option_symbol, provider_params) -> Option:
    """Option instance"""
    opt = Option(exchange_provider, option_symbol, provider_params)
    return opt


def test_option_class_init(exchange_provider, option_symbol):
    opt = Option(exchange_provider, option_symbol)
    assert isinstance(opt, Option)


def test_option_class_df_opt(option_instance):
    assert isinstance(option_instance.df_hist, pd.DataFrame)
    assert all((col in PandasLocalFileProvider.option_columns for col in option_instance.df_hist.columns))


def test_option_class_with_extra_columns(exchange_provider, option_symbol, provider_params):
    columns = PandasLocalFileProvider.option_columns + ['iv', 'delta', 'gamma', 'vega', 'theta', 'quick_delta']
    opt = Option(exchange_provider, option_symbol, provider_params, option_columns=columns)
    assert isinstance(opt, Option)
    assert isinstance(opt.df_hist, pd.DataFrame)
    assert all((col in opt.df_hist.columns for col in columns))


def test_enrichment_add_future(option_instance):
    df_opt = option_instance.df_hist
    assert OCl.FUTURES_PRICE.nm not in df_opt.columns
    option_instance.enrichment.add_future()
    df_opt = option_instance.df_hist
    assert isinstance(df_opt, pd.DataFrame)
    assert OCl.FUTURES_PRICE.nm in df_opt.columns


def test_chain_select_chain(option_instance):
    df_brn_chain = option_instance.chain.select_chain()
    assert isinstance(df_brn_chain, pd.DataFrame)
    option_instance.chain.validate_chain(df_brn_chain)
