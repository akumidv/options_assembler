"""Tests for local provider"""
import pandas as pd
from option_lib.entities import ProviderOptionColumns, ProviderFuturesColumns


def test_load_option(exchange_provider, option_symbol, provider_params):
    df_opt = exchange_provider.load_option_history(symbol=option_symbol, params=provider_params)
    assert isinstance(df_opt, pd.DataFrame)
    assert all((p_ocl.nm in df_opt.columns for p_ocl in ProviderOptionColumns))


def test_load_future(exchange_provider, option_symbol, provider_params):
    df_fut = exchange_provider.load_future_history(symbol=option_symbol, params=provider_params)
    assert isinstance(df_fut, pd.DataFrame)
    assert all((f_col.nm in df_fut.columns for f_col in ProviderFuturesColumns))
