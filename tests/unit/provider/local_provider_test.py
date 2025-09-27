"""Tests for local provider"""
import pandas as pd
from provider import AbstractProvider


def test_load_option(exchange_provider, option_symbol, provider_params):
    df_opt = exchange_provider.load_options_history(asset_code=option_symbol, params=provider_params)
    assert isinstance(df_opt, pd.DataFrame)
    assert all((p_ocl in df_opt.columns for p_ocl in AbstractProvider.options_columns))


def test_load_future(exchange_provider, option_symbol, provider_params):
    df_fut = exchange_provider.load_futures_history(asset_code=option_symbol, params=provider_params)
    assert isinstance(df_fut, pd.DataFrame)
    assert all((f_col in df_fut.columns for f_col in AbstractProvider.futures_columns))
