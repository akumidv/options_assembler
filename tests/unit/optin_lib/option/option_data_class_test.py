"""Tests for OptionData class"""
import pandas as pd
from options_assembler.option_data_class import OptionData
from options_assembler.provider import PandasLocalFileProvider


def test_option_data_class_init(exchange_provider, option_symbol):
    opt = OptionData(exchange_provider, option_symbol)
    assert isinstance(opt, OptionData)


def test_option_data_class_df_opt(exchange_provider, option_symbol, provider_params):
    opt = OptionData(exchange_provider, option_symbol, provider_params)
    assert isinstance(opt, OptionData)
    assert isinstance(opt.df_hist, pd.DataFrame)
    assert all((col in PandasLocalFileProvider.option_columns for col in opt.df_hist.columns))


def test_option_data_class_df_fut(exchange_provider, option_symbol, provider_params):
    opt = OptionData(exchange_provider, option_symbol, provider_params)
    assert isinstance(opt, OptionData)
    assert isinstance(opt.df_fut, pd.DataFrame)
    assert all((col in PandasLocalFileProvider.future_columns for col in opt.df_fut.columns))
