"""Test for time values chart"""
import datetime

import pytest
from plotly import graph_objs as go

from option_lib.enrichment import add_intrinsic_and_time_value
from option_lib.enrichment import join_option_with_future
from options_assembler.chart.price.chart_price_class import ChartPriceClass

@pytest.fixture(name='chart_price')
def chart_price_class(option_data):
    """Price chart fixture"""
    chart_price = ChartPriceClass(option_data)
    return chart_price


def check_is_datetime_scatter(chart_fg, chart_len):
    """Checking chart format and data"""
    assert isinstance(chart_fg, list)
    assert isinstance(chart_fg[0], go.Scatter)
    assert hasattr(chart_fg[0], 'mode')
    assert hasattr(chart_fg[0], 'x')
    assert hasattr(chart_fg[0], 'y')
    assert len(chart_fg[0].x) >= chart_len
    assert len(chart_fg[0].y) >= chart_len
    assert isinstance(chart_fg[0].x[0], datetime.date)
    assert isinstance(chart_fg[0].y[0], float)


def test_time_values_for_missed_dataframe(chart_price, df_chain_exp_len):
    chart_fg = chart_price.time_values()
    check_is_datetime_scatter(chart_fg, df_chain_exp_len)


def test_time_values_for_dataframe_wo_time_values_columns(chart_price, option_data):
    with pytest.raises(KeyError):
        _ = chart_price.time_values(option_data.df_hist)


def test_time_values_for_dataframe(chart_price, df_opt_hist, df_fut_hist, df_chain_exp_len):
    df = join_option_with_future(df_opt_hist, df_fut_hist)
    df = add_intrinsic_and_time_value(df)
    chart_fg = chart_price.time_values(df)
    check_is_datetime_scatter(chart_fg, df_chain_exp_len)


def test_time_values_for_strike_and_missed_dataframe(chart_price, df_chain_exp_len, atm_strike):
    chart_fg = chart_price.time_values_for_strike(strike=atm_strike)
    check_is_datetime_scatter(chart_fg, df_chain_exp_len)
