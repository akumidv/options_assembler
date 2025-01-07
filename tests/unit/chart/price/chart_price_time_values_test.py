"""Test for time values chart"""
import datetime

import pytest
from plotly import graph_objs as go
from option_lib.chart.price.chart_price_class import ChartPriceClass
from option_lib.enrichment import add_intrinsic_and_time_value
from option_lib.enrichment import join_option_with_future


@pytest.fixture(name='chart_price')
def chart_price_class(option_data):
    """Price chart fixture"""
    chart_price = ChartPriceClass(option_data)
    return chart_price


def check_is_datetime_scatter(chart_fg):
    """Checking chart format and data"""
    assert isinstance(chart_fg, list)
    assert isinstance(chart_fg[0], go.Scatter)
    assert hasattr(chart_fg[0], 'mode')
    assert hasattr(chart_fg[0], 'x')
    assert hasattr(chart_fg[0], 'y')
    assert len(chart_fg[0].x) > 10
    assert len(chart_fg[0].y) > 10
    assert isinstance(chart_fg[0].x[0], datetime.date)
    assert isinstance(chart_fg[0].y[0], float)


def test_time_values_for_missed_dataframe(chart_price):
    chart_fg = chart_price.time_values()
    check_is_datetime_scatter(chart_fg)


def test_time_values_for_dataframe_wo_time_values_columns(chart_price, option_data):
    with pytest.raises(KeyError):
        _ = chart_price.time_values(option_data.df_hist)


def test_time_values_for_dataframe(chart_price, df_brn_hist, df_brn_fut):
    df = join_option_with_future(df_brn_hist, df_brn_fut)
    df = add_intrinsic_and_time_value(df)
    chart_fg = chart_price.time_values(df)
    check_is_datetime_scatter(chart_fg)


def test_time_values_for_strike_and_missed_dataframe(chart_price, atm_strike):
    chart_fg = chart_price.time_values_for_strike(strike=atm_strike)
    check_is_datetime_scatter(chart_fg)
