"""Tests for time values analysis"""
import pandas as pd

from option_lib.analytic.price._time_values import (
    time_value_series_by_strike_to_atm_distance, time_value_series_by_atm_distance
)
from option_lib.entities import OptionColumns as OCl



def test_time_value_changes_from_strike_to_atm_distance_atm(df_opt_hist, df_chain_exp_len):
    df_time_values = time_value_series_by_strike_to_atm_distance(df_opt_hist)
    assert isinstance(df_time_values, pd.DataFrame)
    assert OCl.TIMESTAMP.nm in df_time_values.columns
    assert OCl.STRIKE.nm in df_time_values.columns
    assert OCl.TIME_VALUE.nm in df_time_values.columns
    assert len(df_time_values) >= df_chain_exp_len


def test_time_value_changes_from_strike_to_atm_distance_strike_itm(df_opt_hist, df_chain_exp_len, atm_strike):
    df_time_values = time_value_series_by_strike_to_atm_distance(df_opt_hist, strike=atm_strike - 20)
    assert isinstance(df_time_values, pd.DataFrame)

    assert OCl.TIMESTAMP.nm in df_time_values.columns
    assert OCl.STRIKE.nm in df_time_values.columns
    assert OCl.TIME_VALUE.nm in df_time_values.columns
    assert len(df_time_values) >= df_chain_exp_len


def test_time_value_changes_from_strike_to_atm_distance_strike_otm(df_opt_hist, df_chain_exp_len, atm_strike):
    df_time_values = time_value_series_by_strike_to_atm_distance(df_opt_hist, strike=atm_strike + 20)
    assert isinstance(df_time_values, pd.DataFrame)
    assert OCl.TIMESTAMP.nm in df_time_values.columns
    assert OCl.STRIKE.nm in df_time_values.columns
    assert OCl.TIME_VALUE.nm in df_time_values.columns
    assert len(df_time_values) >= df_chain_exp_len


def test_time_value_series_by_atm_distance(df_opt_hist, df_chain_exp_len):
    df_time_values = time_value_series_by_atm_distance(df_opt_hist, distance=10)
    assert isinstance(df_time_values, pd.DataFrame)
    assert OCl.TIMESTAMP.nm in df_time_values.columns
    assert OCl.STRIKE.nm in df_time_values.columns
    assert OCl.TIME_VALUE.nm in df_time_values.columns
    assert len(df_time_values) >= df_chain_exp_len
