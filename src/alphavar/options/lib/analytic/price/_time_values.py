"""Option Price changes by time functions"""

import datetime

import pandas as pd
from pandera.typing import DataFrame

from alphavar.options.dictionary import OptionsTerm, OptionsType
from alphavar.options.lib.chain import get_chain_atm_strike, get_max_settlement_valid_expired_date, select_chain
from alphavar.options.lib.enrichment import add_intrinsic_and_time_value
from alphavar.options.schemas import TimeValueSeriesSchema


def _calc_atm_distance(df_chain: pd.DataFrame, strike: float) -> float:
    atm_strike = get_chain_atm_strike(df_chain)
    if strike is None:
        strike = atm_strike
    distance = strike - atm_strike
    return distance


def _get_nearest_to_distance_strike(df_chain: pd.DataFrame, distance: float) -> pd.DataFrame:
    """Distance is absolute price value"""
    atm_strike = get_chain_atm_strike(df_chain)
    df_chain.loc[:, "_distance"] = (df_chain[OptionsTerm.STRIKE] - atm_strike - distance).abs()
    return df_chain.loc[df_chain["_distance"] == df_chain["_distance"].min()]


def time_value_series_by_atm_distance(
    df_opt_fut_hist,
    distance: float | None = None,
    expiration_date: pd.Timestamp | None = None,
    option_type: OptionsType | None = OptionsType.CALL,
) -> DataFrame[TimeValueSeriesSchema]:
    """
    expiration_date None - will be used nearest for last settlement_date in history dataframe
    distance - 0 will be used ATM Strike
    strike value - nearest with distance between strike and atm_strike
    """
    if distance is None:
        distance = 0
    if expiration_date is None:
        expiration_date = get_max_settlement_valid_expired_date(df_opt_fut_hist)
    df_hist = (
        df_opt_fut_hist[
            (df_opt_fut_hist[OptionsTerm.EXPIRATION_DATE] == expiration_date)
            & (df_opt_fut_hist[OptionsTerm.OPTION_RIGHT] == option_type.value)
        ]
        .sort_values(by=OptionsTerm.TIMESTAMP)
        .reset_index(drop=True)
        .copy()
    )
    if df_hist.empty:
        raise ValueError(f"No data found for expiration data {expiration_date} and option type {option_type.value}")
    if OptionsTerm.TIMED_VALUE not in df_hist.columns:
        df_hist = add_intrinsic_and_time_value(df_hist)
    df_time_value_series = df_hist.groupby(OptionsTerm.TIMESTAMP, group_keys=False).apply(
        _get_nearest_to_distance_strike, distance
    )
    df_time_value_series.drop(
        columns=[
            col
            for col in df_time_value_series.columns
            if col not in [OptionsTerm.TIMESTAMP, OptionsTerm.STRIKE, OptionsTerm.TIMED_VALUE]
        ],
        inplace=True,
    )
    return df_time_value_series


def time_value_series_by_strike_to_atm_distance(
    df_opt_fut_hist,
    strike: float | None = None,
    expiration_date: pd.Timestamp | None = None,
    option_type: OptionsType | None = OptionsType.CALL,
) -> pd.DataFrame:
    """
    expiration_date None - will be used nearest for last settlement_date in history dataframe
    strike None - will be used ATM Strike
    strike value - when strike 50 and futures is 73.6 - will be used nearest with distance between strike and atm_strike
    """
    df_cur_chain = select_chain(df_opt_fut_hist, expiation_date=expiration_date)
    if expiration_date is None:
        expiration_date = df_cur_chain[OptionsTerm.EXPIRATION_DATE].min()
    distance = _calc_atm_distance(df_cur_chain, strike)
    del df_cur_chain
    df_time_value_series = time_value_series_by_atm_distance(df_opt_fut_hist, distance, expiration_date, option_type)
    return df_time_value_series


def time_value_series_for_strike(
    df_opt_fut_hist,
    strike: float | None = None,
    expiration_date: datetime.date | None = None,
    option_type: OptionsType | None = OptionsType.CALL,
) -> pd.DataFrame:
    """
    expiration_date None - will be used nearest for last settlement_date in history dataframe
    strike None - will be used ATM Strike
    strike value - when strike 50 and futures is 73.6 - will be used nearest with distance between strike and atm_strike
    """
    # TODO strike
    raise NotImplementedError
    # df_cur_chain = select_chain(df_opt_fut_hist, expiation_date=expiration_date)
    # if expiration_date is None:
    #     expiration_date = df_cur_chain.iloc[0][OptionsTerm.EXPIRATION_DATE]
    # distance = _calc_atm_distance(df_cur_chain, strike)
    # del df_cur_chain
    # df_time_value_series = time_value_series_by_atm_distance(df_opt_fut_hist, distance, expiration_date, option_type)
    # return df_time_value_series
