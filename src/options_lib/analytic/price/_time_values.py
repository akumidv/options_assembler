"""Option Price changes by time functions"""
import datetime

import pandas as pd

from options_lib.entities import OptionsColumns as OCl, OptionsType
from options_lib.chain import get_chain_atm_strike, select_chain, get_max_settlement_valid_expired_date
from options_lib.enrichment import add_intrinsic_and_time_value


def _calc_atm_distance(df_chain: pd.DataFrame, strike: float) -> float:
    atm_strike = get_chain_atm_strike(df_chain)
    if strike is None:
        strike = atm_strike
    distance = strike - atm_strike
    return distance


def _get_nearest_to_distance_strike(df_chain: pd.DataFrame, distance: float) -> pd.DataFrame:
    """Distance is absolute price value"""
    atm_strike = get_chain_atm_strike(df_chain)
    df_chain.loc[:, '_distance'] = (df_chain[OCl.STRIKE.nm] - atm_strike - distance).abs()
    return df_chain.loc[df_chain['_distance'] == df_chain['_distance'].min()]


def time_value_series_by_atm_distance(df_opt_fut_hist, distance: float | None = None,
                                      expiration_date: pd.Timestamp | None = None,
                                      option_type: OptionsType | None = OptionsType.CALL) -> pd.DataFrame:
    """
    expiration_date None - will be used nearest for last settlement_date in history dataframe
    distance - 0 will be used ATM Strike
    strike value - nearest with distance between strike and atm_strike
    """
    if distance is None:
        distance = 0
    if expiration_date is None:
        expiration_date = get_max_settlement_valid_expired_date(df_opt_fut_hist)
    df_hist = df_opt_fut_hist[(df_opt_fut_hist[OCl.EXPIRATION_DATE.nm] == expiration_date) & (
        df_opt_fut_hist[OCl.OPTION_TYPE.nm] == option_type.code)] \
        .sort_values(by=OCl.TIMESTAMP.nm).reset_index(drop=True).copy()
    if df_hist.empty:
        raise ValueError(f'No data found for expiration data {expiration_date} and option type {option_type.value}')
    if OCl.TIMED_VALUE.nm not in df_hist.columns:
        df_hist = add_intrinsic_and_time_value(df_hist)
    df_time_value_series = df_hist.groupby(OCl.TIMESTAMP.nm, group_keys=False).apply(_get_nearest_to_distance_strike,
                                                                                     distance)
    df_time_value_series.drop(columns=[col for col in df_time_value_series.columns if col not in [
        OCl.TIMESTAMP.nm, OCl.STRIKE.nm, OCl.TIMED_VALUE.nm]], inplace=True)
    return df_time_value_series


def time_value_series_by_strike_to_atm_distance(df_opt_fut_hist, strike: float | None = None,
                                                expiration_date: pd.Timestamp | None = None,
                                                option_type: OptionsType | None = OptionsType.CALL) -> pd.DataFrame:
    """
    expiration_date None - will be used nearest for last settlement_date in history dataframe
    strike None - will be used ATM Strike
    strike value - when strike 50 and futures is 73.6 - will be used nearest with distance between strike and atm_strike
    """
    df_cur_chain = select_chain(df_opt_fut_hist, expiation_date=expiration_date)
    if expiration_date is None:
        expiration_date = df_cur_chain[OCl.EXPIRATION_DATE.nm].min()
    distance = _calc_atm_distance(df_cur_chain, strike)
    del df_cur_chain
    df_time_value_series = time_value_series_by_atm_distance(df_opt_fut_hist, distance, expiration_date, option_type)
    return df_time_value_series


def time_value_series_for_strike(df_opt_fut_hist, strike: float | None = None,
                                 expiration_date: datetime.date | None = None,
                                 option_type: OptionsType | None = OptionsType.CALL) -> pd.DataFrame:
    """
    expiration_date None - will be used nearest for last settlement_date in history dataframe
    strike None - will be used ATM Strike
    strike value - when strike 50 and futures is 73.6 - will be used nearest with distance between strike and atm_strike
    """
    # TODO strike
    raise NotImplementedError
    # df_cur_chain = select_chain(df_opt_fut_hist, expiation_date=expiration_date)
    # if expiration_date is None:
    #     expiration_date = df_cur_chain.iloc[0][OCl.EXPIRATION_DATE.nm]
    # distance = _calc_atm_distance(df_cur_chain, strike)
    # del df_cur_chain
    # df_time_value_series = time_value_series_by_atm_distance(df_opt_fut_hist, distance, expiration_date, option_type)
    # return df_time_value_series
