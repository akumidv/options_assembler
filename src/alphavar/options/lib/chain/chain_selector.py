"""Chain selection and verification module"""

import datetime

import pandas as pd
from pandera.typing import DataFrame

from alphavar.options.dictionary import OptionsTerm
from alphavar.options.schemas import ChainSchema


def select_chain(
    df_hist: pd.DataFrame, settlement_date: pd.Timestamp | None = None, expiation_date: pd.Timestamp | None = None
) -> DataFrame[ChainSchema]:
    """Select for chain dataframe. If parameters do not set - it return chain for nearest actual expiration date"""
    if df_hist is None:
        raise ValueError("Option dataframe should be provided")
    if settlement_date is None:
        settlement_date = df_hist[OptionsTerm.TIMESTAMP].max()
    df_chain = df_hist[
        (df_hist[OptionsTerm.TIMESTAMP] == settlement_date) & (df_hist[OptionsTerm.EXPIRATION_DATE] >= settlement_date)
    ]  # For crypto data can contain expired date
    if df_chain.empty:
        raise ValueError(f"{OptionsTerm.TIMESTAMP} {settlement_date.isoformat()} is not present in option dataframe")
    if expiation_date is None:
        expiation_date = df_chain[OptionsTerm.EXPIRATION_DATE].min()
    df_chain = df_chain[df_chain[OptionsTerm.EXPIRATION_DATE] == expiation_date]

    if df_chain.empty:
        raise ValueError(f"{OptionsTerm.TIMESTAMP} {settlement_date.isoformat()} is not present in option dataframe")
    return df_chain.copy()


def get_max_settlement_valid_expired_date(df_opt_fut_hist: pd.DataFrame) -> pd.Timestamp:
    """In chain search expired date after or equal settlement date"""
    df_chain = select_chain(df_opt_fut_hist)
    expiration_date = df_chain[OptionsTerm.EXPIRATION_DATE].min()
    del df_chain
    return expiration_date


def get_settlement_longest_period_expired_date(
    df_opt_fut_hist: pd.DataFrame, settlement_date: pd.Timestamp | None = None
) -> pd.Timestamp:
    """In history dataframe foe settlement date search expiration date with longest series of data"""
    if settlement_date is None:
        settlement_date = df_opt_fut_hist[OptionsTerm.TIMESTAMP].max()
    else:
        df_opt_fut_hist = df_opt_fut_hist[df_opt_fut_hist[OptionsTerm.TIMESTAMP] < settlement_date]
    expiration_date = (
        df_opt_fut_hist[df_opt_fut_hist[OptionsTerm.EXPIRATION_DATE] >= settlement_date]
        .drop_duplicates(subset=[OptionsTerm.TIMESTAMP, OptionsTerm.EXPIRATION_DATE])
        .groupby([OptionsTerm.EXPIRATION_DATE])[OptionsTerm.EXPIRATION_DATE]
        .count()
        .idxmax()
    )
    return expiration_date


def validate_chain(df_chain: pd.DataFrame):
    """Validate is dataframe chain"""
    if not isinstance(df_chain, pd.DataFrame):
        raise TypeError("Chain is not dataframe")
    if df_chain.empty:
        raise ValueError("Chain dataframe is empty")
    if len(df_chain[OptionsTerm.TIMESTAMP].unique()) != 1:
        raise ValueError(f"Chain contain data more than one {OptionsTerm.TIMESTAMP}")
    if len(df_chain[OptionsTerm.EXPIRATION_DATE].unique()) != 1:
        raise ValueError(f"Chain contain options for more than one {OptionsTerm.EXPIRATION_DATE}")


def get_chain_settlement_and_expiration_date(df_chain: pd.DataFrame) -> tuple[datetime.date, datetime.date]:
    """Get settlement and expiration dates"""
    row = df_chain.iloc[0]
    return row[OptionsTerm.TIMESTAMP], row[OptionsTerm.EXPIRATION_DATE]
