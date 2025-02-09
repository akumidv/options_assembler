"""Price values corrections"""
import pandas as pd
from option_lib.entities import (
    OptionColumns as OCl
)


def fill_option_price(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fill price value if it is nit exist by follow logic:
    price =
        - last
        - or avg bis/ask
        - or avg bid/high
        - or avg low/ask
        - or avg low/high
        - or any if only one present of ask, bid, high, low
    """
    if OCl.PRICE.nm in df.columns and df[OCl.PRICE.nm].notnull().all():
        return df
    price_col = f'__{OCl.PRICE.nm}'
    if OCl.LAST.nm in df.columns:
        df[price_col] = df[OCl.LAST.nm]
    else:
        df[price_col] = pd.NA
    if OCl.ASK.nm in df.columns and OCl.BID.nm in df.columns and df[price_col].isnull().any():
        mid_price_col = f'__mid_{OCl.PRICE.nm}'
        low_price_col = f'__low_{OCl.PRICE.nm}'
        df[mid_price_col] = df[OCl.ASK.nm]
        df[low_price_col] = df[OCl.BID.nm]
        if OCl.HIGH.nm in df.columns:
            df[mid_price_col].fillna(df[OCl.HIGH.nm], inplace=True)
        if OCl.LOW.nm in df.columns:
            df[low_price_col].fillna(df[OCl.LOW.nm], inplace=True)
        df[mid_price_col].fillna(df[low_price_col], inplace=True)
        df[low_price_col].fillna(df[mid_price_col], inplace=True)
        df.loc[:, mid_price_col] = df.loc[:, [mid_price_col, low_price_col]].mean(axis='columns')
        df[price_col].fillna(df[mid_price_col], inplace=True)
        df.drop(columns=[mid_price_col, low_price_col], inplace=True)
    if OCl.PRICE.nm in df.columns:
        df = df[OCl.PRICE.nm].fillna(df[price_col]).drop(columns=price_col)
    else:
        df.rename(columns={price_col: OCl.PRICE.nm}, inplace=True)
    return df
