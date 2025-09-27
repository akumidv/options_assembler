"""Price values corrections"""
import pandas as pd
from options_lib.dictionary import (
    OptionsColumns as OCl
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
            # df[mid_price_col].fillna(df[OCl.HIGH.nm], inplace=True)
            df.fillna(value={mid_price_col: df[OCl.HIGH.nm]}, inplace=True)
        if OCl.LOW.nm in df.columns:
            # df[low_price_col].fillna(df[OCl.LOW.nm], inplace=True)
            df.fillna(value={low_price_col: df[OCl.LOW.nm]}, inplace=True)
        # df[mid_price_col].fillna(df[low_price_col], inplace=True)
        df.fillna(value={mid_price_col: df[low_price_col]}, inplace=True)
        # df[low_price_col].fillna(df[mid_price_col], inplace=True)
        df.fillna(value={low_price_col: df[mid_price_col]}, inplace=True)
        df.loc[:, mid_price_col] = df.loc[:, [mid_price_col, low_price_col]].mean(axis='columns')
        # df[price_col].fillna(df[mid_price_col], inplace=True)
        df.fillna(value={price_col: df[mid_price_col]}, inplace=True)
        df.drop(columns=[mid_price_col, low_price_col], inplace=True)
    if OCl.PRICE.nm in df.columns:
        #  value is trying to be set on a copy of a DataFrame or Series through chained assignment using an inplace method.
        # The behavior will change in pandas 3.0. This inplace method will never work because the intermediate object on which we are setting values always behaves as a copy.
        # For example, when doing 'df[col].method(value, inplace=True)', try using 'df.method({col: value}, inplace=True)' or df[col] = df[col].method(value) instead, to perform the operation inplace on the original object.
        # df = df[OCl.PRICE.nm].fillna(df[price_col]).drop(columns=price_col)
        df = df.fillna(value={OCl.PRICE.nm: df[price_col]}).drop(columns=price_col)
    else:
        df.rename(columns={price_col: OCl.PRICE.nm}, inplace=True)
    return df
