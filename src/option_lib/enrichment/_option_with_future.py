"""
Add necessary future value to options
"""
import pandas as pd
from option_lib.entities import OptionColumns as OCl, FuturesColumns as FCl


def join_option_with_future(df_hist: pd.DataFrame, df_fut: pd.DataFrame) -> pd.DataFrame:
    """Join futures column to correspond options"""
    df_fut = df_fut.rename(columns={FCl.PRICE.nm: OCl.FUTURES_PRICE.nm,
                                    FCl.EXPIRATION_DATE.nm: OCl.FUTURES_EXPIRATION_DATE.nm})
    df_ext_opt = df_hist.merge(df_fut, on=[OCl.DATETIME.nm, OCl.FUTURES_EXPIRATION_DATE.nm], how='left')
    return df_ext_opt
