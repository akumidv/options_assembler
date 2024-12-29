"""
Add necessary future value to options
"""
import pandas as pd


def join_option_with_future(df_opt: pd.DataFrame, df_fut: pd.DataFrame) -> pd.DataFrame:
    """Join futures column to correspond options"""
    df_fut = df_fut.rename(columns={'price': 'future_price', 'expiration_date': 'future_expiration_date'})
    df_ext_opt = df_opt.merge(df_fut, on=['datetime', 'future_expiration_date'], how='left')
    return df_ext_opt

