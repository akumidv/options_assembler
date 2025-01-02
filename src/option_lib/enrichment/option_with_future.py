"""
Add necessary future value to options
"""
import pandas as pd
from option_lib.entities import OptionColumns as OCl, FutureColumns as FCl


def join_option_with_future(df_opt: pd.DataFrame, df_fut: pd.DataFrame) -> pd.DataFrame:
    """Join futures column to correspond options"""
    df_fut = df_fut.rename(columns={FCl.PRICE.col: OCl.FUTURE_PRICE.col,
                                    FCl.EXPIRATION_DATE.col: OCl.FUTURE_EXPIRATION_DATE.col})
    df_ext_opt = df_opt.merge(df_fut, on=[OCl.DATETIME.col, OCl.FUTURE_EXPIRATION_DATE.col], how='left')
    return df_ext_opt
