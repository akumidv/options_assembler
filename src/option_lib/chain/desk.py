"""
Prepare desk for option chain
"""

from option_lib.entities import OptionType
from option_lib.entities import OptionColumns as OCl


def convert_chain_to_desk(df_chain, option_columns: list | None = None, future_columns: list | None = None):
    """Prepare option desk from options or option_chain"""
    if option_columns is None:
        option_columns = [OCl.DATETIME.nm, OCl.EXPIRATION_DATE.nm, OCl.STRIKE.nm, OCl.PREMIUM.nm]
    append_col = [col for col in [OCl.DATETIME.nm, OCl.EXPIRATION_DATE.nm, OCl.STRIKE.nm
                                  ] if col not in option_columns]
    if len(append_col):
        option_columns += append_col
    option_columns = [col for col in option_columns if col in df_chain.columns]
    if future_columns is None:
        future_columns = [col for col in [OCl.FUTURES_PRICE.nm, OCl.FUTURES_EXPIRATION_DATE.nm
                                          ] if col in df_chain.columns]
    future_columns = [col for col in future_columns if col in df_chain.columns]

    df_hist_desk = df_chain[df_chain[OCl.TYPE.nm] == OptionType.CALL.code][option_columns] \
        .merge(df_chain[df_chain[OCl.TYPE.nm] == OptionType.PUT.code][option_columns],
               on=[OCl.DATETIME.nm, OCl.EXPIRATION_DATE.nm, OCl.STRIKE.nm], suffixes=['_call', '_put'], how='outer')\
        .sort_values(by=OCl.STRIKE.nm)
    if future_columns:
        df_fut = df_chain[[OCl.DATETIME.nm, OCl.EXPIRATION_DATE.nm] + future_columns].drop_duplicates()
        df_hist_desk = df_hist_desk\
            .merge(df_fut, on=[OCl.DATETIME.nm, OCl.EXPIRATION_DATE.nm], how='left')
    return df_hist_desk
