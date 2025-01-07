"""
Internal realization for option money data enrichment
"""

import pandas as pd
from option_lib.entities import OptionColumns as OCl, OptionType, OptionPriceStatus
from option_lib.chain import (
    get_chain_atm_itm_otm
)


def add_intrinsic_and_time_value(df_hist):
    """
    Adding columns with intrinsic value and time value
    """

    df_hist.loc[:, OCl.INTRINSIC_VALUE.nm] = 0.
    df_hist.loc[df_hist[OCl.TYPE.nm] == OptionType.CALL.code, OCl.INTRINSIC_VALUE.nm] = \
        df_hist[OCl.FUTURES_PRICE.nm] - df_hist[OCl.STRIKE.nm]
    df_hist.loc[df_hist[OCl.TYPE.nm] == OptionType.PUT.code, OCl.INTRINSIC_VALUE.nm] = \
        df_hist[OCl.STRIKE.nm] - df_hist[OCl.FUTURES_PRICE.nm]
    df_hist.loc[df_hist[OCl.INTRINSIC_VALUE.nm] < 0, OCl.INTRINSIC_VALUE.nm] = 0
    df_hist.loc[:, OCl.TIME_VALUE.nm] = df_hist[OCl.PREMIUM.nm] - df_hist[OCl.INTRINSIC_VALUE.nm]
    return df_hist


def add_atm_itm_otm_by_chain(df_hist):
    """
    Should be optimized - very slow
    """

    money_col_df = df_hist.groupby([OCl.DATETIME.nm, OCl.EXPIRATION_DATE.nm],
                                   group_keys=False) \
        .apply(get_chain_atm_itm_otm, include_groups=False)
    df_hist = pd.concat([df_hist, money_col_df], axis='columns')
    return df_hist


def add_atm_itm_otm_exp(df_hist):
    """
    Slower than add_atm_itm_otm_by

    Alternative:
     - 1. Idea to improve change call to 1 and put to -1 and multiple on _diff
     - 2. calc based on intrinsic values. If less 0 - OTM, If greater ITM. Question in atm
       detection - minimal abs intrinsic?.
    """

    df_hist.loc[:, '_diff'] = df_hist[OCl.FUTURES_PRICE.nm] - df_hist[OCl.STRIKE.nm]
    df_hist.loc[:, '_diff_abs'] = df_hist['_diff'].abs()

    def atm_otm_itm(x):
        atm_strikes = x[x['_diff_abs'] == x['_diff_abs'].min()]
        atm_strike_diff = atm_strikes.iloc[0]['_diff']

        if x.iloc[0][OCl.TYPE.nm] == OptionType.CALL.code:
            itm = x[x['_diff'] > atm_strike_diff]
            otm = x[x['_diff'] < atm_strike_diff]
            return pd.Series([OptionPriceStatus.ITM.code] * len(itm) + [OptionPriceStatus.ATM.code] + \
                             [OptionPriceStatus.OTM.code] * len(otm))
        itm = x[x['_diff'] < atm_strike_diff]
        otm = x[x['_diff'] > atm_strike_diff]
        return pd.Series([OptionPriceStatus.OTM.code] * len(otm) + [OptionPriceStatus.ATM.code] + \
                         [OptionPriceStatus.ITM.code] * len(itm))

    df_hist.loc[:, OCl.PRICE_STATUS.nm] = \
        df_hist.sort_values(by=[OCl.DATETIME.nm, OCl.EXPIRATION_DATE.nm, OCl.TYPE.nm, OCl.STRIKE.nm]) \
            .groupby([OCl.DATETIME.nm, OCl.EXPIRATION_DATE.nm, OCl.TYPE.nm], group_keys=False)[
            ['_diff', '_diff_abs', OCl.TYPE.nm]] \
            .apply(atm_otm_itm, include_groups=False).drop(columns=['_diff']).reset_index(drop=True)
    return df_hist
