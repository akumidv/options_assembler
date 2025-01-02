"""
Internal realization for option money data enrichment
"""

from option_lib.entities import OptionColumns as OCl, OptionType
from option_lib.chain.money_status import (
    get_chain_atm_itm_otm
)


def add_intrinsic_and_time_value(df_opt):
    """
    Adding columns with intrinsic value and time value
    """

    df_opt.loc[:, OCl.INTRINSIC_VALUE.col] = 0.
    df_opt.loc[df_opt[OCl.TYPE.col] == OptionType.CALL.code, OCl.INTRINSIC_VALUE.col] = \
        df_opt[OCl.FUTURE_PRICE.col] - df_opt[OCl.STRIKE.col]
    df_opt.loc[df_opt[OCl.TYPE.col] == OptionType.PUT.code, OCl.INTRINSIC_VALUE.col] = \
        df_opt[OCl.STRIKE.col] - df_opt[OCl.FUTURE_PRICE.col]
    df_opt.loc[df_opt[OCl.INTRINSIC_VALUE.col] < 0, OCl.INTRINSIC_VALUE.col] = 0
    df_opt.loc[:, OCl.TIME_VALUE.col] = df_opt[OCl.PREMIUM.col] - df_opt[OCl.INTRINSIC_VALUE.col]
    return df_opt


def add_atm_itm_otm(df_opt):
    """
    Should be optimized - very slow
    """

    df_opt[OCl.MONEY_STATUS.col] = df_opt.groupby([OCl.DATETIME.col, OCl.EXPIRATION_DATE.col], group_keys=False) \
        .apply(get_chain_atm_itm_otm)  # include_groups=False
    return df_opt
