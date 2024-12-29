"""
Internal realization for option money data enrichment
"""

from option_lib.entities import OptionType


def add_intrinsic_and_time_value(df_opt):
    """
    Adding columns with intrinsic value and time value
    """

    df_opt.loc[:, 'intrinsic_value'] = 0.
    df_opt.loc[df_opt['type'] == OptionType.CALL.code, 'intrinsic_value'] = df_opt['future_price'] - df_opt[
        'strike']
    df_opt.loc[df_opt['type'] == OptionType.PUT.code, 'intrinsic_value'] = df_opt['strike'] - df_opt['future_price']
    df_opt.loc[df_opt['intrinsic_value'] < 0, 'intrinsic_value'] = 0
    df_opt.loc[:, 'time_value'] = df_opt['premium'] - df_opt['intrinsic_value']
    return df_opt


# def add_atm_itm_otm(df_opt):
#     """
#     Should be optimized - very slow
#     """
#     df_opt_money_status = df_opt.groupby(['datetime', 'expiration_date'], group_keys=False).apply(chain.get_chain_atm_itm_otm) # include_groups=False
#     return df_opt_money_status
