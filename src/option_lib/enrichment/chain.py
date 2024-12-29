"""
Prepare data for option chain - options with the same date and expiration date
"""

from option_lib.entities import OptionType, OptionMoneyStatus


def convert_chain_to_desk(df_chain_opt, columns: list | None = None, underlying_columns: list | None = None):
    """Prepare option desk from options or option_chain"""
    if columns is None:
        columns = ['premium', 'iv', 'delta', 'gamma', 'vega', 'theta', 'quick_delta', 'datetime', 'expiration_date',
                   'strike']
    append_col = [col for col in ['datetime', 'expiration_date', 'strike'] if col not in columns]
    if len(append_col):
        columns += append_col
    columns = [col for col in df_chain_opt.columns if col in columns]
    if underlying_columns is None:
        underlying_columns = ['future_price', 'future_expiration_date']
    df_opt_desk = df_chain_opt[df_chain_opt['type'] == OptionType.CALL.code][columns].merge(
        df_chain_opt[df_chain_opt['type'] == OptionType.PUT.code][columns],
        on=['datetime', 'expiration_date', 'strike'], suffixes=['_call', '_put'], how='outer').sort_values(by='strike')
    if underlying_columns:
        df_opt_desk = df_opt_desk.merge(
            df_chain_opt[['datetime', 'expiration_date'] + underlying_columns].drop_duplicates(),
            on=['datetime', 'expiration_date'], how='left')
    return df_opt_desk


def get_chain_atm(df, datetime=None, expiration_date=None):
    """Get strike atm"""
    atm_nearest_strikes = get_chain_atm_nearest_strikes(df, datetime, expiration_date)
    atm_strike = atm_nearest_strikes[0]
    return atm_strike


def get_chain_atm_nearest_strikes(df_opt, settlement_date=None, expiration_date=None):
    """Get strikes sorted to nearest to atm (for showing desk for example)"""
    df_opt_chain = df_opt
    if settlement_date:
        df_opt_chain = df_opt_chain[(df_opt_chain['datetime'] == settlement_date)]
    elif len(df_opt_chain['datetime'].unique()) != 1:
        raise ValueError('Should be set datetime or provided dataframe for one datetime')
    if expiration_date:
        df_opt_chain = df_opt_chain[(df_opt_chain['expiration_date'] == expiration_date)]
    elif len(df_opt_chain['expiration_date'].unique()) != 1:
        raise ValueError('Should be set expiration_date or provided dataframe for one expiration_date')

    atm_nearest_strikes = \
        df_opt_chain.assign(_diff=lambda x: abs(x['underlying_price'] - x['strike'])).sort_values(by='_diff')[
            'strike'].unique()
    return atm_nearest_strikes


def get_chain_atm_itm_otm(df_opt_chain):
    """
    ITM In the Money
    OTM Out of the Money
    ATM At the Money
    """
    atm_strike = get_chain_atm(df_opt_chain)
    df_opt_chain.loc[:, 'money_status'] = OptionMoneyStatus.OTM.code
    df_opt_chain.loc[df_opt_chain['strike'] == atm_strike, 'money_status'] = OptionMoneyStatus.ATM.code
    df_opt_chain.loc[(df_opt_chain['strike'] < atm_strike) & (
        df_opt_chain['type'] == OptionType.CALL.code), 'money_status'] = OptionMoneyStatus.ITM.code
    df_opt_chain.loc[(df_opt_chain['strike'] > atm_strike) & (
        df_opt_chain['type'] == OptionType.PUT.code), 'money_status'] = OptionMoneyStatus.ITM.code
    return df_opt_chain['money_status']
