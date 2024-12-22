import enum

class EnumCode(str, enum.Enum):
    def __new__(cls, value, code):
        obj = str.__new__(cls, [value])
        obj._value_ = value
        obj.code = code
        return obj

    def __str__(self):
        return self.value

    def __repr__(self):
        return self.value

@enum.unique
class OptionType(EnumCode):
    """
    Usage code "c" instead of value "call" for filter dataframe reduce time for ~5%-30% and memory usage
    """
    CALL = "call", "c"
    PUT = "put", "p"

@enum.unique
class OptionMoneyStatus(EnumCode):
# class OptionMoneyStatus(enum.Enum):
    ATM = 'ATM', 'atm'
    ITM = 'ITM', 'itm'
    OTM = 'OTM', 'otm'


def prepare_desk(df_opt, columns: list | None = None, underlying_columns: list | None = None):
    """Prepare option desk from options or option_chain"""
    if columns is None:
        columns = ['premium', 'iv' , 'delta', 'gamma', 'vega', 'theta', 'quick_delta', 'datetime', 'expiration_date', 'strike']
    append_col = [col for col in ['datetime', 'expiration_date', 'strike'] if col not in columns]
    if len(append_col):
        columns += append_col
    if underlying_columns is None:
        underlying_columns = ['underlying_price', 'underlying_expiration_date']
    df_opt_desk = df_opt[df_opt['type']=='c'][columns].merge(df_opt[df_opt['type']=='p'][columns], on=['datetime', 'expiration_date', 'strike'], suffixes=['_call', '_put'], how='outer').sort_values(by='strike')
    if underlying_columns:
        df_opt_desk = df_opt_desk.merge(df_opt[['datetime', 'expiration_date'] + underlying_columns].drop_duplicates(), on=['datetime', 'expiration_date'], how='left')
    return df_opt_desk


def get_chain_atm(df, datetime = None, expiration_date = None):
    """Get strike atm"""
    atm_nearest_strikes = get_chain_atm_nearest_strikes(df, datetime, expiration_date)
    atm_strike = atm_nearest_strikes[0]
    return atm_strike
    

def get_chain_atm_nearest_strikes(df_opt, settlement_date = None, expiration_date = None):
    """Get strikes sorted to nearest to atm (for showing desk for example)"""
    df_opt_chain = df_opt
    if settlement_date:
        df_opt_chain = df_opt_chain[(df_res['datetime']==settlement_date)]
    elif len(df_opt_chain['datetime'].unique()) != 1:
        raise ValueError('Should be set datetime or provided dataframe for one datetime')
    if expiration_date:
        df_opt_chain = df_res[(df_res['expiration_date']==expiration_date)]
    elif len(df_opt_chain['expiration_date'].unique()) != 1:
        raise ValueError('Should be set expiration_date or provided dataframe for one expiration_date')
    
    atm_nearest_strikes = df_opt_chain.assign(_diff=lambda x: abs(x['underlying_price'] - x['strike'])).sort_values(by='_diff')['strike'].unique()
    return atm_nearest_strikes
    
def get_chain_atm_itm_otm(df_opt_chain):
    """
    ITM In the Money
    OTM Out of the Money
    ATM At the Money
    """
    atm_strike = get_chain_atm(df_opt_chain)
    df_opt_chain.loc[ : , 'money_status'] = OptionMoneyStatus.OTM.code
    df_opt_chain.loc[df_opt_chain['strike']==atm_strike, 'money_status'] = OptionMoneyStatus.ATM.code
    df_opt_chain.loc[(df_opt_chain['strike']<atm_strike)&(df_opt_chain['type']==OptionType.CALL.code), 'money_status'] =  OptionMoneyStatus.ITM.code
    df_opt_chain.loc[(df_opt_chain['strike']>atm_strike)&(df_opt_chain['type']==OptionType.PUT.code), 'money_status'] = OptionMoneyStatus.ITM.code
    return df_opt_chain['money_status']


def get_atm_itm_otm(df_opt):
    """
    TODO should be optimized - very slow
    """
    df_opt_money_status = df_opt.groupby(['datetime', 'expiration_date'], group_keys=False).apply(get_chain_atm_itm_otm) # include_groups=False
    return df_opt_money_status

def add_intrinsic_and_time_value(df_opt):
    """
    Adding columns with intrinsic value and time value
    """

    df_opt.loc[ : , 'intrinsic_value'] = 0.
    df_opt.loc[df_opt['type'] == OptionType.CALL.code, 'intrinsic_value'] = df_opt['underlying_price'] - df_opt['strike']
    df_opt.loc[df_opt['type'] == OptionType.PUT.code, 'intrinsic_value'] = df_opt['strike'] - df_opt['underlying_price']
    df_opt.loc[df_opt['intrinsic_value'] < 0, 'intrinsic_value'] = 0
    df_opt.loc[ : , 'time_value'] = df_opt['premium'] - df_opt['intrinsic_value']
    return df_opt



    