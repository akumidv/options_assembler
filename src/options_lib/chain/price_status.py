"""
Prepare data for option chain - options with the same date and expiration date
"""

from options_lib.entities import OptionsType, OptionsPriceStatus, OptionsColumns as OCl


def get_chain_atm_strike(df_chain):
    """Get strike atm"""
    atm_nearest_strikes = get_chain_atm_nearest_strikes(df_chain)
    atm_strike = atm_nearest_strikes[0]
    return atm_strike


def get_chain_atm_nearest_strikes(df_chain):
    """Get strikes sorted as nearest to atm (for showing desk for example)"""
    atm_nearest_strikes = df_chain.assign(_diff=lambda x: abs(x[OCl.UNDERLYING_PRICE.nm] - x[OCl.STRIKE.nm]))\
                                  .sort_values(by='_diff')[OCl.STRIKE.nm].unique()
    return atm_nearest_strikes


def get_chain_atm_itm_otm(df_chain):
    """
    ITM In the Money
    OTM Out of the Money
    ATM At the Money
    """
    atm_strike = get_chain_atm_strike(df_chain)
    df_chain.loc[:, OCl.PRICE_STATUS.nm] = OptionsPriceStatus.OTM.code
    df_chain.loc[df_chain[OCl.STRIKE.nm] == atm_strike, OCl.PRICE_STATUS.nm] = OptionsPriceStatus.ATM.code
    df_chain.loc[(df_chain[OCl.STRIKE.nm] < atm_strike) & (
            df_chain[OCl.OPTION_TYPE.nm] == OptionsType.CALL.code), OCl.PRICE_STATUS.nm] = OptionsPriceStatus.ITM.code
    df_chain.loc[(df_chain[OCl.STRIKE.nm] > atm_strike) & (
            df_chain[OCl.OPTION_TYPE.nm] == OptionsType.PUT.code), OCl.PRICE_STATUS.nm] = OptionsPriceStatus.ITM.code
    return df_chain[OCl.PRICE_STATUS.nm]
