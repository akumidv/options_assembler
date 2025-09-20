from options_lib.entities import OptionsColumns as OCl
from options_lib.chain.price_status import get_chain_atm_strike


def test_get_chain_atm_strike(df_chain):
    atm_strike = get_chain_atm_strike(df_chain)
    assert isinstance(atm_strike, float)
    assert atm_strike in df_chain[OCl.STRIKE.nm].unique()
    df_chain['_diff'] = (df_chain[OCl.UNDERLYING_PRICE.nm] - df_chain[OCl.STRIKE.nm]).abs()
    assert atm_strike == df_chain[df_chain['_diff'] == df_chain['_diff'].min()].iloc[0][OCl.STRIKE.nm]
