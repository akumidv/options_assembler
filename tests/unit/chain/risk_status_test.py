from option_lib.entities import OptionColumns as OCl
from option_lib.chain.price_status import get_chain_atm_strike


def test_get_chain_atm_strike(df_brn_chain):
    atm_strike = get_chain_atm_strike(df_brn_chain)
    assert isinstance(atm_strike, float)
    assert atm_strike in df_brn_chain[OCl.STRIKE.nm].unique()
    df_brn_chain['_diff'] = (df_brn_chain[OCl.FUTURES_PRICE.nm] - df_brn_chain[OCl.STRIKE.nm]).abs()
    assert atm_strike == df_brn_chain[df_brn_chain['_diff'] == df_brn_chain['_diff'].min()].iloc[0][OCl.STRIKE.nm]
