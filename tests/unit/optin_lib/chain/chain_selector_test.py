import datetime

import pandas as pd

from option_lib.entities import OptionColumns as OCl
from option_lib.chain.chain_selector import select_chain, validate_chain, get_chain_settlement_and_expiration_date


def test_select_chain(df_brn_hist):
    df_brn_chain = select_chain(df_brn_hist)
    assert isinstance(df_brn_chain, pd.DataFrame)
    assert len(df_brn_chain[OCl.TIMESTAMP.nm].unique()) == 1
    assert len(df_brn_chain[OCl.EXPIRATION_DATE.nm].unique()) == 1
    validate_chain(df_brn_chain)


def test_get_chain_datetime_and_expiration_date(df_brn_chain):
    settlement_date, expiration_date = get_chain_settlement_and_expiration_date(df_brn_chain)
    assert isinstance(settlement_date, datetime.date)
    assert isinstance(expiration_date, datetime.date)
