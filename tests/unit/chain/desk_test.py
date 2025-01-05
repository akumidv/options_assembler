"""Desk for chain module tests"""
import pandas as pd

from option_lib.chain.desk import convert_chain_to_desk


def test_convert_chain_to_desk(df_brn_chain):
    df_desk = convert_chain_to_desk(df_brn_chain)
    assert isinstance(df_desk, pd.DataFrame)
    assert 'premium_call' in df_desk.columns
    assert 'premium_put' in df_desk.columns
