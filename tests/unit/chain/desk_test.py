"""Desk for chain module tests"""
import pandas as pd

from option_lib.chain.desk import convert_chain_to_desk
from option_lib.entities import OptionColumns as OCl

def test_convert_chain_to_desk(df_brn_chain):
    df_desk = convert_chain_to_desk(df_brn_chain)
    assert isinstance(df_desk, pd.DataFrame)
    assert OCl.PRICE.nm + '_call' in df_desk.columns
    assert OCl.PRICE.nm + '_put' in df_desk.columns
