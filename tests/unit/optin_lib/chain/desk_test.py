"""Desk for chain module tests"""
import pandas as pd

from options_assembler.chain.desk import convert_chain_to_desk
from options_assembler.entities import OptionColumns as OCl

def test_convert_chain_to_desk(df_chain):
    df_desk = convert_chain_to_desk(df_chain)
    assert isinstance(df_desk, pd.DataFrame)
    assert OCl.PRICE.nm + '_call' in df_desk.columns
    assert OCl.PRICE.nm + '_put' in df_desk.columns
