"""Option chain data class tests"""
import datetime
import pandas as pd
import pytest

from options_lib.entities import OptionsColumns as OCl
from options_assembler.chain import OptionChain


@pytest.fixture(name='opt_chain')
def fixture_opt_chain(option_data):
    """Fixture for OptionChain instance"""
    opt_enr = OptionChain(option_data)
    return opt_enr


def test_option_chain_class_init(option_data):
    opt_enr = OptionChain(option_data)
    assert isinstance(opt_enr, OptionChain)


def test_select_chain(opt_chain):
    assert opt_chain._data.df_chain is None
    df_opt_chain = opt_chain.select_chain()
    assert isinstance(df_opt_chain, pd.DataFrame)
    opt_chain.validate_chain(df_opt_chain)


def test_getter_option_chain(opt_chain):
    assert opt_chain._data.df_chain is None
    df_opt_chain = opt_chain.df_chain
    assert isinstance(df_opt_chain, pd.DataFrame)
    opt_chain.validate_chain(df_opt_chain)


def test_get_settlement_and_expiration_date(opt_chain):
    opt_chain.select_chain()
    settlement_date, expiration_date = opt_chain.get_settlement_and_expiration_date()
    assert isinstance(settlement_date, datetime.date)
    assert isinstance(expiration_date, datetime.date)


def test_get_desk(opt_chain):
    opt_chain.select_chain()
    df_desk = opt_chain.get_desk()
    assert isinstance(df_desk, pd.DataFrame)
    assert OCl.PRICE.nm + '_call' in df_desk.columns
    assert OCl.PRICE.nm + '_put' in df_desk.columns
