"""Option enrichment data class tests"""
import pandas as pd
import pytest

from option_lib.entities import OptionColumns as Ocl
from option_lib.enrichment import OptionEnrichment


@pytest.fixture(name='opt_enrich')
def fixture_opt_enrich(option_data):
    """Fixture for OptionEnrichment instance"""
    opt_enr = OptionEnrichment(option_data)
    return opt_enr


def test_option_enrichment_class_init(option_data):
    opt_enr = OptionEnrichment(option_data)
    assert isinstance(opt_enr, OptionEnrichment)


def test_option_enrichment_get_joint_option_with_future(opt_enrich):
    if Ocl.UNDERLYING_PRICE.nm in opt_enrich.df_hist.columns:
        opt_enrich.df_hist.drop(columns=[Ocl.UNDERLYING_PRICE.nm], inplace=True)
    assert Ocl.UNDERLYING_PRICE.nm not in opt_enrich.df_hist.columns
    df_opt = opt_enrich.get_joint_option_with_future()
    assert isinstance(df_opt, pd.DataFrame)
    assert Ocl.UNDERLYING_PRICE.nm in df_opt.columns


def test_option_enrichment_add_future(opt_enrich):
    if Ocl.UNDERLYING_PRICE.nm in opt_enrich.df_hist.columns:
        opt_enrich.df_hist.drop(columns=[Ocl.UNDERLYING_PRICE.nm], inplace=True)
    assert Ocl.UNDERLYING_PRICE.nm not in opt_enrich.df_hist.columns
    res = opt_enrich.add_future()
    assert isinstance(res, OptionEnrichment)
    assert isinstance(opt_enrich.df_hist, pd.DataFrame)
    assert Ocl.UNDERLYING_PRICE.nm in opt_enrich.df_hist.columns


def  test_option_enrichment_add_intrinsic_and_time_value(opt_enrich):
    assert Ocl.INTRINSIC_VALUE.nm not in opt_enrich.df_hist.columns
    res = opt_enrich.add_intrinsic_and_time_value()
    assert isinstance(res, OptionEnrichment)
    assert isinstance(opt_enrich.df_hist, pd.DataFrame)
    assert Ocl.INTRINSIC_VALUE.nm in opt_enrich.df_hist.columns
    assert Ocl.TIME_VALUE.nm in opt_enrich.df_hist.columns


def test_add_atm_itm_otm(opt_enrich):
    assert Ocl.PRICE_STATUS.nm not in opt_enrich.df_hist.columns
    opt_enrich.df_hist = opt_enrich.df_hist.iloc[-10_000:]
    res = opt_enrich.add_atm_itm_otm()
    assert isinstance(res, OptionEnrichment)
    assert isinstance(opt_enrich.df_hist, pd.DataFrame)
    assert Ocl.PRICE_STATUS.nm in opt_enrich.df_hist.columns
