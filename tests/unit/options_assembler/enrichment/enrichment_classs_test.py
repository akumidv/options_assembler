"""Option enrichment data class tests"""
import pandas as pd
import pytest

from options_lib.entities import OptionsColumns as OCl
from options_assembler.enrichment import OptionEnrichment


@pytest.fixture(name='opt_enrich')
def fixture_opt_enrich(option_data):
    """Fixture for OptionEnrichment instance"""
    opt_enr = OptionEnrichment(option_data)
    return opt_enr


def test_option_enrichment_class_init(option_data):
    opt_enr = OptionEnrichment(option_data)
    assert isinstance(opt_enr, OptionEnrichment)


def test__prepare_order_of_columns_enrichment(opt_enrich):
    columns_to_enrich = [OCl.TIMED_VALUE, OCl.INTRINSIC_VALUE, OCl.PRICE_STATUS]
    columns = opt_enrich._prepare_order_of_columns_enrichment(columns_to_enrich)
    assert columns == [OCl.UNDERLYING_PRICE, OCl.INTRINSIC_VALUE, OCl.TIMED_VALUE, OCl.PRICE_STATUS]


def test_option_enrichment_get_joint_option_with_future(opt_enrich):
    if OCl.UNDERLYING_PRICE.nm in opt_enrich.data.df_hist.columns:
        opt_enrich.data.df_hist.drop(columns=OCl.UNDERLYING_PRICE.nm, inplace=True)
    assert OCl.UNDERLYING_PRICE.nm not in opt_enrich.data.df_hist.columns
    df_opt = opt_enrich.enrich_options(OCl.UNDERLYING_PRICE)
    assert isinstance(df_opt, pd.DataFrame)
    assert OCl.UNDERLYING_PRICE.nm in df_opt.columns


def test_option_enrichment_add_future(opt_enrich):
    if OCl.UNDERLYING_PRICE.nm in opt_enrich.data.df_hist.columns:
        opt_enrich.data.df_hist.drop(columns=[OCl.UNDERLYING_PRICE.nm], inplace=True)
    assert OCl.UNDERLYING_PRICE.nm not in opt_enrich.data.df_hist.columns
    res = opt_enrich.add_column(OCl.UNDERLYING_PRICE)
    assert isinstance(res, OptionEnrichment)
    assert isinstance(opt_enrich.data.df_hist, pd.DataFrame)
    assert OCl.UNDERLYING_PRICE.nm in opt_enrich.data.df_hist.columns


def test_option_enrichment_add_intrinsic_and_time_value(opt_enrich):
    assert OCl.INTRINSIC_VALUE.nm not in opt_enrich.data.df_hist.columns
    res = opt_enrich.add_column(OCl.INTRINSIC_VALUE)
    assert isinstance(res, OptionEnrichment)
    assert isinstance(opt_enrich.data.df_hist, pd.DataFrame)
    assert OCl.INTRINSIC_VALUE.nm in opt_enrich.data.df_hist.columns
    assert OCl.TIMED_VALUE.nm in opt_enrich.data.df_hist.columns


def test_add_atm_itm_otm(opt_enrich):
    assert OCl.PRICE_STATUS.nm not in opt_enrich.data.df_hist.columns
    opt_enrich.data.df_hist = opt_enrich.data.df_hist.iloc[-10_000:]
    res = opt_enrich.add_column(OCl.PRICE_STATUS)
    assert isinstance(res, OptionEnrichment)
    assert isinstance(opt_enrich.data.df_hist, pd.DataFrame)
    assert OCl.PRICE_STATUS.nm in opt_enrich.data.df_hist.columns
