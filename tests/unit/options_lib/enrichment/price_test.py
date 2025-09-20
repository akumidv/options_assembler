"""Money status data enrichment tests"""
import datetime

import pandas as pd
import pytest

from options_lib.entities import OptionsColumns as OCl, OptionsPriceStatus
from options_lib.enrichment import price


def test_add_intrinsic_and_time_value(df_opt_hist):
    df_opt_ext = price.add_intrinsic_and_time_value(df_opt_hist)
    assert isinstance(df_opt_ext, pd.DataFrame)
    assert OCl.INTRINSIC_VALUE.nm in df_opt_ext.columns
    assert OCl.TIMED_VALUE.nm in df_opt_ext.columns


def test_add_atm_itm_otm(df_opt_hist):
    dt_filter = df_opt_hist[OCl.TIMESTAMP.nm].max() - datetime.timedelta(days=10)
    df_opt_ext = price.add_atm_itm_otm_by_chain(df_opt_hist[df_opt_hist[OCl.TIMESTAMP.nm] > dt_filter])
    assert isinstance(df_opt_ext, pd.DataFrame)
    assert OCl.PRICE_STATUS.nm in df_opt_ext.columns
    assert not df_opt_ext[df_opt_ext[OCl.PRICE_STATUS.nm] == OptionsPriceStatus.ATM.code].empty


@pytest.mark.skip('Developing')
def test_add_atm_itm_otm_exp(df_opt_hist):
    dt_filter = df_opt_hist[OCl.TIMESTAMP.nm].max() - datetime.timedelta(days=10)
    df_opt_ext = price.add_atm_itm_otm_exp(df_opt_hist[df_opt_hist[OCl.TIMESTAMP.nm] > dt_filter])
    assert isinstance(df_opt_ext, pd.DataFrame)
    assert OCl.PRICE_STATUS.nm in df_opt_ext.columns
    assert not df_opt_ext[df_opt_ext[OCl.PRICE_STATUS.nm] == OptionsPriceStatus.ATM.code].empty
