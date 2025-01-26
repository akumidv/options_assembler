"""Money status data enrichment tests"""
import datetime

import pandas as pd
import pytest

from option_lib.entities import OptionColumns as OCl, OptionPriceStatus
from option_lib.enrichment import price


def test_add_intrinsic_and_time_value(df_ext_brn_hist):
    df_opt_ext = price.add_intrinsic_and_time_value(df_ext_brn_hist)
    assert isinstance(df_opt_ext, pd.DataFrame)
    assert OCl.INTRINSIC_VALUE.nm in df_opt_ext.columns
    assert OCl.TIME_VALUE.nm in df_opt_ext.columns


def test_add_atm_itm_otm(df_ext_brn_hist):
    dt_filter = df_ext_brn_hist[OCl.TIMESTAMP.nm].max() - datetime.timedelta(days=10)
    df_opt_ext = price.add_atm_itm_otm_by_chain(df_ext_brn_hist[df_ext_brn_hist[OCl.TIMESTAMP.nm] > dt_filter])
    assert isinstance(df_opt_ext, pd.DataFrame)
    assert OCl.PRICE_STATUS.nm in df_opt_ext.columns
    assert not df_opt_ext[df_opt_ext[OCl.PRICE_STATUS.nm] == OptionPriceStatus.ATM.code].empty


@pytest.mark.skip('Developing')
def test_add_atm_itm_otm_exp(df_ext_brn_hist):
    dt_filter = df_ext_brn_hist[OCl.TIMESTAMP.nm].max() - datetime.timedelta(days=10)
    df_opt_ext = price.add_atm_itm_otm_exp(df_ext_brn_hist[df_ext_brn_hist[OCl.TIMESTAMP.nm] > dt_filter])
    assert isinstance(df_opt_ext, pd.DataFrame)
    assert OCl.PRICE_STATUS.nm in df_opt_ext.columns
    assert not df_opt_ext[df_opt_ext[OCl.PRICE_STATUS.nm] == OptionPriceStatus.ATM.code].empty
