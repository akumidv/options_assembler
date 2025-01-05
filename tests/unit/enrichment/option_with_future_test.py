"""Tests for joining option with futures"""
import pandas as pd

from option_lib.enrichment import _option_with_future as option_with_future
from option_lib.entities import OptionColumns as OCl


def test_join_option_future(df_brn_hist, df_brn_fut):
    assert OCl.FUTURES_PRICE.nm not in df_brn_hist.columns
    df_ext_opt = option_with_future.join_option_with_future(df_brn_hist, df_brn_fut)
    assert isinstance(df_ext_opt, pd.DataFrame)
    assert len(df_ext_opt) == len(df_brn_hist)
    assert OCl.FUTURES_PRICE.nm in df_ext_opt.columns
