import pandas as pd
from options_lib.dictionary import (
    OptionsColumns as OCl, OptionsType
)
from options_lib.normalization.price import fill_option_price


def test_fill_option_price():
    df = pd.DataFrame({OCl.OPTION_TYPE.nm: [OptionsType.CALL] * 7,
                       OCl.LAST.nm: [80, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA],
                       OCl.ASK.nm: [pd.NA, 105, 105, pd.NA, pd.NA, pd.NA, pd.NA],
                       OCl.BID.nm: [pd.NA, 95, pd.NA, 95, pd.NA, pd.NA, pd.NA],
                       OCl.HIGH.nm: [pd.NA, pd.NA, pd.NA, pd.NA, 110, 110, pd.NA],
                       OCl.LOW.nm: [pd.NA, pd.NA, pd.NA, pd.NA, 90, pd.NA, 90],
                       })
    df = fill_option_price(df)
    assert df[OCl.PRICE.nm].notnull().all()
