"""Testing dataframe columns integrity"""

from options_lib.dictionary import (
    OptionsColumns as OCl,
    # FuturesColumns as FCl,
    # SpotColumns as SCl,
    OPTION_NON_FUTURES_COLUMN_NAMES,
    OPTION_NON_SPOT_COLUMN_NAMES
)


def test_option_non_fut_spot_columns():
    assert isinstance(OPTION_NON_FUTURES_COLUMN_NAMES, list)
    assert OCl.TIMESTAMP.nm not in OPTION_NON_FUTURES_COLUMN_NAMES
    assert OCl.STRIKE.nm in OPTION_NON_FUTURES_COLUMN_NAMES
    assert isinstance(OPTION_NON_SPOT_COLUMN_NAMES, list)
    assert OCl.TIMESTAMP.nm not in OPTION_NON_SPOT_COLUMN_NAMES
    assert OCl.STRIKE.nm in OPTION_NON_SPOT_COLUMN_NAMES
