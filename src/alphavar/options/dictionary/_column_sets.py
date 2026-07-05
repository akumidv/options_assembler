"""Per-dataset column membership for the options/futures domain (R0, R4.3).

Replaces the old ``FuturesColumns``/``SpotColumns`` enums (T23.1): the dataset
*composition* (which columns form options / futures / spot frames) lives here as plain
``tuple[str, ...]`` over the ``OptionsTerm`` registry. Column dtypes live in the pandera
schema layer; resample aggregations live next to the resampler — neither is duplicated
here.

The futures/spot membership reproduces the prior ``FuturesColumns``/``SpotColumns``
enums 1:1 by value. The planned-but-unused ``mark_price`` / ``mark_iv`` columns are
intentionally dropped (superseded by R4.2 / T23.6).
"""

from alphavar.options.dictionary._terms import OptionsTerm

OPTIONS_COLUMN_NAMES: tuple[str, ...] = (
    OptionsTerm.TIMESTAMP,
    OptionsTerm.STRIKE,
    OptionsTerm.EXPIRATION_DATE,
    OptionsTerm.OPTION_RIGHT,
    OptionsTerm.PRICE,
    OptionsTerm.ASK,
    OptionsTerm.BID,
    OptionsTerm.OPEN_INTEREST,
    OptionsTerm.VOLUME,
    OptionsTerm.VOLUME_PREMIUM,
    OptionsTerm.VOLUME_NOTIONAL,
    OptionsTerm.UNDERLYING_EXPIRATION_DATE,
    OptionsTerm.EXCH_MARK_PRICE,
    OptionsTerm.EXCH_MARK_IV,
    OptionsTerm.OPEN,
    OptionsTerm.CLOSE,
    OptionsTerm.HIGH,
    OptionsTerm.LOW,
    OptionsTerm.REQUEST_TIMESTAMP,
    OptionsTerm.EXCH_TIMESTAMP,
    OptionsTerm.LAST,
    OptionsTerm.LOW_24,
    OptionsTerm.HIGH_24,
    OptionsTerm.UNDERLYING_PRICE,
    OptionsTerm.INTRINSIC_VALUE,
    OptionsTerm.TIMED_VALUE,
    OptionsTerm.PRICE_STATUS,
    OptionsTerm.IV,
    OptionsTerm.DELTA,
    OptionsTerm.GAMMA,
    OptionsTerm.VEGA,
    OptionsTerm.THETA,
    OptionsTerm.RHO,
    OptionsTerm.SERIES_CODE,
    OptionsTerm.ASSET_CODE,
    OptionsTerm.EXCH_SYMBOL,
    OptionsTerm.INSTRUMENT_KIND,
    OptionsTerm.UNDERLYING_CODE,
    OptionsTerm.UNDERLYING_ASSET_CLASS,
    OptionsTerm.BASE_CODE,
    OptionsTerm.TITLE,
    OptionsTerm.OPTION_STYLE,
    OptionsTerm.CURRENCY,
)

FUTURES_COLUMN_NAMES: tuple[str, ...] = (
    OptionsTerm.TIMESTAMP,
    OptionsTerm.EXPIRATION_DATE,
    OptionsTerm.PRICE,
    OptionsTerm.ASK,
    OptionsTerm.BID,
    OptionsTerm.OPEN_INTEREST,
    OptionsTerm.VOLUME,
    OptionsTerm.VOLUME_NOTIONAL,
    OptionsTerm.OPEN,
    OptionsTerm.CLOSE,
    OptionsTerm.HIGH,
    OptionsTerm.LOW,
    OptionsTerm.REQUEST_TIMESTAMP,
    OptionsTerm.EXCH_TIMESTAMP,
    OptionsTerm.LAST,
    OptionsTerm.LOW_24,
    OptionsTerm.HIGH_24,
    OptionsTerm.SERIES_CODE,
    OptionsTerm.BASE_CODE,
    OptionsTerm.INSTRUMENT_KIND,
    OptionsTerm.ASSET_CODE,
    OptionsTerm.UNDERLYING_ASSET_CLASS,
    OptionsTerm.UNDERLYING_CODE,
    OptionsTerm.TITLE,
)

SPOT_COLUMN_NAMES: tuple[str, ...] = (
    OptionsTerm.TIMESTAMP,
    OptionsTerm.PRICE,
    OptionsTerm.ASK,
    OptionsTerm.BID,
    OptionsTerm.OPEN_INTEREST,
    OptionsTerm.VOLUME,
    OptionsTerm.VOLUME_NOTIONAL,
    OptionsTerm.ASSET_CODE,
    OptionsTerm.INSTRUMENT_KIND,
    OptionsTerm.TITLE,
    OptionsTerm.OPEN,
    OptionsTerm.CLOSE,
    OptionsTerm.HIGH,
    OptionsTerm.LOW,
    OptionsTerm.REQUEST_TIMESTAMP,
    OptionsTerm.EXCH_TIMESTAMP,
)

# Options-only columns relative to the other datasets (used for ETL column routing).
OPTION_NON_FUTURES_COLUMN_NAMES: list[str] = [col for col in OPTIONS_COLUMN_NAMES if col not in FUTURES_COLUMN_NAMES]
OPTION_NON_SPOT_COLUMN_NAMES: list[str] = [col for col in OPTIONS_COLUMN_NAMES if col not in SPOT_COLUMN_NAMES]

ALL_COLUMN_NAMES: list[str] = list(dict.fromkeys(OPTIONS_COLUMN_NAMES + FUTURES_COLUMN_NAMES + SPOT_COLUMN_NAMES))
