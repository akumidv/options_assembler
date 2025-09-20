"""Pandas columns code, value and types"""

import enum
import pandas as pd
from options_lib.entities.enum_code import EnumDataFrameColumn
from options_lib.entities._option_types import OptionsPriceStatus


@enum.unique
class OptionsColumns(EnumDataFrameColumn):
    """
    Pandas option column name, type
    """

    # NAME, value, code, type, resample function name
    # Base mandatory columns from provider
    TIMESTAMP = "timestamp", pd.Timestamp, "last"
    STRIKE = "strike", float, "last"
    EXPIRATION_DATE = "expiration_date", pd.Timestamp, "last"
    OPTION_TYPE = "option_type", str, "last"  # OptionType
    PRICE = "price", float, "last"
    ASK = "ask", float, "last"
    BID = "bid", float, "last"
    OPEN_INTEREST = "open_interest", float, "last"  # Contracts
    VOLUME = "volume", float, "last"  # Contract
    VOLUME_PREMIUM = "volume_premium", float, "last"  # Money
    VOLUME_NOTIONAL = "volume_notional", float, "last"  # Money
    UNDERLYING_EXPIRATION_DATE = "underlying_expiration_date", pd.Timestamp, "last"

    # Exchange estimate
    EXCHANGE_PRICE = "exchange_price", float, "last"  # Estimated by exchange price
    EXCHANGE_IV = (
        "exchange_iv",
        float,
        "last",
    )  # IV caclculated by exchange based on exchange_price

    # OCHL model
    OPEN = "open", float, "first"
    CLOSE = "close", float, "last"
    HIGH = "high", float, "max"
    LOW = "low", float, "min"

    # ETL
    REQUEST_TIMESTAMP = "request_timestamp", pd.Timestamp, "last"
    ORIGINAL_TIMESTAMP = "original_timestamp", pd.Timestamp, "last"
    LAST = "last", float, "last"
    LOW_24 = "low_24", float, None
    HIGH_24 = "high_24", float, None

    # Added by enrichment or present in options dataframe
    # Future columns
    UNDERLYING_PRICE = "underlying_price", float, "last"

    # Money columns
    INTRINSIC_VALUE = "intrinsic_value", float, "last"
    TIMED_VALUE = "timed_value", float, "last"
    PRICE_STATUS = "price_status", OptionsPriceStatus, "last"

    # Pricer
    # """Price dataframes columns"""
    # # Risk columns
    # LEG_ID = 'leg_id', None, str # chain_pnl_risk_profile
    # RISK_PNL = 'risk_pnl', None, float

    # Greeks
    IV = "iv", float, "mean"
    DELTA = "delta", float, "mean"
    GAMMA = "gamma", float, "mean"
    VEGA = "vega", float, "mean"
    THETA = "theta", float, "mean"
    RHO = "rho", float, "mean"

    # Extra columns that doublets for every row - not mandatory
    SERIES_CODE = (
        "series_code",
        str,
        "last",
    )  # TODO prev 'exchange_symbol'  # Equal to BASE_CODE fot spot
    ASSET_CODE = (
        "asset_code",
        str,
        "last",
    )  # TODO prev 'exchange_symbol'  # Equal to BASE_CODE fot spot
    ASSET_TYPE = "asset_type", str, "last"  # AssetKind TODO prev 'kind'
    UNDERLYING_CODE = (
        "underlying_asset_code",
        str,
        "last",
    )  # TODO prev 'exchange_underlying_symbol' # None for spot
    UNDERLYING_TYPE = "underlying_asset_type", str, "last"  # AssetType
    BASE_CODE = "base_asset_code", str, "last"  # TODO prev 'symbol'
    TITLE = "title", str, "last"
    OPTION_STYLE = "option_style", str, "last"  # OptionStyle
    CURRENCY = "currency", str, "last"  # Currency


OPTION_COLUMNS_DEPENDENCIES = {
    OptionsColumns.INTRINSIC_VALUE: [OptionsColumns.UNDERLYING_PRICE],
    OptionsColumns.TIMED_VALUE: [OptionsColumns.INTRINSIC_VALUE],
    OptionsColumns.PRICE_STATUS: [OptionsColumns.UNDERLYING_PRICE],
}


@enum.unique
class FuturesColumns(EnumDataFrameColumn):
    """
    Pandas option column name, type
    """

    # NAME, value, code, type
    # Base mandatory columns
    TIMESTAMP = (
        OptionsColumns.TIMESTAMP.nm,
        OptionsColumns.TIMESTAMP.type,
        OptionsColumns.TIMESTAMP.resample_func,
    )
    EXPIRATION_DATE = (
        OptionsColumns.EXPIRATION_DATE.nm,
        OptionsColumns.EXPIRATION_DATE.type,
        OptionsColumns.EXPIRATION_DATE.resample_func,
    )
    PRICE = (
        OptionsColumns.PRICE.nm,
        OptionsColumns.PRICE.type,
        OptionsColumns.PRICE.resample_func,
    )
    ASK = (
        OptionsColumns.ASK.nm,
        OptionsColumns.ASK.type,
        OptionsColumns.ASK.resample_func,
    )
    BID = (
        OptionsColumns.BID.nm,
        OptionsColumns.BID.type,
        OptionsColumns.BID.resample_func,
    )
    OPEN_INTEREST = (
        OptionsColumns.OPEN_INTEREST.nm,
        OptionsColumns.OPEN_INTEREST.type,
        OptionsColumns.OPEN_INTEREST.resample_func,
    )
    VOLUME = (
        OptionsColumns.VOLUME.nm,
        OptionsColumns.VOLUME.type,
        OptionsColumns.VOLUME.resample_func,
    )
    VOLUME_NOTIONAL = (
        OptionsColumns.VOLUME_NOTIONAL.nm,
        OptionsColumns.VOLUME_NOTIONAL.type,
        OptionsColumns.VOLUME_NOTIONAL.resample_func,
    )

    # OCHL model
    OPEN = (
        OptionsColumns.OPEN.nm,
        OptionsColumns.OPEN.type,
        OptionsColumns.OPEN.resample_func,
    )
    CLOSE = (
        OptionsColumns.CLOSE.nm,
        OptionsColumns.CLOSE.type,
        OptionsColumns.CLOSE.resample_func,
    )
    HIGH = (
        OptionsColumns.HIGH.nm,
        OptionsColumns.HIGH.type,
        OptionsColumns.HIGH.resample_func,
    )
    LOW = (
        OptionsColumns.LOW.nm,
        OptionsColumns.LOW.type,
        OptionsColumns.LOW.resample_func,
    )

    # ETL
    REQUEST_TIMESTAMP = (
        OptionsColumns.REQUEST_TIMESTAMP.nm,
        OptionsColumns.REQUEST_TIMESTAMP.type,
        OptionsColumns.REQUEST_TIMESTAMP.resample_func,
    )
    ORIGINAL_TIMESTAMP = (
        OptionsColumns.ORIGINAL_TIMESTAMP.nm,
        OptionsColumns.ORIGINAL_TIMESTAMP.type,
        OptionsColumns.ORIGINAL_TIMESTAMP.resample_func,
    )
    LAST = (
        OptionsColumns.LAST.nm,
        OptionsColumns.LAST.type,
        OptionsColumns.LAST.resample_func,
    )
    LOW_24 = (
        OptionsColumns.LOW_24.nm,
        OptionsColumns.LOW_24.type,
        OptionsColumns.LOW_24.resample_func,
    )
    HIGH_24 = (
        OptionsColumns.HIGH_24.nm,
        OptionsColumns.HIGH_24.type,
        OptionsColumns.HIGH_24.resample_func,
    )

    # Extra columns for futures
    SERIES_CODE = (
        OptionsColumns.SERIES_CODE.nm,
        OptionsColumns.SERIES_CODE.type,
        OptionsColumns.SERIES_CODE.resample_func,
    )
    BASE_CODE = (
        OptionsColumns.BASE_CODE.nm,
        OptionsColumns.BASE_CODE.type,
        OptionsColumns.BASE_CODE.resample_func,
    )
    ASSET_TYPE = (
        OptionsColumns.ASSET_TYPE.nm,
        OptionsColumns.ASSET_TYPE.type,
        OptionsColumns.ASSET_TYPE.resample_func,
    )
    ASSET_CODE = (
        OptionsColumns.ASSET_CODE.nm,
        OptionsColumns.ASSET_CODE.type,
        OptionsColumns.ASSET_CODE.resample_func,
    )
    UNDERLYING_TYPE = (
        OptionsColumns.UNDERLYING_TYPE.nm,
        OptionsColumns.UNDERLYING_TYPE.type,
        OptionsColumns.UNDERLYING_TYPE.resample_func,
    )
    UNDERLYING_CODE = (
        OptionsColumns.UNDERLYING_CODE.nm,
        OptionsColumns.UNDERLYING_CODE.type,
        OptionsColumns.UNDERLYING_CODE.resample_func,
    )
    TITLE = (
        OptionsColumns.TITLE.nm,
        OptionsColumns.TITLE.type,
        OptionsColumns.TITLE.resample_func,
    )


@enum.unique
class SpotColumns(EnumDataFrameColumn):
    """
    Pandas option column name, type
    """

    # NAME, value, code, type
    # Base mandatory columns
    TIMESTAMP = (
        OptionsColumns.TIMESTAMP.nm,
        OptionsColumns.TIMESTAMP.type,
        OptionsColumns.TIMESTAMP.resample_func,
    )
    PRICE = (
        OptionsColumns.PRICE.nm,
        OptionsColumns.PRICE.type,
        OptionsColumns.PRICE.resample_func,
    )
    ASK = (
        OptionsColumns.ASK.nm,
        OptionsColumns.ASK.type,
        OptionsColumns.ASK.resample_func,
    )
    BID = (
        OptionsColumns.BID.nm,
        OptionsColumns.BID.type,
        OptionsColumns.BID.resample_func,
    )
    OPEN_INTEREST = (
        OptionsColumns.OPEN_INTEREST.nm,
        OptionsColumns.OPEN_INTEREST.type,
        OptionsColumns.OPEN_INTEREST.resample_func,
    )
    VOLUME = (
        OptionsColumns.VOLUME.nm,
        OptionsColumns.VOLUME.type,
        OptionsColumns.VOLUME.resample_func,
    )
    VOLUME_NOTIONAL = (
        OptionsColumns.VOLUME_NOTIONAL.nm,
        OptionsColumns.VOLUME_NOTIONAL.type,
        OptionsColumns.VOLUME_NOTIONAL.resample_func,
    )

    # Extra columns for spot
    ASSET_CODE = (
        OptionsColumns.ASSET_CODE.nm,
        OptionsColumns.ASSET_CODE.type,
        OptionsColumns.ASSET_CODE.resample_func,
    )
    ASSET_TYPE = (
        OptionsColumns.ASSET_TYPE.nm,
        OptionsColumns.ASSET_TYPE.type,
        OptionsColumns.ASSET_TYPE.resample_func,
    )
    TITLE = (
        OptionsColumns.TITLE.nm,
        OptionsColumns.TITLE.type,
        OptionsColumns.TITLE.resample_func,
    )

    # OCHL model
    OPEN = (
        OptionsColumns.OPEN.nm,
        OptionsColumns.OPEN.type,
        OptionsColumns.OPEN.resample_func,
    )
    CLOSE = (
        OptionsColumns.CLOSE.nm,
        OptionsColumns.CLOSE.type,
        OptionsColumns.CLOSE.resample_func,
    )
    HIGH = (
        OptionsColumns.HIGH.nm,
        OptionsColumns.HIGH.type,
        OptionsColumns.HIGH.resample_func,
    )
    LOW = (
        OptionsColumns.LOW.nm,
        OptionsColumns.LOW.type,
        OptionsColumns.LOW.resample_func,
    )

    # ETL
    REQUEST_TIMESTAMP = (
        OptionsColumns.REQUEST_TIMESTAMP.nm,
        OptionsColumns.REQUEST_TIMESTAMP.type,
        OptionsColumns.REQUEST_TIMESTAMP.resample_func,
    )
    ORIGINAL_TIMESTAMP = (
        OptionsColumns.ORIGINAL_TIMESTAMP.nm,
        OptionsColumns.ORIGINAL_TIMESTAMP.type,
        OptionsColumns.ORIGINAL_TIMESTAMP.resample_func,
    )


OPTION_NON_FUTURES_COLUMN_NAMES = [
    col.nm
    for col in OptionsColumns
    if col.nm not in [f_col.nm for f_col in FuturesColumns]
]
OPTION_NON_SPOT_COLUMN_NAMES = [
    col.nm
    for col in OptionsColumns
    if col.nm not in [s_col.nm for s_col in SpotColumns]
]

ALL_COLUMN_NAMES = list(
    set(
        [col.nm for col in OptionsColumns]
        + [col.nm for col in FuturesColumns]
        + [col.nm for col in SpotColumns]
    )
)
