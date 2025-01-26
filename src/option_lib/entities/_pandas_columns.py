"""Pandas columns code, value and types"""
import enum
import datetime
from option_lib.entities.enum_code import EnumColumnType
from option_lib.entities._option_types import OptionPriceStatus
import pandas as pd

@enum.unique
class OptionColumns(EnumColumnType):
    """
    Pandas option column name, type
    """
    # NAME, value, code, type
    # Base mandatory columns from provider
    TIMESTAMP = 'timestamp', pd.Timestamp
    STRIKE = 'strike', float
    EXPIRATION_DATE = 'expiration_date', pd.Timestamp
    OPTION_TYPE = 'option_type', str  # OptionType
    PRICE = 'price', float
    UNDERLYING_EXPIRATION_DATE = 'underlying_expiration_date', pd.Timestamp

    # Extra columns for options (joined for example)
    SYMBOL = 'symbol', str
    EXCHANGE_SYMBOL = 'exchange_symbol', str
    EXCHANGE_UNDERLYING_SYMBOL = 'exchange_underlying_symbol', str
    KIND = 'kind', str  # AssetKind

    # Added by enrichment
    # Future columns
    # FUTURES_PRICE = 'Futures Price', 'futures_price', float
    UNDERLYING_PRICE = 'underlying_price', float

    # Money columns
    INTRINSIC_VALUE = 'intrinsic_value', float
    TIME_VALUE = 'time_value', float
    PRICE_STATUS = 'price_status', OptionPriceStatus

    # Pricer

    # Greeks

    # ETL
    REQUEST_TIMESTAMP = 'request_timestamp', pd.Timestamp


@enum.unique
class FuturesColumns(EnumColumnType):
    """
    Pandas option column name, type
    """
    # NAME, value, code, type
    # Base mandatory columns
    TIMESTAMP = OptionColumns.TIMESTAMP.nm, OptionColumns.TIMESTAMP.type
    EXPIRATION_DATE = OptionColumns.EXPIRATION_DATE.nm,  OptionColumns.EXPIRATION_DATE.type
    PRICE = OptionColumns.PRICE.nm, OptionColumns.PRICE.type

    # Extra columns for futures
    SYMBOL = OptionColumns.SYMBOL.nm, OptionColumns.SYMBOL.type
    KIND = OptionColumns.KIND.nm, OptionColumns.KIND.type
    EXCHANGE_ASSET_SYMBOL = OptionColumns.EXCHANGE_SYMBOL.nm, OptionColumns.EXCHANGE_SYMBOL.type

    # ETL
    REQUEST_TIMESTAMP = 'request_datetime', pd.Timestamp

@enum.unique
class SpotColumns(EnumColumnType):
    """
    Pandas option column name, type
    """
    # NAME, value, code, type
    # Base mandatory columns
    TEMESTAMP = OptionColumns.TIMESTAMP.nm, OptionColumns.TIMESTAMP.type
    PRICE = OptionColumns.PRICE.nm, OptionColumns.PRICE.type

    # Extra columns for futures
    SYMBOL = OptionColumns.SYMBOL.nm, OptionColumns.SYMBOL.type
    KIND = OptionColumns.KIND.nm, OptionColumns.KIND.type
    EXCHANGE_ASSET_SYMBOL = OptionColumns.EXCHANGE_SYMBOL.nm, OptionColumns.EXCHANGE_SYMBOL.type

    # ETL
    REQUEST_TIMESTAMP = 'request_datetime', pd.Timestamp

OPTION_NON_FUTURES_COLUMN_NAMES = [col.nm for col in OptionColumns if col.nm not in [f_col.nm for f_col in FuturesColumns]]
OPTION_NON_SPOT_COLUMN_NAMES = [col.nm for col in OptionColumns if col.nm not in [s_col.nm for s_col in SpotColumns]]
