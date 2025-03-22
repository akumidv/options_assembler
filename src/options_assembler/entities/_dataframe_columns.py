"""Pandas columns code, value and types"""
import enum
import datetime
from typing import OrderedDict
from options_assembler.entities.enum_code import EnumDataFrameColumn
from options_assembler.entities._option_types import OptionPriceStatus
import pandas as pd


@enum.unique
class OptionColumns(EnumDataFrameColumn):
    """
    Pandas option column name, type
    """
    # NAME, value, code, type, resample function name
    # Base mandatory columns from provider
    TIMESTAMP = 'timestamp', pd.Timestamp, 'last',
    STRIKE = 'strike', float, 'last'
    EXPIRATION_DATE = 'expiration_date', pd.Timestamp, 'last'
    OPTION_TYPE = 'option_type', str, 'last'  # OptionType
    PRICE = 'price', float, 'last'
    ASK = 'ask', float, 'last'
    BID = 'bid', float, 'last'
    OPEN_INTEREST = 'open_interest', float, 'last'
    VOLUME = 'volume', float, 'last'
    VOLUME_NOTIONAL = 'volume_notional', float, 'last'
    UNDERLYING_EXPIRATION_DATE = 'underlying_expiration_date', pd.Timestamp, 'last'

    # Extra columns for options (joined for example), not mandatory
    SYMBOL = 'symbol', str, 'last'
    EXCHANGE_SYMBOL = 'exchange_symbol', str, 'last'
    EXCHANGE_UNDERLYING_SYMBOL = 'exchange_underlying_symbol', str, 'last'
    KIND = 'kind', str, 'last'  # AssetKind
    EXCHANGE_PRICE = 'exchange_price', float, 'last'  # Estimated by exchange price
    EXCHANGE_IV = 'exchange_iv', float, 'last'  # IV caclculated by exchange based on exchange_price

    # OCHL model
    OPEN = 'open', float, 'first'
    CLOSE = 'close', float, 'last'
    HIGH = 'high', float, 'max'
    LOW = 'low', float, 'min'

    # ETL
    REQUEST_TIMESTAMP = 'request_timestamp', pd.Timestamp, 'last'
    ORIGINAL_TIMESTAMP = 'original_timestamp', pd.Timestamp, 'last'
    LAST = 'last', float, 'last'
    LOW_24 = 'low_24', float, None
    HIGH_24 = 'high_24', float, None

    # Added by enrichment or present in options dataframe
    # Future columns
    UNDERLYING_PRICE = 'underlying_price', float, 'last'

    # Money columns
    INTRINSIC_VALUE = 'intrinsic_value', float, 'last'
    TIME_VALUE = 'time_value', float, 'last'
    PRICE_STATUS = 'price_status', OptionPriceStatus, 'last'

    # Pricer
    # """Price dataframes columns"""
    # # Risk columns
    # LEG_ID = 'leg_id', None, str # chain_pnl_risk_profile
    # RISK_PNL = 'risk_pnl', None, float

    # Greeks


OPTION_COLUMNS_DEPENDENCIES = {
    OptionColumns.INTRINSIC_VALUE: [OptionColumns.UNDERLYING_PRICE],
    OptionColumns.TIME_VALUE: [OptionColumns.INTRINSIC_VALUE],
    OptionColumns.PRICE_STATUS: [OptionColumns.UNDERLYING_PRICE],
}


@enum.unique
class FuturesColumns(EnumDataFrameColumn):
    """
    Pandas option column name, type
    """
    # NAME, value, code, type
    # Base mandatory columns
    TIMESTAMP = OptionColumns.TIMESTAMP.nm, OptionColumns.TIMESTAMP.type, OptionColumns.TIMESTAMP.resample_func
    EXPIRATION_DATE = OptionColumns.EXPIRATION_DATE.nm, OptionColumns.EXPIRATION_DATE.type, OptionColumns.EXPIRATION_DATE.resample_func
    PRICE = OptionColumns.PRICE.nm, OptionColumns.PRICE.type, OptionColumns.PRICE.resample_func
    ASK = OptionColumns.ASK.nm, OptionColumns.ASK.type, OptionColumns.ASK.resample_func
    BID = OptionColumns.BID.nm, OptionColumns.BID.type, OptionColumns.BID.resample_func
    OPEN_INTEREST = OptionColumns.OPEN_INTEREST.nm, OptionColumns.OPEN_INTEREST.type, OptionColumns.OPEN_INTEREST.resample_func
    VOLUME = OptionColumns.VOLUME.nm, OptionColumns.VOLUME.type, OptionColumns.VOLUME.resample_func
    VOLUME_NOTIONAL = OptionColumns.VOLUME_NOTIONAL.nm, OptionColumns.VOLUME_NOTIONAL.type, OptionColumns.VOLUME_NOTIONAL.resample_func

    # Extra columns for futures
    SYMBOL = OptionColumns.SYMBOL.nm, OptionColumns.SYMBOL.type, OptionColumns.SYMBOL.resample_func
    KIND = OptionColumns.KIND.nm, OptionColumns.KIND.type, OptionColumns.KIND.resample_func
    EXCHANGE_SYMBOL = OptionColumns.EXCHANGE_SYMBOL.nm, OptionColumns.EXCHANGE_SYMBOL.resample_func, OptionColumns.EXCHANGE_SYMBOL.resample_func

    # OCHL model
    OPEN = OptionColumns.OPEN.nm, OptionColumns.OPEN.type, OptionColumns.OPEN.resample_func
    CLOSE = OptionColumns.CLOSE.nm, OptionColumns.CLOSE.type, OptionColumns.CLOSE.resample_func
    HIGH = OptionColumns.HIGH.nm, OptionColumns.HIGH.type, OptionColumns.HIGH.resample_func
    LOW = OptionColumns.LOW.nm, OptionColumns.LOW.type, OptionColumns.LOW.resample_func

    # ETL
    REQUEST_TIMESTAMP = OptionColumns.REQUEST_TIMESTAMP.nm, OptionColumns.REQUEST_TIMESTAMP.type, OptionColumns.REQUEST_TIMESTAMP.resample_func
    ORIGINAL_TIMESTAMP = OptionColumns.ORIGINAL_TIMESTAMP.nm, OptionColumns.ORIGINAL_TIMESTAMP.type, OptionColumns.ORIGINAL_TIMESTAMP.resample_func
    LAST = OptionColumns.LAST.nm, OptionColumns.LAST.type, OptionColumns.LAST.resample_func
    LOW_24 = OptionColumns.LOW_24.nm, OptionColumns.LOW_24.type, OptionColumns.LOW_24.resample_func
    HIGH_24 = OptionColumns.HIGH_24.nm, OptionColumns.HIGH_24.type, OptionColumns.HIGH_24.resample_func


@enum.unique
class SpotColumns(EnumDataFrameColumn):
    """
    Pandas option column name, type
    """
    # NAME, value, code, type
    # Base mandatory columns
    TIMESTAMP = OptionColumns.TIMESTAMP.nm, OptionColumns.TIMESTAMP.type, OptionColumns.TIMESTAMP.resample_func
    PRICE = OptionColumns.PRICE.nm, OptionColumns.PRICE.type, OptionColumns.PRICE.resample_func
    ASK = OptionColumns.ASK.nm, OptionColumns.ASK.type, OptionColumns.ASK.resample_func
    BID = OptionColumns.BID.nm, OptionColumns.BID.type, OptionColumns.BID.resample_func
    OPEN_INTEREST = OptionColumns.OPEN_INTEREST.nm, OptionColumns.OPEN_INTEREST.type, OptionColumns.OPEN_INTEREST.resample_func
    VOLUME = OptionColumns.VOLUME.nm, OptionColumns.VOLUME.type, OptionColumns.VOLUME.resample_func
    VOLUME_NOTIONAL = OptionColumns.VOLUME_NOTIONAL.nm, OptionColumns.VOLUME_NOTIONAL.type, OptionColumns.VOLUME_NOTIONAL.resample_func

    # Extra columns for futures
    SYMBOL = OptionColumns.SYMBOL.nm, OptionColumns.SYMBOL.type, OptionColumns.SYMBOL.resample_func
    KIND = OptionColumns.KIND.nm, OptionColumns.KIND.type, OptionColumns.KIND.resample_func
    EXCHANGE_SYMBOL = OptionColumns.EXCHANGE_SYMBOL.nm, OptionColumns.EXCHANGE_SYMBOL.type, OptionColumns.EXCHANGE_SYMBOL.resample_func

    # OCHL model
    OPEN = OptionColumns.OPEN.nm, OptionColumns.OPEN.type, OptionColumns.OPEN.resample_func
    CLOSE = OptionColumns.CLOSE.nm, OptionColumns.CLOSE.type, OptionColumns.CLOSE.resample_func
    HIGH = OptionColumns.HIGH.nm, OptionColumns.HIGH.type, OptionColumns.HIGH.resample_func
    LOW = OptionColumns.LOW.nm, OptionColumns.LOW.type, OptionColumns.LOW.resample_func

    # ETL
    REQUEST_TIMESTAMP = OptionColumns.REQUEST_TIMESTAMP.nm, OptionColumns.REQUEST_TIMESTAMP.type, OptionColumns.REQUEST_TIMESTAMP.resample_func
    ORIGINAL_TIMESTAMP = OptionColumns.ORIGINAL_TIMESTAMP.nm, OptionColumns.ORIGINAL_TIMESTAMP.type, OptionColumns.ORIGINAL_TIMESTAMP.resample_func


OPTION_NON_FUTURES_COLUMN_NAMES = [col.nm for col in OptionColumns if
                                   col.nm not in [f_col.nm for f_col in FuturesColumns]]
OPTION_NON_SPOT_COLUMN_NAMES = [col.nm for col in OptionColumns if col.nm not in [s_col.nm for s_col in SpotColumns]]

ALL_COLUMN_NAMES = list(set([col.nm for col in OptionColumns] + [col.nm for col in FuturesColumns] +
                            [col.nm for col in SpotColumns]))
