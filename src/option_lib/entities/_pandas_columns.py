"""Pandas columns code, value and types"""
import enum

from option_lib.entities.enum_code import EnumColumnType
from option_lib.entities._option_types import OptionPriceStatus

from option_lib.entities._provider_pandas_columns import ProviderOptionColumns as POCl
from option_lib.entities._provider_pandas_columns import ProviderFuturesColumns


@enum.unique
class OptionColumns(EnumColumnType):
    """
    Pandas option column value for chart, code, type
    """
    # NAME, value, code, type
    # Base mandatory columns from provider
    # DATETIME = POCl.DATETIME.value, POCl.DATETIME.nm, POCl.DATETIME.type
    # STRIKE = POCl.STRIKE.value, POCl.STRIKE.nm, POCl.STRIKE.type
    # EXPIRATION_DATE = POCl.EXPIRATION_DATE.value, POCl.EXPIRATION_DATE.nm, POCl.EXPIRATION_DATE.type
    # TYPE = POCl.TYPE.value, POCl.TYPE.nm, POCl.TYPE.type
    # PREMIUM = POCl.PREMIUM.value, POCl.PREMIUM.nm, POCl.PREMIUM.type
    # FUTURES_EXPIRATION_DATE = POCl.FUTURES_EXPIRATION_DATE.value, POCl.FUTURES_EXPIRATION_DATE.nm, \
    #     POCl.FUTURES_EXPIRATION_DATE.type

    DATETIME = POCl.DATETIME.nm,  POCl.DATETIME.type
    STRIKE = POCl.STRIKE.nm,  POCl.STRIKE.type
    EXPIRATION_DATE = POCl.EXPIRATION_DATE.nm, POCl.EXPIRATION_DATE.type
    TYPE = POCl.TYPE.nm, POCl.TYPE.type
    PREMIUM = POCl.PREMIUM.nm, POCl.PREMIUM.type
    FUTURES_EXPIRATION_DATE = POCl.FUTURES_EXPIRATION_DATE.nm, POCl.FUTURES_EXPIRATION_DATE.type

    # Added by enrichment
    # Future columns
    # FUTURES_PRICE = 'Futures Price', 'futures_price', float
    FUTURES_PRICE = 'futures_price', float

    # Money columns
    INTRINSIC_VALUE = 'intrinsic_value', float
    TIME_VALUE = 'time_value', float
    PRICE_STATUS = 'price_status', OptionPriceStatus
    # INTRINSIC_VALUE = 'Intrinsic Value', 'intrinsic_value', float
    # TIME_VALUE = 'Time Value', 'time_value', float
    # MONEY_STATUS = 'Money Place', 'money_status', OptionMoneyStatus

    # Pricer

    # Greeks


FuturesColumns = ProviderFuturesColumns
