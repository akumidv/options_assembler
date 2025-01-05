"""Pandas columns code, value and types"""
import enum
import datetime
from option_lib.entities.enum_code import EnumColumnType


@enum.unique
class ProviderOptionColumns(EnumColumnType):
    """
    Pandas option column value for chart, code, type
    """
    # NAME, value, code, type
    # Base mandatory columns
    DATETIME = 'datetime', datetime.date
    STRIKE = 'strike', float
    EXPIRATION_DATE = 'expiration_date', datetime.date
    TYPE = 'type', str  # OptionType
    PREMIUM = 'premium', float
    FUTURES_EXPIRATION_DATE = 'futures_expiration_date', datetime.date
 # DATETIME = 'Datetime', 'datetime', datetime.date
 #    STRIKE = 'Strike', 'strike', float
 #    EXPIRATION_DATE = 'Expiration date', 'expiration_date', datetime.date
 #    TYPE = 'Type', 'type', str  # OptionType
 #    PREMIUM = 'Premium', 'premium', float
 #    FUTURES_EXPIRATION_DATE = 'Futures expiration date', 'futures_expiration_date', datetime.date


@enum.unique
class ProviderFuturesColumns(EnumColumnType):
    """
    Pandas option column value for chart, code, type
    """
    # NAME, value, code, type
    # Base mandatory columns
    DATETIME = 'datetime',  datetime.date
    EXPIRATION_DATE = 'expiration_date',  datetime.date
    PRICE = 'price', float
    # DATETIME = 'datetime', 'datetime',  datetime.date
    # EXPIRATION_DATE = 'expiration_date', 'expiration_date',  datetime.date
    # PRICE = 'price', 'price', float
