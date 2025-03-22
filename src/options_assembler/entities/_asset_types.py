"""Instrument types"""
import enum
from options_assembler.entities.enum_code import EnumCode


@enum.unique
class AssetKind(EnumCode):
    """
    Usage code for database and parquet to reduce time for filtering
    """
    OPTION = 'option', 'o'
    FUTURE = 'future', "f"
    SPOT = 'spot', 's'


@enum.unique
class AssetType(EnumCode):
    """
    Usage code for database and parquet to reduce time for filtering
    """
    STOCK = 'stock', 's'
    COMMODITIES = 'm'
    INDEX = 'index', 'i'
    CURRENCY = 'currency', 'c'
    CRYPTO = 'crypto', 'y'
