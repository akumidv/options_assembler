"""Instrument types"""
import enum
from option_lib.entities.enum_code import EnumCode


@enum.unique
class AssetType(EnumCode):
    SHARE = 'share', 's'
    COMMODITY = 'commodity', 'm'
    INDEX = 'index', 'i'
    CURRENCY = 'currency', 'c'
    CRYPTO = 'crypto', 'y'
    FUTURES = 'future', 'f'
    OPTION = 'option', 'o'

@enum.unique
class AssetKind(EnumCode):
    """
    Usage code for database and parquet to reduce time for filtering
    """
    OPTION = AssetType.OPTION.value, AssetType.OPTION.code
    FUTURES = AssetType.FUTURES.value, AssetType.FUTURES.code # TODO rename to futures but also should be data folders rename
    SPOT = 'spot', 's'  # No sense
