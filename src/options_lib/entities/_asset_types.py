"""Instrument types"""
import enum
from options_lib.entities.enum_code import EnumCode


@enum.unique
class AssetType(EnumCode):
    """AssetType enumerates the different types of financial instruments supported.

    Examples:
        AssetType.SHARE.value -> 'share'
        AssetType.SHARE.code  -> 's'
    """
    SHARE = 'share', 's'
    COMMODITY = 'commodity', 'm'
    INDEX = 'index', 'i'
    CURRENCY = 'currency', 'c'
    CRYPTO = 'crypto', 'y'
    # FUTURES = 'futures', 'f'
    # OPTIONS = 'options', 'o'


@enum.unique
class AssetKind(EnumCode):
    """
    Usage code for database and parquet to reduce time for filtering
    """
    OPTIONS = 'options', 'o'  # AssetType.OPTIONS.value, AssetType.OPTIONS.code
    FUTURES = 'futures', 'f'  # AssetType.FUTURES.value, AssetType.FUTURES.code
    SPOT = 'spot', 's'  # No sense
