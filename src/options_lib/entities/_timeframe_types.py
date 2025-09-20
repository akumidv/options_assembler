"""Timeframe values"""
import enum
from options_lib.entities import EnumMultiplier


@enum.unique
class Timeframe(EnumMultiplier):
    """Types fo period"""
    EOD = 'EOD', 1440, '1D'
    MINUTE_1 = '1m', 1, '1min'
    MINUTE_5 = '5m', 5, '5min'
    MINUTE_15 = '15m', 15, '15min'
    MINUTE_30 = '30m', 30, '30min'
    HOUR_1 = '1h', 60, '1h'
    HOUR_4 = '4h', 240, '4h'

    def __new__(cls, value: str, mult: int | float, offset: str):
        obj = object.__new__(cls)
        obj._value_ = value
        obj.mult = mult
        obj.nm = value
        obj.offset = offset
        return obj
