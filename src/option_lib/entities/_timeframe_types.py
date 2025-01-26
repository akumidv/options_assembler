"""Timeframe values"""
import enum
from option_lib.entities import EnumMultiplier

@enum.unique
class Timeframe(EnumMultiplier):
    """Types fo period"""
    EOD = 'EOD', 1440
    MINUTE_1 = '1m', 1
    MINUTE_5 = '5m', 5
    MINUTE_15 = '15m', 15
    MINUTE_30 = '30m', 30
    HOUR_1 = '1h', 60
    HOUR_4 = '4h', 240
