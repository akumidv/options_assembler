"""Timeframe values"""
import enum


@enum.unique
class Timeframe(enum.Enum):
    """Types fo period"""
    EOD = 'EOD'
    MINUTE_1 = '1m'
    MINUTE_5 = '5m'
    HOUR_1 = '1h'
