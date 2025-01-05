"""Timeframe values"""
import enum


@enum.unique
class TimeframeCode(enum.Enum):
    """Types fo period"""
    EOD = 'EOD'
