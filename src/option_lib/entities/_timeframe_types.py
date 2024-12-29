import enum
import datetime
from pydantic.dataclasses import dataclass
from pydantic import BaseModel, PositiveInt


@enum.unique
class TimeframeCode(enum.Enum):
    """Types fo period"""
    EOD = 'EOD'
