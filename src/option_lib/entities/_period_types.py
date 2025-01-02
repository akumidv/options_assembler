import enum
import datetime
from pydantic.dataclasses import dataclass
from pydantic import BaseModel, PositiveInt


@enum.unique
class PeriodType(enum.Enum):
    """Types fo period"""
    YEAR = 'year'
    YEARS_PERIOD = 'year_period'


@dataclass
class Period:
    period_type: PeriodType = PeriodType.YEAR
    year: PositiveInt | None = None
    year_period: tuple[PositiveInt, PositiveInt] | None = None
    date: datetime.date | None = None
    date_period: tuple[datetime.date, datetime.date] | None = None
