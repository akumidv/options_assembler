"""Provider entities"""
import enum
import datetime

from pydantic import BaseModel
from option_lib.entities import TimeframeCode


class DataEngine(enum.Enum):
    """Data engines"""
    PANDAS = 'pandas'
    POLARIS = 'polaris'
    DASK = 'dask'
    SPARK = 'spark'


class DataSource(enum.Enum):
    """Source of data"""
    LOCAL = 'local'
    S3 = 's3'
    API = 'api'


class RequestParameters(BaseModel):
    """Parameters to request provider data"""
    period_from: int | datetime.date | datetime.datetime | None = None
    period_to: int | datetime.date | datetime.datetime | None = None
    timeframe: TimeframeCode = TimeframeCode.EOD
    columns: list | None = None
