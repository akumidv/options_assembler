"""Provider entities tests"""
import datetime
from option_lib.entities import TimeframeCode

from option_lib.provider import RequestParameters


def test_provider_parameters():
    cur_dt = datetime.date.today()
    provider_params = RequestParameters(period_from=None, period_to=cur_dt.year,
                                        timeframe=TimeframeCode.EOD, columns=None)
    assert isinstance(provider_params, RequestParameters)
    assert provider_params.period_to == cur_dt.year
