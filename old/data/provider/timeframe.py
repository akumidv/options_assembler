"""
TODO Main goal to convert to timeframes from request that made in some near time for timeframe value and fill gaps if request wasn't made

"""


def normalize_timeframe_for_eod():
    """
    Idea to get period of exchange working hours, and if datetime not in working hours - its datetime for eod
    There also have sense to check working days for exchange. Probably https://pypi.org/project/exchange-calendars/
    """


def normalize_timeframe_from_request_datetime():
    """
    Idea to check request datetime and compare to timeframe for that endpoint who do not provide history only real-time data.
    If we have nearest datetime for timeframe use it. Fill with NaN if we do not have. But if we have some for first
    and do not for second time interval, but have addition records there - usage it.
    """