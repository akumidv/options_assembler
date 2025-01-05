""""Public price analytic api class that should hide realization of functions"""

import pandas as pd

from option_lib.option_data_class import OptionData
from option_lib.analytic.price.time_values import (
    time_value_series_by_strike_to_atm_distance, time_value_series_by_atm_distance
)


class OptionAnalyticPrice:
    """
    Wrapper about price analytics modules functions
    """

    def __init__(self, data: OptionData):
        self._data = data

    def time_value_series_by_strike_to_atm_distance(self, strike: float) -> pd.DataFrame:
        """Get time value series by strike to atm distance"""
        return time_value_series_by_strike_to_atm_distance(self._data.df_hist, strike)

    def time_value_series_by_atm_distance(self, distance: float = 0) -> pd.DataFrame:
        """Get time value series by distance from atm"""
        return time_value_series_by_atm_distance(self._data.df_hist, distance)
