""""Public price analytic api class that should hide realization of functions"""
import pandas as pd

from options_assembler.entities import OptionType, OptionColumns as OCl
from options_assembler.option_data_class import OptionData
from options_assembler.analytic.price._time_values import (
    time_value_series_by_strike_to_atm_distance, time_value_series_by_atm_distance
)
from options_assembler.enrichment import OptionEnrichment


class OptionAnalyticPrice:
    """
    Wrapper about price analytics modules functions
    """

    def __init__(self, data: OptionData):
        self._data = data
        self._enrichment: OptionEnrichment = OptionEnrichment(self._data)

    def time_value_series_by_strike_to_atm_distance(self, strike: float | None = None,
                                                    expiration_date: pd.Timestamp | None = None,
                                                    option_type: OptionType | None = OptionType.CALL) -> pd.DataFrame:
        """Get time value series by strike to atm distance"""
        self._enrichment.enrich_options([OCl.INTRINSIC_VALUE, OCl.TIME_VALUE])
        return time_value_series_by_strike_to_atm_distance(self._data.df_hist, strike, expiration_date, option_type)

    def time_value_series_by_atm_distance(self, distance: float | None = None,
                                          expiration_date: pd.Timestamp | None = None,
                                          option_type: OptionType | None = OptionType.CALL) -> pd.DataFrame:
        """Get time value series by distance from atm"""
        self._enrichment.enrich_options([OCl.INTRINSIC_VALUE, OCl.TIME_VALUE])
        return time_value_series_by_atm_distance(self._data.df_hist, distance, expiration_date, option_type)
