"""Implementation for price charts"""
import datetime
import pandas as pd

from plotly import graph_objs as go
from options_assembler.entities import OptionColumns as OCl, OptionType
from options_assembler.option_data_class import OptionData
from options_assembler.analytic import OptionAnalytic
from options_assembler.chart.price._time_value_chart import get_chart_data_for_time_values_series


class ChartPriceClass:
    """Price charts
    add chart engines plottly https://plotly.com/python/line-and-scatter/
    """

    def __init__(self, data: OptionData):
        self._data = data
        self._analytic: OptionAnalytic = OptionAnalytic(self._data)
        self.figure_data: list = []

    def empty_figure_data(self):
        """Emtpy all figures data"""
        self.figure_data: list = []

    def _get_time_values_dfs(self, df: None | pd.DataFrame | list[pd.DataFrame],
                             expiration_date: pd.Timestamp | None,
                             option_type: OptionType | None,
                             strike: float | None = None, distance: float | None = None
                             ) -> list[pd.DataFrame]:
        if df is None:
            if strike is not None:
                df = self._analytic.price.time_value_series_by_strike_to_atm_distance(strike, expiration_date,
                                                                                      option_type or OptionType.CALL)
            else:
                df = self._analytic.price.time_value_series_by_atm_distance(distance, expiration_date,
                                                                            option_type or OptionType.CALL)
        if isinstance(df, pd.DataFrame):
            dfs = [df]
        else:
            dfs = df
        for df_ts in dfs:
            if OCl.TIMESTAMP.nm not in df_ts.columns:
                raise KeyError(f'{OCl.TIMESTAMP.nm} not in dataframe columns')
            if OCl.TIME_VALUE.nm not in df_ts.columns:
                raise KeyError(f'{OCl.TIME_VALUE.nm} not in dataframe columns')
        return dfs

    @staticmethod
    def _get_time_values_names(name, is_empty_df: bool) -> list[str]:
        return ['Call ATM for nearest expiration date'] if is_empty_df is None else (
            [name] if not isinstance(name, list) else name)

    def time_values(self, df: None | pd.DataFrame | list[pd.DataFrame] = None, name: str | list[str] | None = None,
                    expiration_date: pd.Timestamp | None = None) -> list[go.Scatter]:
        """Prepare chart data. Default return chart for call with ATM strike, for nearest or set expiration.
          Expiration ignored if dfs are set"""
        dfs = self._get_time_values_dfs(df, expiration_date=expiration_date, option_type=None, strike=None)
        names = self._get_time_values_names(name, df is None)
        data = get_chart_data_for_time_values_series(dfs, names)
        self.figure_data.extend(data)
        return data

    def time_values_for_strike(self, df: None | pd.DataFrame = None,
                               strike: float | None = None,
                               expiration_date: datetime.date | None = None,
                               option_type: OptionType | None = OptionType.CALL,
                               name: str | None = None) -> list[go.Scatter]:
        """Implement full process with analyze than requests"""
        dfs = self._get_time_values_dfs(df, strike=strike, expiration_date=expiration_date, option_type=option_type)
        return self.time_values(dfs, name=name)

    def time_values_for_distance(self, df: None | pd.DataFrame = None,
                                 distance: float | None = None,
                                 expiration_date: datetime.date | None = None,
                                 option_type: OptionType | None = OptionType.CALL,
                                 name: str | None = None) -> list[go.Scatter]:
        """Implement full process with analyze than requests"""
        dfs = self._get_time_values_dfs(df, distance=distance, expiration_date=expiration_date, option_type=option_type)
        return self.time_values(dfs, name=name)
