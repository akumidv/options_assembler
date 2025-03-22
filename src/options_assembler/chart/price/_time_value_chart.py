"""Plotly implementation for time value charts"""
import pandas as pd
from plotly import graph_objs as go  # Check for version: from chart_studio import plotly
from options_assembler.entities import OptionColumns as OCl


def get_chart_data_for_time_values_series(df_time_series: pd.DataFrame | list[pd.DataFrame],
                                          name: str | list[str] | None = None) -> list[go.Scatter]:
    """
    Prepare data object for plotly for time_values dataframe
    mandatory columns: datetime, time_value
    """
    if isinstance(df_time_series, pd.DataFrame):
        dfs = [df_time_series]
        names = [name]
    else:
        dfs = df_time_series
        names = name
    data = []
    for df, chart_name in zip(dfs, names):
        data.append(
            go.Scatter(x=df[OCl.TIMESTAMP.nm].to_list(), y=df[OCl.TIME_VALUE.nm].to_list(),
                       mode='lines', name=chart_name)
        )
    return data
