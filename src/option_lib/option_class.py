"""Ralisation for option class"""
import datetime

import pandas as pd

from option_lib.entities import TimeframeCode

from option_lib.provider import AbstractProvider, RequestParameters
from option_lib.option_data_class import OptionData
from option_lib.enrichment import OptionEnrichment
from option_lib.chain import OptionChain
from option_lib.analytic import OptionAnalytic


class Option:
    """Base option class that provide possibility to work with option data different way"""
    _data: OptionData

    def __init__(self, provider: AbstractProvider, option_symbol: str, params: RequestParameters | None = None,
                 option_columns: list | None = None, future_columns: list | None = None):
        self._data = OptionData(provider, option_symbol, params, option_columns, future_columns)
        self.enrichment: OptionEnrichment = OptionEnrichment(self._data)
        self.chain: OptionChain = OptionChain(self._data)
        self.analytic: OptionAnalytic = OptionAnalytic(self._data)

    @property
    def option_symbol(self) -> str:
        """Option symbol"""
        return self._data.option_symbol

    @property
    def period_from(self) -> int | datetime.date | datetime.datetime | None:
        """Option data period from """
        return self._data.period_from

    @property
    def period_to(self) -> int | datetime.date | datetime.datetime | None:
        """Option data period to """
        return self._data.period_to

    @property
    def timeframe(self) -> TimeframeCode:
        """Option data timeframe"""
        return self._data.timeframe

    @property
    def df_hist(self):
        """Option dataframe getter"""
        return self._data.df_hist

    @df_hist.setter
    def df_hist(self, df: pd.DataFrame):
        """Option dataframe setter"""
        self._data.df_hist = df

    @property
    def df_fut(self) -> pd.DataFrame:
        """Future dataframe getter"""
        return self._data.df_fut

    @df_fut.setter
    def df_fut(self, df: pd.DataFrame):
        """Future dataframe setter"""
        self._data.df_fut = df
