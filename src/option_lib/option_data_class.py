"""Option data class realisation"""
import datetime

import pandas as pd

from option_lib.entities import TimeframeCode
from option_lib.provider import AbstractProvider, RequestParameters


class OptionData:
    """Option data hide from user provider realisation and allow to share data with different option lib components"""
    _df_opt = None
    _df_fut = None

    def __init__(self, provider: AbstractProvider, option_symbol: str,
                 provider_params: RequestParameters | None = None, opt_columns: list | None = None,
                 fut_columns: list | None = None
                 ):
        self._provider = provider
        self._option_symbol = option_symbol
        self._provider_params = provider_params
        self._opt_columns = opt_columns if isinstance(opt_columns, list) else AbstractProvider.option_columns
        self._fut_columns = opt_columns if isinstance(fut_columns, list) else AbstractProvider.future_columns

    @property
    def option_symbol(self) -> str:
        """Option symbol"""
        return self._option_symbol

    @property
    def period_from(self) -> int | datetime.date | datetime.datetime | None:
        """Option data period from """
        return self._period_from

    @property
    def period_to(self) -> int | datetime.date | datetime.datetime | None:
        """Option data period to """
        return self._period_to

    @property
    def timeframe(self) -> TimeframeCode:
        """Option data timeframe"""
        return self._timeframe

    @property
    def df_opt(self) -> pd.DataFrame:
        """Option dataframe getter"""
        if self._df_opt is None:
            opt_params = RequestParameters(period_from=self._provider_params.period_from,
                                           period_to=self._provider_params.period_to,
                                           timeframe=self._provider_params.timeframe,
                                           columns=self._opt_columns)
            self._df_opt = self._provider.load_option(self._option_symbol, params=opt_params)
        return self._df_opt

    @df_opt.setter
    def df_opt(self, df: pd.DataFrame):
        """Option dataframe setter"""
        self._df_opt = df

    @property
    def df_fut(self) -> pd.DataFrame:
        """Future dataframe getter"""
        if self._df_fut is None:
            fut_params = RequestParameters(period_from=self._provider_params.period_from,
                                           period_to=self._provider_params.period_to,
                                           timeframe=self._provider_params.timeframe,
                                           columns=self._fut_columns)
            self._df_fut = self._provider.load_future(self._option_symbol,  params=fut_params)
        return self._df_fut

    @df_fut.setter
    def df_fut(self, df: pd.DataFrame):
        """Future dataframe setter"""
        self._df_fut = df
