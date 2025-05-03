"""Option data class realisation"""
import datetime

import pandas as pd

from option_lib.entities import Timeframe, OptionColumns as OCl
from options_assembler.provider import AbstractProvider, RequestParameters


class OptionData:
    """Option data hide from user provider realisation and allow to share data with different option lib components"""
    _df_hist = None
    _df_fut = None
    _df_chain = None

    def __init__(self, provider: AbstractProvider, option_symbol: str,
                 provider_params: RequestParameters | None = None, option_columns: list | None = None,
                 future_columns: list | None = None
                 ):
        self._provider = provider
        self._option_symbol = option_symbol
        self._provider_params = provider_params
        self._opt_columns = option_columns if isinstance(option_columns, list) else AbstractProvider.option_columns
        self._fut_columns = future_columns if isinstance(future_columns, list) else AbstractProvider.future_columns

    @property
    def option_symbol(self) -> str:
        """Option symbol"""
        return self._option_symbol

    @property
    def period_from(self) -> int | datetime.date | datetime.datetime | None:
        """Option data period from """
        return self._provider_params.period_from

    @property
    def period_to(self) -> int | datetime.date | datetime.datetime | None:
        """Option data period to """
        return self._provider_params.period_to

    @property
    def timeframe(self) -> Timeframe:
        """Option data timeframe"""
        return self._provider_params.timeframe

    @property
    def df_hist(self) -> pd.DataFrame:
        """Option dataframe getter"""
        if self._df_hist is None:
            opt_params = RequestParameters(period_from=self._provider_params.period_from,
                                           period_to=self._provider_params.period_to,
                                           timeframe=self._provider_params.timeframe)
            self._df_hist = self._provider.load_option_history(self._option_symbol, params=opt_params,
                                                               columns=self._opt_columns)
            self._df_hist.dropna(subset=[OCl.PRICE.nm], inplace=True)
        return self._df_hist

    @df_hist.setter
    def df_hist(self, df: pd.DataFrame):
        """Option dataframe setter"""
        self._df_hist = df

    @property
    def df_fut(self) -> pd.DataFrame:
        """Future dataframe getter"""
        if self._df_fut is None:
            fut_params = RequestParameters(period_from=self._provider_params.period_from,
                                           period_to=self._provider_params.period_to,
                                           timeframe=self._provider_params.timeframe)
            self._df_fut = self._provider.load_future_history(self._option_symbol, params=fut_params,
                                                              columns=self._fut_columns)
        return self._df_fut

    @df_fut.setter
    def df_fut(self, df: pd.DataFrame):
        """Future dataframe setter"""
        self._df_fut = df

    def update_option_chain(self, settlement_date: datetime.datetime | None = None,
                            expiration_date: datetime.datetime | None = None) -> bool:
        """Update option chain by api request if it supported by provider"""
        df_chain = self._provider.load_option_chain(self.option_symbol, settlement_date, expiration_date)
        if df_chain is None:
            return False
        self._df_chain = df_chain
        return True

    @property
    def df_chain(self) -> pd.DataFrame:
        """Chain dataframe getter"""
        return self._df_chain

    @df_chain.setter
    def df_chain(self, df_chain):
        """Chain dataframe setter"""
        self._df_chain = df_chain
