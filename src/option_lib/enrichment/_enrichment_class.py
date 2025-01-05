"""Public api class that should hide realization of functions"""
from typing import Self
import pandas as pd

from option_lib.entities import OptionColumns as Ocl
from option_lib.option_data_class import OptionData
from option_lib.enrichment._option_with_future import (
    join_option_with_future
)

from option_lib.enrichment.price import (
    add_intrinsic_and_time_value, add_atm_itm_otm_by_chain
)


class OptionEnrichment:
    """
    Wrapper about enrichment module function
    methods
      - with 'get' - return new columns or option dataframe
      - with 'add' - add columns to option dataframe and return enrichment instance itself to use in chain
    """

    def __init__(self, data: OptionData):
        self._data = data

    @property
    def df_hist(self):
        """Getter to option dataframe"""
        return self._data.df_hist

    @df_hist.setter
    def df_hist(self, df_hist):
        """Setter to option dataframe"""
        self._data.df_hist = df_hist

    def get_joint_option_with_future(self) -> pd.DataFrame:
        """Return new option data with correspondent future columns"""
        if Ocl.FUTURES_PRICE.nm in self._data.df_hist.columns:
            return self._data.df_hist
        return join_option_with_future(self._data.df_hist, self._data.df_fut)

    def add_future(self) -> Self:
        """Update option data with correspondent future columns"""
        if 'future_price' in self._data.df_hist.columns:
            return self._data.df_hist
        self._data.df_hist = self.get_joint_option_with_future()
        return self

    def add_intrinsic_and_time_value(self) -> Self:
        """Add columns with intrinsic and time value, also join future if data is not enough"""
        if Ocl.INTRINSIC_VALUE.nm in self._data.df_hist.columns:
            return self
        if Ocl.FUTURES_PRICE.nm not in self._data.df_hist.columns:
            self.add_future()
        self._data.df_hist = add_intrinsic_and_time_value(self._data.df_hist)
        return self

    def add_atm_itm_otm(self) -> Self:
        """Add column with ATM, OTM, ITM values"""
        if Ocl.FUTURES_PRICE.nm not in self._data.df_hist.columns:
            self.add_future()
        if Ocl.PRICE_STATUS.nm in self._data.df_hist.columns:
            return self
        self._data.df_hist = add_atm_itm_otm_by_chain(self._data.df_hist)
        return self
