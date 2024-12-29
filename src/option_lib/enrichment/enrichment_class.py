"""Public api class that should hide realization of functions"""
from typing import Self
import pandas as pd

from option_lib.option_data_class import OptionData
from option_lib.enrichment.option_with_future import (
    join_option_with_future
)

# from option_lib.enrichment import _chain as chain
# from option_lib.enrichment._date import ()
from option_lib.enrichment.money import (
    add_intrinsic_and_time_value
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
    def df_opt(self):
        """Getter to option dataframe"""
        return self._data.df_opt

    def get_joint_option_with_future(self) -> pd.DataFrame:
        """Return new option data with correspondent future columns"""
        if 'future_price' in self._data.df_opt.columns:
            return self._data.df_opt
        return join_option_with_future(self._data.df_opt, self._data.df_fut)

    def add_future(self) -> Self:
        """Update option data with correspondent future columns"""
        if 'future_price' in self._data.df_opt.columns:
            return self._data.df_opt
        self._data.df_opt = join_option_with_future(self._data.df_opt, self._data.df_fut)
        return self

    def add_intrinsic_and_time_value(self) -> Self:
        """Add columns with intrinsic and time value, also join future if data is not enough"""
        if 'intrinsic_value' in self._data.df_opt.columns:
            return self
        if 'future_price' not in self._data.df_opt.columns:
            self.add_future()
        self._data.df_opt = add_intrinsic_and_time_value(self._data.df_opt)
        return self
