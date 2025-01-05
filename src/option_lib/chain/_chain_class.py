""""Public api class that should hide realization of functions"""
import datetime
from typing import Self
import pandas as pd

from option_lib.entities import OptionColumns as OCl
from option_lib.option_data_class import OptionData

from option_lib.chain.chain_selector import validate_chain, select_chain, get_chain_settlement_and_expiration_date
from option_lib.chain.price_status import (get_chain_atm_itm_otm, get_chain_atm_nearest_strikes, get_chain_atm_strike)
from option_lib.chain.desk import convert_chain_to_desk


class OptionChain:
    """
    Wrapper about chain module function
    methods
      - with 'get' - return new columns or option dataframe
      - with 'add' - add columns to option dataframe and return enrichment instance itself to use in chain
    """

    def __init__(self, data: OptionData):
        self._data = data

    @property
    def df_chain(self) -> pd.DataFrame:
        """Getter for chain dataframe. If not selected - it return chain for nearest actual expiration date"""
        if self._data.df_chain is None:
            return self.select_chain()
        return self._data.df_chain

    @df_chain.setter
    def df_chain(self, df_chain):
        """Setter for chain dataframe"""
        self.validate_chain(df_chain)
        self._data.df_chain = df_chain

    @staticmethod
    def validate_chain(df_chain: pd.DataFrame):
        """Validate dataframe is it option chain"""
        validate_chain(df_chain)

    def select_chain(self, settlement_date: datetime.date | None = None,
                     expiation_date: datetime.date | None = None) -> pd.DataFrame:
        """Select for chain dataframe. If parameters do not set - it return chain for nearest actual expiration date
        TODO from exchange provider current df_chain can be get directly from exchange api, so have sense to move get
        form provider if impossible - get from history
        """
        is_updated = self._data.update_option_chain(settlement_date, expiation_date)
        if is_updated:
            return self._data.df_chain

        self._data.df_chain = select_chain(self._data.df_hist, settlement_date, expiation_date)
        return self._data.df_chain

    def add_atm_itm_otm(self) -> Self:
        """Add column with ATM/ITM/OTM"""
        self._data.df_chain[OCl.PRICE_STATUS.nm] = get_chain_atm_itm_otm(self._data.df_chain)
        return self

    def get_atm_nearest_strikes(self) -> list:
        """Get strikes sorted as nearest to atm (for showing desk for example)"""
        return get_chain_atm_nearest_strikes(self._data.df_chain)

    def get_atm_strike(self) -> float:
        """Get strikes sorted as nearest to atm (for showing desk for example)"""
        return get_chain_atm_strike(self._data.df_chain)

    def get_settlement_and_expiration_date(self) -> tuple[datetime.date, datetime.date]:
        """Get settlement and expiration dates"""
        return get_chain_settlement_and_expiration_date(self._data.df_chain)

    def get_desk(self, option_columns: list | None = None, future_columns: list | None = None) -> pd.DataFrame:
        """Prepare desk from chain where call and put merged by strike"""
        return convert_chain_to_desk(self._data.df_chain, option_columns, future_columns)
