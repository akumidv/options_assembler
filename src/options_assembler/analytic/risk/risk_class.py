""""Public risk analytic api class that should hide realization of functions"""

import pandas as pd

from option_lib.analytic.risk.payoff import chain_payoff
from option_lib.entities import OptionLeg
from options_assembler.option_data_class import OptionData


class OptionAnalyticRisk:
    """
    Wrapper about risk analytics modules functions
    """

    def __init__(self, data: OptionData):
        self._data = data

    def chain_payoff(self, legs: list[OptionLeg]) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Calculate option risk profile"""
        return chain_payoff(self._data.df_chain, legs)
