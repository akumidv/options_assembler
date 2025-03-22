""""Public risk analytic api class that should hide realization of functions"""

import pandas as pd

from options_assembler.option_data_class import OptionData
from options_assembler.analytic.risk.risk_profile import chain_pnl_risk_profile
from options_assembler.entities import OptionLeg


class OptionAnalyticRisk:
    """
    Wrapper about risk analytics modules functions
    """

    def __init__(self, data: OptionData):
        self._data = data

    def chain_risk_profile(self, legs: list[OptionLeg]) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Calculate option risk profile"""
        return chain_pnl_risk_profile(self._data.df_chain, legs)
