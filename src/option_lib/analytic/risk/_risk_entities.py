"""Entities for risk analytics"""
from option_lib.entities import EnumDataFrameColumn


class RiskColumns(EnumDataFrameColumn):
    """Risk dataframes columns TODO move to analytic columns? or dataframe columns"""
    LEG_ID = 'leg_id', str
    RISK_PNL = 'risk_pnl',  float
    RISK_PNL_PREMIUM = 'risk_pnl_premium',  float
