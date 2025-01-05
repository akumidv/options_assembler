"""Entities for risk analytics"""
from option_lib.entities import EnumColumnType


class RiskColumns(EnumColumnType):
    """Risk dataframes columns"""
    LEG_ID = 'leg_id', str
    RISK_PNL = 'risk_pnl',  float
