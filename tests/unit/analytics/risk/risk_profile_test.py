import pytest
import pandas as pd
from option_lib.entities import OptionColumns as OCl, OptionType, LegType, OptionLeg
from option_lib.analytic.risk.risk_profile import chain_pnl_risk_profile, _chain_leg_risk_profile, _get_premium
from option_lib.analytic.risk import RiskColumns as RCl

mock_df_brn_chain = pd.DataFrame({
    f'{OCl.OPTION_TYPE.nm}': [OptionType.CALL.code] * 7 + [OptionType.PUT.code] * 7,
    f'{OCl.STRIKE.nm}': [100, 200, 300, 400, 500, 600, 700] + [100, 200, 300, 400, 500, 600, 700],
    f'{OCl.PRICE.nm}': [7, 8, 9, 35, 119, 218, 317] + [303, 204, 105, 10, 6, 5, 4],
    f'{OCl.UNDERLYING_PRICE.nm}': [385] * 14
})

mock_call_leg = OptionLeg(strike=300, lots=1, type=LegType.OPTION_CALL)
mock_put_leg = OptionLeg(strike=300, lots=1, type=LegType.OPTION_PUT)
mock_fut_leg = OptionLeg(strike=0, lots=10, type=LegType.FUTURE)


def test_mock__get_premium():
    premium = _get_premium(mock_df_brn_chain[mock_df_brn_chain[OCl.OPTION_TYPE.nm] == OptionType.CALL.code], strike=400)
    assert premium == mock_df_brn_chain.iloc[3][OCl.PRICE.nm]


def test_mock__chain_leg_pnl_risk_profile_call_itm():
    leg = mock_call_leg
    df_risk_prof = _chain_leg_risk_profile(mock_df_brn_chain, leg)
    premium = _get_premium(mock_df_brn_chain[mock_df_brn_chain[OCl.OPTION_TYPE.nm] == leg.type.code],
                           strike=leg.strike) * leg.lots * -1
    fut_price = mock_df_brn_chain.iloc[0][OCl.UNDERLYING_PRICE.nm]
    df_risk_prof_less_fut_price = df_risk_prof[df_risk_prof[OCl.STRIKE.nm] <= fut_price]
    assert df_risk_prof_less_fut_price[df_risk_prof_less_fut_price[RCl.RISK_PNL.nm] != premium].empty
    df_risk_prof_greater_fut_price = df_risk_prof[df_risk_prof[OCl.STRIKE.nm] > fut_price]
    assert df_risk_prof_greater_fut_price[df_risk_prof_greater_fut_price[RCl.RISK_PNL.nm] == premium].empty

    strike_sell = 600
    row_buy = mock_df_brn_chain[(mock_df_brn_chain[OCl.STRIKE.nm] == leg.strike) &
                                (mock_df_brn_chain[OCl.OPTION_TYPE.nm] == leg.type.code)].iloc[0]
    expected_profit_call = max(premium,
                               (strike_sell - row_buy[OCl.UNDERLYING_PRICE.nm] - row_buy[OCl.PRICE.nm]) * leg.lots)
    assert df_risk_prof[df_risk_prof[OCl.STRIKE.nm] == strike_sell].iloc[0][RCl.RISK_PNL.nm] == expected_profit_call


def test_mock__chain_leg_pnl_risk_profile_put_itm():
    leg = mock_put_leg
    df_risk_prof = _chain_leg_risk_profile(mock_df_brn_chain, leg)

    premium = _get_premium(mock_df_brn_chain[mock_df_brn_chain[OCl.OPTION_TYPE.nm] == leg.type.code],
                           strike=leg.strike) * leg.lots * -1
    fut_price = mock_df_brn_chain.iloc[0][OCl.UNDERLYING_PRICE.nm]
    df_risk_prof_greater_fut_price = df_risk_prof[df_risk_prof[OCl.STRIKE.nm] >= fut_price]
    assert df_risk_prof_greater_fut_price[df_risk_prof_greater_fut_price[RCl.RISK_PNL.nm] != premium].empty
    df_risk_prof_less_fut_price = df_risk_prof[df_risk_prof[OCl.STRIKE.nm] < fut_price]
    assert df_risk_prof_less_fut_price[df_risk_prof_less_fut_price[RCl.RISK_PNL.nm] == premium].empty

    strike_sell = 400
    row_buy = mock_df_brn_chain[(mock_df_brn_chain[OCl.STRIKE.nm] == leg.strike) &
                                (mock_df_brn_chain[OCl.OPTION_TYPE.nm] == leg.type.code)].iloc[0]
    expected_profit_put = max(premium,
                              (row_buy[OCl.UNDERLYING_PRICE.nm] - strike_sell - row_buy[OCl.PRICE.nm]) * leg.lots)
    assert df_risk_prof[df_risk_prof[OCl.STRIKE.nm] == strike_sell].iloc[0][RCl.RISK_PNL.nm] == expected_profit_put


def test_mock__chain_leg_pnl_risk_profile_fut():
    df_risk_prof = _chain_leg_risk_profile(mock_df_brn_chain, mock_fut_leg)
    strike_sell = 700
    row_buy = mock_df_brn_chain[(mock_df_brn_chain[OCl.STRIKE.nm] == strike_sell) &
                                (mock_df_brn_chain[OCl.OPTION_TYPE.nm] == OptionType.CALL.code)].iloc[0]
    expected_profit_fut = (strike_sell - row_buy[OCl.UNDERLYING_PRICE.nm]) * mock_fut_leg.lots
    assert df_risk_prof[df_risk_prof[OCl.STRIKE.nm] == strike_sell].iloc[0][RCl.RISK_PNL.nm] == expected_profit_fut


def test_chain_leg_pnl_risk_profile(df_brn_chain, structure_long_call):
    leg = structure_long_call[0]
    df_risk_prof = _chain_leg_risk_profile(df_brn_chain, leg)
    premium = df_brn_chain[df_brn_chain[OCl.STRIKE.nm] == leg.strike].iloc[0][OCl.PRICE.nm]
    assert df_risk_prof[RCl.RISK_PNL.nm].min() >= -premium * leg.lots


def test_chain_pnl_risk_profile_long_call(df_brn_chain, structure_long_call):
    df_risk_profile, df_legs_risk_profile = chain_pnl_risk_profile(df_brn_chain, structure_long_call)
    leg = structure_long_call[0]
    premium = df_brn_chain[df_brn_chain[OCl.STRIKE.nm] == leg.strike].iloc[0][OCl.PRICE.nm]
    assert df_risk_profile[RCl.RISK_PNL.nm].min() >= -premium * leg.lots
    assert df_legs_risk_profile[RCl.RISK_PNL.nm].min() >= -premium * leg.lots


def test_chain_pnl_risk_profile_structure_long_straddle(df_brn_chain, structure_long_straddle):
    assert structure_long_straddle[0].strike == structure_long_straddle[1].strike
    assert len(structure_long_straddle) == 2
    df_risk_profile, df_legs_risk_profile = chain_pnl_risk_profile(df_brn_chain, structure_long_straddle)
    assert df_risk_profile.index.name == OCl.STRIKE.nm
    legs_ids = list(df_legs_risk_profile[RCl.LEG_ID.nm].unique())
    assert len(legs_ids) == len(structure_long_straddle)
    straddle_strike = structure_long_straddle[0].strike

    df_leg1 = df_legs_risk_profile[df_legs_risk_profile[RCl.LEG_ID.nm] == legs_ids[0]][
        [OCl.STRIKE.nm, RCl.RISK_PNL.nm]].set_index(keys=OCl.STRIKE.nm)
    df_leg2 = df_legs_risk_profile[df_legs_risk_profile[RCl.LEG_ID.nm] == legs_ids[1]][
        [OCl.STRIKE.nm, RCl.RISK_PNL.nm]].set_index(keys=OCl.STRIKE.nm)
    df_legs = df_leg1.join(df_leg2, lsuffix=legs_ids[0], rsuffix=legs_ids[0], how='left')
    df = df_legs.join(df_risk_profile, how='left')

    premium_payed = -1 * df_brn_chain[df_brn_chain[OCl.STRIKE.nm] == straddle_strike][OCl.PRICE.nm].sum() * (
        structure_long_straddle[0].lots + structure_long_straddle[1].lots) / 2
    max_pnl_lose_sum = df_legs_risk_profile.groupby(RCl.LEG_ID.nm)[RCl.RISK_PNL.nm].agg('min').sum()
    assert premium_payed == max_pnl_lose_sum
    pnl_min = df_risk_profile[RCl.RISK_PNL.nm].min()
    pnl_legs_strike = df.loc[straddle_strike]
    assert pnl_min >= premium_payed
    assert pnl_min == pnl_legs_strike[RCl.RISK_PNL.nm]
