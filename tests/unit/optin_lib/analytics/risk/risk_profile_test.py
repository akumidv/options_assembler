import pytest
import pandas as pd
import numpy as np
from options_assembler.entities import OptionColumns as OCl, OptionType, LegType, OptionLeg
from options_assembler.analytic.risk import risk_profile
from options_assembler.analytic.risk import RiskColumns as RCl

mock_strikes = [100, 200, 300, 400, 500, 600, 700]


@pytest.fixture(name='mock_df_op_chain')
def mock_df_op_chain_fixture():
    mock_df_op_chain = pd.DataFrame({
        f'{OCl.OPTION_TYPE.nm}': [OptionType.CALL.code] * 7 + [OptionType.PUT.code] * 7,
        f'{OCl.STRIKE.nm}': mock_strikes + mock_strikes,
        f'{OCl.PRICE.nm}': [303, 204, 105, 10, 6, 5, 4] + [7, 8, 9, 35, 119, 218, 317],
        f'{OCl.UNDERLYING_PRICE.nm}': [385] * 14
    })
    return mock_df_op_chain


mock_call_leg = OptionLeg(strike=300, lots=1, type=LegType.OPTION_CALL)
mock_put_leg = OptionLeg(strike=300, lots=1, type=LegType.OPTION_PUT)
mock_fut_leg = OptionLeg(strike=0, lots=10, type=LegType.FUTURE)


def test_mock__get_premium(mock_df_op_chain):
    premium = risk_profile._get_premium(mock_df_op_chain[mock_df_op_chain[OCl.OPTION_TYPE.nm] == OptionType.CALL.code],
                                        strike=400)
    assert premium == mock_df_op_chain.iloc[3][OCl.PRICE.nm]


@pytest.mark.parametrize("strike", mock_strikes[1:-2])
def test__calc_profile_long_call(mock_df_op_chain, strike):
    leg = OptionLeg(strike=strike, lots=2, type=LegType.OPTION_CALL)
    premium = risk_profile._get_premium(mock_df_op_chain, strike=leg.strike, leg_type=leg.type)
    df_opt_type = mock_df_op_chain[mock_df_op_chain[OCl.OPTION_TYPE.nm] == leg.type.code]
    df_risk_prof = risk_profile._calc_profile(df_opt_type, leg, premium)
    payed_premium = -premium * leg.lots
    assert df_risk_prof[RCl.RISK_PNL.nm].min() == payed_premium
    df_risk_prof_less_strike_price = df_risk_prof[df_risk_prof[OCl.STRIKE.nm] <= leg.strike]
    assert df_risk_prof_less_strike_price[df_risk_prof_less_strike_price[RCl.RISK_PNL.nm] > 0].empty
    df_risk_prof_greater_strike_price = df_risk_prof[df_risk_prof[OCl.STRIKE.nm] > leg.strike]
    assert df_risk_prof_greater_strike_price[df_risk_prof_greater_strike_price[RCl.RISK_PNL.nm] == premium].empty
    strike_sell = mock_strikes[-2]
    row_buy = mock_df_op_chain[(mock_df_op_chain[OCl.STRIKE.nm] == leg.strike) &
                               (mock_df_op_chain[OCl.OPTION_TYPE.nm] == leg.type.code)].iloc[0]
    expected_profit_call = max(-premium,
                               (strike_sell - leg.strike - row_buy[OCl.PRICE.nm])) * leg.lots
    assert df_risk_prof[df_risk_prof[OCl.STRIKE.nm] == strike_sell].iloc[0][RCl.RISK_PNL.nm] == expected_profit_call


@pytest.mark.parametrize("strike", mock_strikes[1:-2])
def test__calc_profile_short_call(mock_df_op_chain, strike):
    leg = OptionLeg(strike=strike, lots=-2, type=LegType.OPTION_CALL)
    premium = risk_profile._get_premium(mock_df_op_chain, strike=leg.strike, leg_type=leg.type)
    df_opt_type = mock_df_op_chain[mock_df_op_chain[OCl.OPTION_TYPE.nm] == leg.type.code]
    df_risk_prof = risk_profile._calc_profile(df_opt_type, leg, premium)
    earned_premium = premium * abs(leg.lots)
    assert df_risk_prof[RCl.RISK_PNL.nm].max() == earned_premium
    df_risk_prof_less_strike_price = df_risk_prof[df_risk_prof[OCl.STRIKE.nm] > leg.strike]
    assert df_risk_prof_less_strike_price[RCl.RISK_PNL.nm].max() < earned_premium


@pytest.mark.parametrize("strike", mock_strikes[1:-2])
def test__calc_profile_long_put(mock_df_op_chain, strike):
    leg = OptionLeg(strike=strike, lots=2, type=LegType.OPTION_PUT)
    premium = risk_profile._get_premium(mock_df_op_chain, strike=leg.strike, leg_type=leg.type)
    df_opt_type = mock_df_op_chain[mock_df_op_chain[OCl.OPTION_TYPE.nm] == leg.type.code]
    df_risk_prof = risk_profile._calc_profile(df_opt_type, leg, premium)
    payed_premium = -premium * leg.lots
    assert df_risk_prof[RCl.RISK_PNL.nm].min() == payed_premium
    df_risk_prof_less_strike_price = df_risk_prof[df_risk_prof[OCl.STRIKE.nm] >= leg.strike]
    assert df_risk_prof_less_strike_price[df_risk_prof_less_strike_price[RCl.RISK_PNL.nm] > 0].empty
    df_risk_prof_less_strike_price = df_risk_prof[df_risk_prof[OCl.STRIKE.nm] < leg.strike]
    assert df_risk_prof_less_strike_price[df_risk_prof_less_strike_price[RCl.RISK_PNL.nm] == premium].empty
    strike_sell = mock_strikes[-2]
    row_buy = mock_df_op_chain[(mock_df_op_chain[OCl.STRIKE.nm] == leg.strike) &
                               (mock_df_op_chain[OCl.OPTION_TYPE.nm] == leg.type.code)].iloc[0]
    expected_profit_call = max(-premium,
                               (leg.strike - strike_sell - row_buy[OCl.PRICE.nm])) * leg.lots
    assert df_risk_prof[df_risk_prof[OCl.STRIKE.nm] == strike_sell].iloc[0][RCl.RISK_PNL.nm] == expected_profit_call


@pytest.mark.parametrize("strike", mock_strikes[1:-2])
def test__calc_profile_short_put(mock_df_op_chain, strike):
    leg = OptionLeg(strike=strike, lots=-2, type=LegType.OPTION_PUT)
    premium = risk_profile._get_premium(mock_df_op_chain, strike=leg.strike, leg_type=leg.type)
    df_opt_type = mock_df_op_chain[mock_df_op_chain[OCl.OPTION_TYPE.nm] == leg.type.code]
    df_risk_prof = risk_profile._calc_profile(df_opt_type, leg, premium)
    earned_premium = premium * abs(leg.lots)
    assert df_risk_prof[RCl.RISK_PNL.nm].max() == earned_premium
    df_risk_prof_less_strike_price = df_risk_prof[df_risk_prof[OCl.STRIKE.nm] < leg.strike]
    assert df_risk_prof_less_strike_price[RCl.RISK_PNL.nm].max() < earned_premium


@pytest.mark.parametrize("strike", mock_strikes[1:2])
def test__calc_premium_profile_long_call(mock_df_op_chain, strike):
    leg = OptionLeg(strike=strike, lots=2, type=LegType.OPTION_CALL)
    premium = risk_profile._get_premium(mock_df_op_chain, strike=leg.strike, leg_type=leg.type)
    df_opt_type = mock_df_op_chain[mock_df_op_chain[OCl.OPTION_TYPE.nm] == leg.type.code]
    df_risk_prof = risk_profile._calc_premium_profile(df_opt_type, leg, premium)
    payed_premium = -premium * leg.lots
    assert df_risk_prof[RCl.RISK_PNL.nm].min() == payed_premium
    df_risk_prof_less_strike_price = df_risk_prof[df_risk_prof[OCl.STRIKE.nm] <= leg.strike]
    assert df_risk_prof_less_strike_price[df_risk_prof_less_strike_price[RCl.RISK_PNL.nm] > 0].empty
    df_risk_prof_greater_strike_price = df_risk_prof[df_risk_prof[OCl.STRIKE.nm] > leg.strike]
    assert df_risk_prof_greater_strike_price[df_risk_prof_greater_strike_price[RCl.RISK_PNL.nm] == premium].empty
    strike_sell = mock_strikes[-2]
    row_buy = mock_df_op_chain[(mock_df_op_chain[OCl.STRIKE.nm] == leg.strike) &
                               (mock_df_op_chain[OCl.OPTION_TYPE.nm] == leg.type.code)].iloc[0]
    expected_profit_call = max(-premium,
                               (strike_sell - leg.strike - row_buy[OCl.PRICE.nm])) * leg.lots
    assert df_risk_prof[df_risk_prof[OCl.STRIKE.nm] == strike_sell].iloc[0][RCl.RISK_PNL.nm] == expected_profit_call



@pytest.mark.parametrize("strike", mock_strikes[1:-2])
def test_mock__chain_leg_pnl_risk_profile_call_itm(mock_df_op_chain, strike):
    leg = OptionLeg(strike=strike, lots=1, type=LegType.OPTION_CALL)
    df_risk_prof = risk_profile._chain_leg_expiration_risk_profile(mock_df_op_chain, leg)
    premium = risk_profile._get_premium(mock_df_op_chain, strike=leg.strike, leg_type=leg.type) * leg.lots * -1
    df_risk_prof_less_fut_price = df_risk_prof[df_risk_prof[OCl.STRIKE.nm] <= leg.strike]
    assert df_risk_prof_less_fut_price[df_risk_prof_less_fut_price[RCl.RISK_PNL.nm] != premium].empty
    df_risk_prof_greater_fut_price = df_risk_prof[df_risk_prof[OCl.STRIKE.nm] > leg.strike]
    assert df_risk_prof_greater_fut_price[df_risk_prof_greater_fut_price[RCl.RISK_PNL.nm] == premium].empty

    strike_sell = mock_strikes[-2]
    row_buy = mock_df_op_chain[(mock_df_op_chain[OCl.STRIKE.nm] == leg.strike) &
                               (mock_df_op_chain[OCl.OPTION_TYPE.nm] == leg.type.code)].iloc[0]
    expected_profit_call = max(premium,
                               (strike_sell - leg.strike - row_buy[OCl.PRICE.nm]) * leg.lots)

    assert df_risk_prof[df_risk_prof[OCl.STRIKE.nm] == strike_sell].iloc[0][RCl.RISK_PNL.nm] == expected_profit_call


@pytest.mark.parametrize("strike", mock_strikes[1:-2])
def test_mock__chain_leg_pnl_risk_profile_put_itm(mock_df_op_chain, strike):
    leg = OptionLeg(strike=strike, lots=1, type=LegType.OPTION_PUT)
    df_risk_prof = risk_profile._chain_leg_expiration_risk_profile(mock_df_op_chain, leg)

    premium = risk_profile._get_premium(mock_df_op_chain[mock_df_op_chain[OCl.OPTION_TYPE.nm] == leg.type.code],
                                        strike=leg.strike) * leg.lots * -1
    df_risk_prof_greater_fut_price = df_risk_prof[df_risk_prof[OCl.STRIKE.nm] >= leg.strike]
    assert df_risk_prof_greater_fut_price[df_risk_prof_greater_fut_price[RCl.RISK_PNL.nm] != premium].empty
    df_risk_prof_less_fut_price = df_risk_prof[df_risk_prof[OCl.STRIKE.nm] < leg.strike]
    assert df_risk_prof_less_fut_price[df_risk_prof_less_fut_price[RCl.RISK_PNL.nm] == premium].empty

    strike_sell = mock_strikes[2]
    row_buy = mock_df_op_chain[(mock_df_op_chain[OCl.STRIKE.nm] == leg.strike) &
                               (mock_df_op_chain[OCl.OPTION_TYPE.nm] == leg.type.code)].iloc[0]
    expected_profit_put = max(premium,
                              (leg.strike - strike_sell - row_buy[OCl.PRICE.nm]) * leg.lots)
    assert df_risk_prof[df_risk_prof[OCl.STRIKE.nm] == strike_sell].iloc[0][RCl.RISK_PNL.nm] == expected_profit_put


def test_mock__chain_leg_pnl_risk_profile_future(mock_df_op_chain):
    df_risk_prof = risk_profile._chain_leg_expiration_risk_profile(mock_df_op_chain, mock_fut_leg)
    strike_sell = mock_strikes[-1]
    row_buy = mock_df_op_chain[(mock_df_op_chain[OCl.STRIKE.nm] == strike_sell) &
                               (mock_df_op_chain[OCl.OPTION_TYPE.nm] == OptionType.CALL.code)].iloc[0]
    expected_profit_fut = (strike_sell - row_buy[OCl.UNDERLYING_PRICE.nm]) * mock_fut_leg.lots
    assert df_risk_prof[df_risk_prof[OCl.STRIKE.nm] == strike_sell].iloc[0][RCl.RISK_PNL.nm] == expected_profit_fut


def test_chain_leg_pnl_risk_profile(df_chain, structure_long_call):
    leg = structure_long_call[0]
    df_risk_prof = risk_profile._chain_leg_expiration_risk_profile(df_chain, leg)
    premium = df_chain[df_chain[OCl.STRIKE.nm] == leg.strike].iloc[0][OCl.PRICE.nm]
    assert df_risk_prof[RCl.RISK_PNL.nm].min() >= -premium * leg.lots


def test_chain_pnl_risk_profile_long_call(df_chain, structure_long_call):
    df_risk_profile, df_legs_risk_profile = risk_profile.chain_pnl_risk_profile(df_chain, structure_long_call)
    leg = structure_long_call[0]
    premium = df_chain[df_chain[OCl.STRIKE.nm] == leg.strike].iloc[0][OCl.PRICE.nm]
    assert df_risk_profile[RCl.RISK_PNL.nm].min() >= -premium * leg.lots
    assert RCl.RISK_PNL_PREMIUM.nm in df_risk_profile
    assert df_legs_risk_profile[RCl.RISK_PNL.nm].min() >= -premium * leg.lots
    assert RCl.RISK_PNL_PREMIUM.nm in df_legs_risk_profile.columns


def test_chain_pnl_risk_profile_structure_long_straddle(df_chain, structure_long_straddle):
    assert structure_long_straddle[0].strike == structure_long_straddle[1].strike
    assert len(structure_long_straddle) == 2
    df_risk_profile, df_legs_risk_profile = risk_profile.chain_pnl_risk_profile(df_chain, structure_long_straddle)
    assert len(df_risk_profile.drop_duplicates(subset=[OCl.STRIKE.nm])) == len(df_risk_profile)
    legs_ids = list(df_legs_risk_profile[RCl.LEG_ID.nm].unique())
    assert len(legs_ids) == len(structure_long_straddle)
    straddle_strike = structure_long_straddle[0].strike

    df_leg1 = df_legs_risk_profile[df_legs_risk_profile[RCl.LEG_ID.nm] == legs_ids[0]][
        [OCl.STRIKE.nm, RCl.RISK_PNL.nm]].set_index(keys=OCl.STRIKE.nm)
    df_leg2 = df_legs_risk_profile[df_legs_risk_profile[RCl.LEG_ID.nm] == legs_ids[1]][
        [OCl.STRIKE.nm, RCl.RISK_PNL.nm]].set_index(keys=OCl.STRIKE.nm)
    df_legs = df_leg1.join(df_leg2, lsuffix=legs_ids[0], rsuffix=legs_ids[0], how='left')
    df = df_legs.join(df_risk_profile.set_index(OCl.STRIKE.nm), how='left').reset_index(drop=False)

    premium_payed = -1 * df_chain[df_chain[OCl.STRIKE.nm] == straddle_strike][OCl.PRICE.nm].sum() * (
        structure_long_straddle[0].lots + structure_long_straddle[1].lots) / 2
    max_pnl_lose_sum = df_legs_risk_profile.groupby(RCl.LEG_ID.nm)[RCl.RISK_PNL.nm].agg('min').sum()
    assert round(premium_payed, 5) == round(max_pnl_lose_sum, 5)
    pnl_min = df_risk_profile[RCl.RISK_PNL.nm].min()
    pnl_legs_strike = df[df[OCl.STRIKE.nm] == straddle_strike].iloc[0]
    assert round(pnl_min, 5) >= round(premium_payed, 5)
    assert pnl_min == pnl_legs_strike[RCl.RISK_PNL.nm]
