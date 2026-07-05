import os

import matplotlib
import pandas as pd
import pytest

from alphavar.core.dictionary import ResultTerm
from alphavar.options.dictionary import LegType, OptionsTerm, OptionsType
from alphavar.options.entities import OptionsLeg
from alphavar.options.lib.analytic.risk import payoff

matplotlib.use("inline")
import matplotlib.pyplot as plt

mock_strikes = [100, 200, 300, 400, 500, 600, 700]
underlying_price = 385


@pytest.fixture(name="mock_df_op_chain")
def mock_df_op_chain_fixture():
    mock_df_op_chain = pd.DataFrame(
        {
            f"{OptionsTerm.OPTION_RIGHT}": [OptionsType.CALL.value] * 7 + [OptionsType.PUT.value] * 7,
            f"{OptionsTerm.STRIKE}": mock_strikes + mock_strikes,
            f"{OptionsTerm.PRICE}": [303, 204, 105, 10, 6, 5, 4] + [7, 8, 9, 35, 119, 218, 317],
            f"{OptionsTerm.UNDERLYING_PRICE}": [underlying_price] * 14,
        }
    )
    return mock_df_op_chain


mock_call_leg = OptionsLeg(strike=300, lots=1, type=LegType.OPTIONS_CALL)
mock_put_leg = OptionsLeg(strike=300, lots=1, type=LegType.OPTIONS_PUT)
mock_fut_leg = OptionsLeg(strike=0, lots=10, type=LegType.FUTURES)


def _show_df(df, name, out_dir):
    print("\n")
    print(df)
    df.plot(x="strike", y="risk_pnl", title=name)
    plt.savefig(os.path.join(out_dir, f"{name}.svg"))


def test_mock__get_premium(mock_df_op_chain):
    idx = 3
    strike = mock_strikes[idx]
    premium = payoff._get_premium(
        mock_df_op_chain[mock_df_op_chain[OptionsTerm.OPTION_RIGHT] == OptionsType.CALL.value], strike=strike
    )
    assert premium == mock_df_op_chain.iloc[idx][OptionsTerm.PRICE]


@pytest.mark.parametrize("strike", mock_strikes[1:-2])
def test__calc_profile_long_call(mock_df_op_chain, strike, tmp_output_dir):
    lots = 2
    leg = OptionsLeg(strike=strike, lots=lots, type=LegType.OPTIONS_CALL)
    premium = payoff._get_premium(mock_df_op_chain, strike=leg.strike, leg_type=leg.type)
    df_opt_type = mock_df_op_chain[mock_df_op_chain[OptionsTerm.OPTION_RIGHT] == leg.type.value]
    df_payoff = payoff._calc_profile(df_opt_type, leg, premium)
    payed_premium = -premium * leg.lots
    assert df_payoff[ResultTerm.RISK_PNL].min() == payed_premium
    df_payoff_less_strike_price = df_payoff[df_payoff[OptionsTerm.STRIKE] <= leg.strike]
    assert df_payoff_less_strike_price[df_payoff_less_strike_price[ResultTerm.RISK_PNL] > 0].empty
    df_payoff_greater_strike_price = df_payoff[df_payoff[OptionsTerm.STRIKE] > leg.strike]
    assert df_payoff_greater_strike_price[df_payoff_greater_strike_price[ResultTerm.RISK_PNL] == premium].empty
    strike_sell = mock_strikes[-2]
    row_buy = mock_df_op_chain[
        (mock_df_op_chain[OptionsTerm.STRIKE] == leg.strike)
        & (mock_df_op_chain[OptionsTerm.OPTION_RIGHT] == leg.type.value)
    ].iloc[0]
    expected_profit_call = max(-premium, (strike_sell - leg.strike - row_buy[OptionsTerm.PRICE])) * leg.lots
    assert df_payoff[df_payoff[OptionsTerm.STRIKE] == strike_sell].iloc[0][ResultTerm.RISK_PNL] == expected_profit_call
    _show_df(df_payoff, f"{__name__}_long_call_{strike}_lots_{lots}", tmp_output_dir)


@pytest.mark.parametrize("strike", mock_strikes[1:-2])
def test__calc_profile_short_call(mock_df_op_chain, strike, tmp_output_dir):
    lots = -2
    leg = OptionsLeg(strike=strike, lots=lots, type=LegType.OPTIONS_CALL)
    premium = payoff._get_premium(mock_df_op_chain, strike=leg.strike, leg_type=leg.type)
    df_opt_type = mock_df_op_chain[mock_df_op_chain[OptionsTerm.OPTION_RIGHT] == leg.type.value]
    df_payoff = payoff._calc_profile(df_opt_type, leg, premium)
    earned_premium = premium * abs(leg.lots)
    assert df_payoff[ResultTerm.RISK_PNL].max() == earned_premium
    df_payoff_less_strike_price = df_payoff[df_payoff[OptionsTerm.STRIKE] > leg.strike]
    assert df_payoff_less_strike_price[ResultTerm.RISK_PNL].max() < earned_premium
    _show_df(df_payoff, f"{__name__}_short_call_{strike}_lots_{lots}", tmp_output_dir)


@pytest.mark.parametrize("strike", mock_strikes[1:-2])
def test__calc_profile_long_put(mock_df_op_chain, strike, tmp_output_dir):
    lots = 2
    leg = OptionsLeg(strike=strike, lots=lots, type=LegType.OPTIONS_PUT)
    premium = payoff._get_premium(mock_df_op_chain, strike=leg.strike, leg_type=leg.type)
    df_opt_type = mock_df_op_chain[mock_df_op_chain[OptionsTerm.OPTION_RIGHT] == leg.type.value]
    df_payoff = payoff._calc_profile(df_opt_type, leg, premium)
    payed_premium = -premium * leg.lots
    assert df_payoff[ResultTerm.RISK_PNL].min() == payed_premium
    df_payoff_less_strike_price = df_payoff[df_payoff[OptionsTerm.STRIKE] >= leg.strike]
    assert df_payoff_less_strike_price[df_payoff_less_strike_price[ResultTerm.RISK_PNL] > 0].empty
    df_payoff_less_strike_price = df_payoff[df_payoff[OptionsTerm.STRIKE] < leg.strike]
    assert df_payoff_less_strike_price[df_payoff_less_strike_price[ResultTerm.RISK_PNL] == premium].empty
    strike_sell = mock_strikes[-2]
    row_buy = mock_df_op_chain[
        (mock_df_op_chain[OptionsTerm.STRIKE] == leg.strike)
        & (mock_df_op_chain[OptionsTerm.OPTION_RIGHT] == leg.type.value)
    ].iloc[0]
    expected_profit_call = max(-premium, (leg.strike - strike_sell - row_buy[OptionsTerm.PRICE])) * leg.lots
    assert df_payoff[df_payoff[OptionsTerm.STRIKE] == strike_sell].iloc[0][ResultTerm.RISK_PNL] == expected_profit_call
    _show_df(df_payoff, f"{__name__}_long_put_{strike}_lots_{lots}", tmp_output_dir)


@pytest.mark.parametrize("strike", mock_strikes[1:-2])
def test__calc_profile_short_put(mock_df_op_chain, strike, tmp_output_dir):
    lots = -2
    leg = OptionsLeg(strike=strike, lots=lots, type=LegType.OPTIONS_PUT)
    premium = payoff._get_premium(mock_df_op_chain, strike=leg.strike, leg_type=leg.type)
    df_opt_type = mock_df_op_chain[mock_df_op_chain[OptionsTerm.OPTION_RIGHT] == leg.type.value]
    df_payoff = payoff._calc_profile(df_opt_type, leg, premium)
    earned_premium = premium * abs(leg.lots)
    assert df_payoff[ResultTerm.RISK_PNL].max() == earned_premium
    df_payoff_less_strike_price = df_payoff[df_payoff[OptionsTerm.STRIKE] < leg.strike]
    assert df_payoff_less_strike_price[ResultTerm.RISK_PNL].max() < earned_premium
    _show_df(df_payoff, f"{__name__}_short_put_{strike}_lots_{lots}", tmp_output_dir)


@pytest.mark.parametrize("strike", mock_strikes[1:2])
def test__calc_premium_profile_long_call(mock_df_op_chain, strike, tmp_output_dir):
    lots = 2
    leg = OptionsLeg(strike=strike, lots=lots, type=LegType.OPTIONS_CALL)
    premium = payoff._get_premium(mock_df_op_chain, strike=leg.strike, leg_type=leg.type)
    df_opt_type = mock_df_op_chain[mock_df_op_chain[OptionsTerm.OPTION_RIGHT] == leg.type.value]
    df_payoff = payoff._calc_premium_profile(df_opt_type, leg, premium)
    payed_premium = -premium * leg.lots
    assert df_payoff[ResultTerm.RISK_PNL].min() == payed_premium
    df_payoff_less_strike_price = df_payoff[df_payoff[OptionsTerm.STRIKE] <= leg.strike]
    assert df_payoff_less_strike_price[df_payoff_less_strike_price[ResultTerm.RISK_PNL] > 0].empty
    df_payoff_greater_strike_price = df_payoff[df_payoff[OptionsTerm.STRIKE] > leg.strike]
    assert df_payoff_greater_strike_price[df_payoff_greater_strike_price[ResultTerm.RISK_PNL] == premium].empty
    strike_sell = mock_strikes[-2]
    row_buy = mock_df_op_chain[
        (mock_df_op_chain[OptionsTerm.STRIKE] == leg.strike)
        & (mock_df_op_chain[OptionsTerm.OPTION_RIGHT] == leg.type.value)
    ].iloc[0]
    expected_profit_call = max(-premium, (strike_sell - leg.strike - row_buy[OptionsTerm.PRICE])) * leg.lots
    assert df_payoff[df_payoff[OptionsTerm.STRIKE] == strike_sell].iloc[0][ResultTerm.RISK_PNL] == expected_profit_call
    _show_df(df_payoff, f"{__name__}_premium_long_call_{strike}_lots_{lots}", tmp_output_dir)


@pytest.mark.parametrize("strike", mock_strikes[1:-2])
def test_mock__chain_leg_payoff_call_itm(mock_df_op_chain, strike):
    leg = OptionsLeg(strike=strike, lots=1, type=LegType.OPTIONS_CALL)
    df_payoff = payoff._chain_leg_expiration_risk_profile(mock_df_op_chain, leg)
    premium = payoff._get_premium(mock_df_op_chain, strike=leg.strike, leg_type=leg.type) * leg.lots * -1
    df_payoff_less_fut_price = df_payoff[df_payoff[OptionsTerm.STRIKE] <= leg.strike]
    assert df_payoff_less_fut_price[df_payoff_less_fut_price[ResultTerm.RISK_PNL] != premium].empty
    df_payoff_greater_fut_price = df_payoff[df_payoff[OptionsTerm.STRIKE] > leg.strike]
    assert df_payoff_greater_fut_price[df_payoff_greater_fut_price[ResultTerm.RISK_PNL] == premium].empty

    strike_sell = mock_strikes[-2]
    row_buy = mock_df_op_chain[
        (mock_df_op_chain[OptionsTerm.STRIKE] == leg.strike)
        & (mock_df_op_chain[OptionsTerm.OPTION_RIGHT] == leg.type.value)
    ].iloc[0]
    expected_profit_call = max(premium, (strike_sell - leg.strike - row_buy[OptionsTerm.PRICE]) * leg.lots)

    assert df_payoff[df_payoff[OptionsTerm.STRIKE] == strike_sell].iloc[0][ResultTerm.RISK_PNL] == expected_profit_call


@pytest.mark.parametrize("strike", mock_strikes[1:-2])
def test_mock__chain_leg_payoff_put_itm(mock_df_op_chain, strike):
    leg = OptionsLeg(strike=strike, lots=1, type=LegType.OPTIONS_PUT)
    df_payoff = payoff._chain_leg_expiration_risk_profile(mock_df_op_chain, leg)

    premium = (
        payoff._get_premium(
            mock_df_op_chain[mock_df_op_chain[OptionsTerm.OPTION_RIGHT] == leg.type.value], strike=leg.strike
        )
        * leg.lots
        * -1
    )
    df_payoff_greater_fut_price = df_payoff[df_payoff[OptionsTerm.STRIKE] >= leg.strike]
    assert df_payoff_greater_fut_price[df_payoff_greater_fut_price[ResultTerm.RISK_PNL] != premium].empty
    df_payoff_less_fut_price = df_payoff[df_payoff[OptionsTerm.STRIKE] < leg.strike]
    assert df_payoff_less_fut_price[df_payoff_less_fut_price[ResultTerm.RISK_PNL] == premium].empty

    strike_sell = mock_strikes[2]
    row_buy = mock_df_op_chain[
        (mock_df_op_chain[OptionsTerm.STRIKE] == leg.strike)
        & (mock_df_op_chain[OptionsTerm.OPTION_RIGHT] == leg.type.value)
    ].iloc[0]
    expected_profit_put = max(premium, (leg.strike - strike_sell - row_buy[OptionsTerm.PRICE]) * leg.lots)
    assert df_payoff[df_payoff[OptionsTerm.STRIKE] == strike_sell].iloc[0][ResultTerm.RISK_PNL] == expected_profit_put


def test_mock__chain_leg_payoff_future(mock_df_op_chain):
    df_payoff = payoff._chain_leg_expiration_risk_profile(mock_df_op_chain, mock_fut_leg)
    strike_sell = mock_strikes[-1]
    row_buy = mock_df_op_chain[
        (mock_df_op_chain[OptionsTerm.STRIKE] == strike_sell)
        & (mock_df_op_chain[OptionsTerm.OPTION_RIGHT] == OptionsType.CALL.value)
    ].iloc[0]
    expected_profit_fut = (strike_sell - row_buy[OptionsTerm.UNDERLYING_PRICE]) * mock_fut_leg.lots
    assert df_payoff[df_payoff[OptionsTerm.STRIKE] == strike_sell].iloc[0][ResultTerm.RISK_PNL] == expected_profit_fut


def test_chain_leg_pnl_risk_profile(df_chain, structure_long_call):
    leg = structure_long_call[0]
    df_payoff = payoff._chain_leg_expiration_risk_profile(df_chain, leg)
    premium = df_chain[df_chain[OptionsTerm.STRIKE] == leg.strike].iloc[0][OptionsTerm.PRICE]
    assert df_payoff[ResultTerm.RISK_PNL].min() >= -premium * leg.lots


def test_chain_pnl_risk_profile_long_call(df_chain, structure_long_call):
    df_payoff, df_legs_payoff = payoff.chain_payoff(df_chain, structure_long_call)
    leg = structure_long_call[0]
    premium = df_chain[df_chain[OptionsTerm.STRIKE] == leg.strike].iloc[0][OptionsTerm.PRICE]
    assert df_payoff[ResultTerm.RISK_PNL].min() >= -premium * leg.lots
    assert ResultTerm.RISK_PNL_PREMIUM in df_payoff
    assert df_legs_payoff[ResultTerm.RISK_PNL].min() >= -premium * leg.lots
    assert ResultTerm.RISK_PNL_PREMIUM in df_legs_payoff.columns


def test_chain_pnl_risk_profile_structure_long_straddle(df_chain, structure_long_straddle):
    assert structure_long_straddle[0].strike == structure_long_straddle[1].strike
    assert len(structure_long_straddle) == 2
    df_payoff, df_legs_payoff = payoff.chain_payoff(df_chain, structure_long_straddle)
    assert len(df_payoff.drop_duplicates(subset=[OptionsTerm.STRIKE])) == len(df_payoff)
    legs_ids = list(df_legs_payoff[ResultTerm.LEG_ID].unique())
    assert len(legs_ids) == len(structure_long_straddle)
    straddle_strike = structure_long_straddle[0].strike

    df_leg1 = df_legs_payoff[df_legs_payoff[ResultTerm.LEG_ID] == legs_ids[0]][
        [OptionsTerm.STRIKE, ResultTerm.RISK_PNL]
    ].set_index(keys=OptionsTerm.STRIKE)
    df_leg2 = df_legs_payoff[df_legs_payoff[ResultTerm.LEG_ID] == legs_ids[1]][
        [OptionsTerm.STRIKE, ResultTerm.RISK_PNL]
    ].set_index(keys=OptionsTerm.STRIKE)
    df_legs = df_leg1.join(df_leg2, lsuffix=legs_ids[0], rsuffix=legs_ids[0], how="left")
    df = df_legs.join(df_payoff.set_index(OptionsTerm.STRIKE), how="left").reset_index(drop=False)

    premium_payed = (
        -1
        * df_chain[df_chain[OptionsTerm.STRIKE] == straddle_strike][OptionsTerm.PRICE].sum()
        * (structure_long_straddle[0].lots + structure_long_straddle[1].lots)
        / 2
    )
    max_pnl_lose_sum = df_legs_payoff.groupby(ResultTerm.LEG_ID)[ResultTerm.RISK_PNL].agg("min").sum()
    assert round(premium_payed, 5) == round(max_pnl_lose_sum, 5)
    pnl_min = df_payoff[ResultTerm.RISK_PNL].min()
    pnl_legs_strike = df[df[OptionsTerm.STRIKE] == straddle_strike].iloc[0]
    assert round(pnl_min, 5) >= round(premium_payed, 5)
    assert pnl_min == pnl_legs_strike[ResultTerm.RISK_PNL]
