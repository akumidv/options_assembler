"""Option Risk Profile functions"""

import pandas as pd
from pandera.typing import DataFrame

from alphavar.core.dictionary import ResultTerm
from alphavar.options.dictionary import LegType, OptionsTerm, OptionsType
from alphavar.options.entities import OptionsLeg
from alphavar.options.schemas import PayoffCurveSchema, PayoffLegsSchema


def _get_premium(df_chain_type_opt: pd.DataFrame, strike: float, leg_type: LegType | None = None) -> float:
    if leg_type is None and OptionsTerm.OPTION_RIGHT not in df_chain_type_opt.columns:
        raise ValueError("Data frame should be with one option type or ser leg_type")
    if leg_type == LegType.FUTURES:
        raise ValueError("Future do not have premium")
    if leg_type is not None:
        df_chain_type_opt = df_chain_type_opt[df_chain_type_opt[OptionsTerm.OPTION_RIGHT] == leg_type.value]
    premium_df = df_chain_type_opt[df_chain_type_opt[OptionsTerm.STRIKE] == strike]
    if premium_df.empty:
        del premium_df
        type_code = (
            OptionsType.CALL.value
            if df_chain_type_opt.iloc[0][OptionsTerm.OPTION_RIGHT] == OptionsType.CALL.value
            else OptionsType.PUT.value
        )
        raise ValueError(f"Data for strike {strike} for and option type {type_code} absent")
    premium = premium_df.iloc[0][OptionsTerm.PRICE]
    del premium_df
    return premium


def _calc_profile(df_opt_type: pd.DataFrame, leg: OptionsLeg, premium: float) -> pd.DataFrame:
    """Calc P&L profile"""
    if leg.type == LegType.OPTIONS_CALL:
        if leg.lots > 0:
            df_opt_type.loc[:, ResultTerm.RISK_PNL] = df_opt_type[OptionsTerm.STRIKE] - leg.strike - premium
            df_opt_type.loc[df_opt_type[OptionsTerm.STRIKE] <= leg.strike, ResultTerm.RISK_PNL] = -premium
        else:
            df_opt_type.loc[:, ResultTerm.RISK_PNL] = premium - (df_opt_type.loc[:, OptionsTerm.STRIKE] - leg.strike)
            df_opt_type.loc[df_opt_type[OptionsTerm.STRIKE] <= leg.strike, ResultTerm.RISK_PNL] = premium
    else:
        if leg.lots > 0:
            df_opt_type.loc[:, ResultTerm.RISK_PNL] = leg.strike - df_opt_type[OptionsTerm.STRIKE] - premium
            df_opt_type.loc[df_opt_type[OptionsTerm.STRIKE] >= leg.strike, ResultTerm.RISK_PNL] = -premium
        else:
            df_opt_type.loc[:, ResultTerm.RISK_PNL] = premium - (leg.strike - df_opt_type.loc[:, OptionsTerm.STRIKE])
            df_opt_type.loc[df_opt_type[OptionsTerm.STRIKE] >= leg.strike, ResultTerm.RISK_PNL] = premium
    df_opt_type.loc[:, ResultTerm.RISK_PNL] *= abs(leg.lots)
    return df_opt_type


def _calc_premium_profile(df_opt_type: pd.DataFrame, leg: OptionsLeg, premium: float) -> pd.DataFrame:
    """Calc the mark-to-market ("today") P&L profile next to the expiration profile.

    ``RISK_PNL`` (from :func:`_calc_profile`) is the payoff at expiration: intrinsic
    value at each strike net of the premium paid. ``RISK_PNL_PREMIUM`` is the current
    P&L if the underlying were at each strike level *now*, valuing the leg with that
    strike's current option price instead of pure intrinsic value — the "today" line on
    a risk graph. For the long side the loss is bounded by the premium at risk; the
    short side mirrors it (max gain = premium received).
    """
    df_opt_type = _calc_profile(df_opt_type, leg, premium)
    if leg.type == LegType.OPTIONS_CALL:
        intrinsic_shift = df_opt_type[OptionsTerm.STRIKE] - leg.strike
    else:
        intrinsic_shift = leg.strike - df_opt_type[OptionsTerm.STRIKE]
    # Per-lot current P&L: intrinsic shift plus the option's current price at that
    # strike, net of premium paid, capped at the premium at risk (long-side max loss).
    pnl_premium = (intrinsic_shift + df_opt_type[OptionsTerm.PRICE] - premium).clip(lower=-premium)
    if leg.lots < 0:  # short: mirror the long profile (max gain = premium received)
        pnl_premium = -pnl_premium
    df_opt_type.loc[:, ResultTerm.RISK_PNL_PREMIUM] = pnl_premium * abs(leg.lots)
    return df_opt_type


# ─────────────────────────────────────────────────────────────────────────────────────
# 4VERIFY (owner): the mark-to-market math in `_calc_premium_profile`
# above is NOT yet verified by the owner — per DEVELOPMENT_REQUIREMENTS D2 all DataFrame
# / math implementations must be explained and explicitly verified by the owner before
# being treated as final. The original pre-2026-06-14 implementation (raised
# NotImplementedError, with several unfinished attempts) is preserved verbatim below for
# that review. `add_intrinsic_and_time_value` is no longer imported — restore the import
# if this body is reinstated.
#
# def _calc_premium_profile(df_opt_type: pd.DataFrame, leg: OptionsLeg, premium: float) -> pd.DataFrame:
#     """Calc premium P&L profile"""
#     raise NotImplementedError
#     if OptionsTerm.INTRINSIC_VALUE not in df_opt_type.columns:
#         df_opt_type = add_intrinsic_and_time_value(df_opt_type)
#     if leg.type == LegType.OPTIONS_CALL:
#         if leg.lots > 0:
#             df_opt_type.loc[:, RCl.RISK_PNL_PREMIUM.nm] = df_opt_type[OptionsTerm.STRIKE] - leg.strike + \
#                                                           (df_opt_type[OptionsTerm.PRICE]) - premium
#             # df_opt_type.loc[df_opt_type[OptionsTerm.STRIKE] <= leg.strike,
#             # RCl.RISK_PNL_PREMIUM.nm] = df_opt_type[OptionsTerm.STRIKE] - leg.strike + \
#             #                            (df_opt_type[OptionsTerm.PRICE]) - premium
#             df_opt_type.loc[df_opt_type[RCl.RISK_PNL_PREMIUM.nm] < -premium, RCl.RISK_PNL_PREMIUM.nm] = -premium
#             # df_opt_type.loc[:, RCl.RISK_PNL_PREMIUM.nm] = (df_opt_type[OptionsTerm.STRIKE] + df_opt_type[
#             #     OptionsTerm.PRICE] - leg.strike - premium) * leg.lots
#             # loss_strike_filter = df_opt_type[OptionsTerm.STRIKE] <= leg.strike
#             # df_opt_type.loc[loss_strike_filter, RCl.RISK_PNL_PREMIUM.nm] = (df_opt_type.loc[
#             #                                                                     loss_strike_filter, OptionsTerm.PRICE] -
#             #                                                                 (leg.strike - df_opt_type.loc[
#             #                                                                     loss_strike_filter, OptionsTerm.STRIKE])
#             #                                                                 - premium) * leg.lots
#         else:
#             df_opt_type.loc[:, RCl.RISK_PNL_PREMIUM.nm] = premium - (df_opt_type.loc[:, OptionsTerm.STRIKE] - leg.strike)
#             df_opt_type.loc[df_opt_type[OptionsTerm.STRIKE] <= leg.strike, RCl.RISK_PNL_PREMIUM.nm] = premium
#
#
#     else:
#         df_opt_type.loc[:, RCl.RISK_PNL_PREMIUM.nm] = (leg.strike - df_opt_type[OptionsTerm.STRIKE] + df_opt_type[
#             OptionsTerm.PRICE] - premium) * leg.lots
#         loss_strike_filter = df_opt_type[OptionsTerm.STRIKE] >= leg.strike
#         df_opt_type.loc[loss_strike_filter, RCl.RISK_PNL_PREMIUM.nm] = (df_opt_type.loc[
#                                                                             loss_strike_filter, OptionsTerm.PRICE] -
#                                                                         (df_opt_type.loc[
#                                                                              loss_strike_filter, OptionsTerm.STRIKE] - leg.strike)
#                                                                         - premium) * leg.lots
#         df_opt_type.loc[
#             df_opt_type[RCl.RISK_PNL_PREMIUM.nm] < -premium * leg.lots, RCl.RISK_PNL_PREMIUM.nm] = -premium * leg.lots
#     df_opt_type = _calc_profile(df_opt_type, leg, premium) # TODO remove
#     df_opt_type.loc[:, RCl.RISK_PNL_PREMIUM.nm] *= abs(leg.lots)
#     return df_opt_type
# ─────────────────────────────────────────────────────────────────────────────────────


def _chain_leg_expiration_risk_profile(df_chain: pd.DataFrame, leg: OptionsLeg) -> pd.DataFrame:
    """Calc PNL Risk profile for leg"""
    type_code = OptionsType.PUT.value if leg.type == LegType.OPTIONS_PUT else OptionsType.CALL.value
    df = df_chain[df_chain[OptionsTerm.OPTION_RIGHT] == type_code].copy()
    if leg.type == LegType.FUTURES:
        df.loc[:, ResultTerm.RISK_PNL] = (df[OptionsTerm.STRIKE] - df[OptionsTerm.UNDERLYING_PRICE]) * leg.lots
        df.loc[:, ResultTerm.RISK_PNL_PREMIUM] = df[ResultTerm.RISK_PNL]
    else:
        premium_df = df[df[OptionsTerm.STRIKE] == leg.strike]
        if premium_df.empty:
            raise ValueError(f"Data for strike {leg.strike} for and option type {leg.type.value} absent")
        premium = premium_df.iloc[0][OptionsTerm.PRICE]
        df = _calc_premium_profile(df, leg, premium)
    df.drop(
        columns=[
            col
            for col in df.columns
            if col not in [OptionsTerm.STRIKE, ResultTerm.RISK_PNL, ResultTerm.RISK_PNL_PREMIUM]
        ],
        inplace=True,
    )
    return df


def payoff_legs(df_chain: pd.DataFrame, legs: list[OptionsLeg]) -> DataFrame[PayoffLegsSchema]:
    """The per-leg payoff breakdown of a strategy over a chain — one row per ``(leg_id, strike)``.

    Each leg's expiration P&L (``risk_pnl``) and mark-to-market P&L (``risk_pnl_premium``) at every
    strike, tagged with ``leg_id``. Base producer for ``payoff_curve`` (which sums this over legs).
    Risk-graph background: https://www.investopedia.com/trading/options-risk-graphs/.
    """
    legs_dfs = []
    for idx, leg in enumerate(legs):
        df_leg = _chain_leg_expiration_risk_profile(df_chain, leg)
        df_leg.loc[:, ResultTerm.LEG_ID] = f"#{idx}_{leg.type.value}_{leg.strike}_{leg.lots}"
        legs_dfs.append(df_leg)
    if len(legs_dfs) == 0:
        raise ValueError(f"Can not prepared risk profile for {len(legs)} legs number")
    df_legs_risk_profile = pd.concat(legs_dfs, axis="rows", ignore_index=True) if len(legs_dfs) > 1 else legs_dfs[0]
    df_legs_risk_profile.sort_values(by=[OptionsTerm.STRIKE, ResultTerm.LEG_ID], inplace=True)
    return df_legs_risk_profile


def payoff_curve(df_legs_risk_profile: pd.DataFrame) -> DataFrame[PayoffCurveSchema]:
    """The combined strategy payoff curve — the per-strike P&L of the whole position.

    Consumes a :func:`payoff_legs` frame and sums each strike's ``risk_pnl`` / ``risk_pnl_premium``
    across legs (``payoff_curve ← payoff_legs``).
    """
    return (
        df_legs_risk_profile.groupby(OptionsTerm.STRIKE, group_keys=False)[
            [ResultTerm.RISK_PNL, ResultTerm.RISK_PNL_PREMIUM]
        ]
        .agg({ResultTerm.RISK_PNL: "sum", ResultTerm.RISK_PNL_PREMIUM: "sum"})
        .reset_index(drop=False)
    )
    # 4VERIFY (owner): aggregation sums RISK_PNL_PREMIUM alongside RISK_PNL (2026-06-14). Original
    # RISK_PNL-only aggregation preserved: `.groupby(STRIKE)[[RISK_PNL]].agg({RISK_PNL: 'sum'})`.


def chain_payoff(df_chain: pd.DataFrame, legs: list[OptionsLeg]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Options payoff (risk profile) as ``(combined curve, per-leg breakdown)``.

    Convenience over the two producers: ``payoff_curve(payoff_legs(...))`` plus the legs frame.
    Example profiles https://www.optionstaxguy.com/risk-profiles.
    """
    df_legs_risk_profile = payoff_legs(df_chain, legs)
    df_risk_profile = payoff_curve(df_legs_risk_profile)
    return df_risk_profile, df_legs_risk_profile
