"""Option Risk Profile functions"""
import pandas as pd

from option_lib.entities import OptionColumns as OCl, OptionType, LegType, OptionLeg
from option_lib.analytic.risk._risk_entities import RiskColumns as RCl


def _get_premium(df_chain_type_opt: pd.DataFrame, strike: float, leg_type: LegType | None = None) -> float:
    if leg_type is None and OCl.OPTION_TYPE.nm not in df_chain_type_opt.columns:
        raise ValueError(f'Data frame should be with one option type or ser leg_type')
    if leg_type == LegType.FUTURE:
        raise ValueError(f'Future do not have premium')
    if leg_type is not None:
        df_chain_type_opt = df_chain_type_opt[df_chain_type_opt[OCl.OPTION_TYPE.nm] == leg_type.code]
    premium_df = df_chain_type_opt[df_chain_type_opt[OCl.STRIKE.nm] == strike]
    if premium_df.empty:
        del premium_df
        type_code = OptionType.CALL.value if df_chain_type_opt.iloc[0][OCl.OPTION_TYPE.nm] == OptionType.CALL.code else \
            OptionType.PUT.value
        raise ValueError(f'Data for strike {strike} for and option type {type_code} absent')
    premium = premium_df.iloc[0][OCl.PRICE.nm]
    del premium_df
    return premium


def _chain_leg_expiration_risk_profile(df_chain, leg: OptionLeg):
    """Calc PNL Risk profile for leg"""
    type_code = OptionType.PUT.code if leg.type == LegType.OPTION_PUT else OptionType.CALL.code
    df = df_chain[df_chain[OCl.OPTION_TYPE.nm] == type_code].copy()
    if leg.type == LegType.FUTURE:
        df.loc[:, RCl.RISK_PNL.nm] = (df[OCl.STRIKE.nm] - df[OCl.UNDERLYING_PRICE.nm]) * leg.lots
        df.loc[:, RCl.RISK_PNL_PREMIUM.nm] = df[RCl.RISK_PNL.nm]
    else:
        premium_df = df[df[OCl.STRIKE.nm] == leg.strike]
        if premium_df.empty:
            raise ValueError(f'Data for strike {leg.strike} for and option type {leg.type.value} absent')
        premium = premium_df.iloc[0][OCl.PRICE.nm]
        if leg.type == LegType.OPTION_CALL:
            df.loc[:, RCl.RISK_PNL.nm] = (df[OCl.STRIKE.nm] - leg.strike - premium) * leg.lots
            df.loc[:, RCl.RISK_PNL_PREMIUM.nm] = (df[OCl.STRIKE.nm] + df[
                OCl.PRICE.nm] - leg.strike - premium) * leg.lots
            loss_strike_filter = df[OCl.STRIKE.nm] <= leg.strike
            # df.loc[loss_strike_filter, RCl.RISK_PNL.nm] = -premium * leg.lots
            # df.loc[loss_strike_filter, RCl.RISK_PNL_PREMIUM.nm] = df[OCl.PRICE.nm] - premium * leg.lots
            df.loc[loss_strike_filter, RCl.RISK_PNL_PREMIUM.nm] = (df.loc[loss_strike_filter, OCl.PRICE.nm] -
                                                                   (leg.strike - df.loc[loss_strike_filter, OCl.STRIKE.nm])
                                                                   - premium) * leg.lots
        else:
            df.loc[:, RCl.RISK_PNL.nm] = (leg.strike - df[OCl.STRIKE.nm] - premium) * leg.lots
            df.loc[:, RCl.RISK_PNL_PREMIUM.nm] = (leg.strike - df[OCl.STRIKE.nm] + df[
                OCl.PRICE.nm] - premium) * leg.lots
            loss_strike_filter = df[OCl.STRIKE.nm] >= leg.strike
            # df.loc[df[OCl.STRIKE.nm] >= leg.strike, RCl.RISK_PNL.nm] = -premium * leg.lots
            # df.loc[df[OCl.STRIKE.nm] >= leg.strike, RCl.RISK_PNL_PREMIUM.nm] = df[OCl.PRICE.nm] - premium * leg.lots
            df.loc[loss_strike_filter, RCl.RISK_PNL_PREMIUM.nm] = (df.loc[loss_strike_filter, OCl.PRICE.nm] -
                                                                   (df.loc[loss_strike_filter, OCl.STRIKE.nm] - leg.strike)
                                                                   - premium) * leg.lots
        df.loc[loss_strike_filter, RCl.RISK_PNL.nm] = -premium * leg.lots
        df.loc[df[RCl.RISK_PNL_PREMIUM.nm] < -premium * leg.lots, RCl.RISK_PNL_PREMIUM.nm] = -premium * leg.lots

    df.drop(columns=[col for col in df.columns if col not in [OCl.STRIKE.nm, RCl.RISK_PNL.nm,
                                                                                      RCl.RISK_PNL_PREMIUM.nm]],
                                    inplace=True)
    return df


def chain_pnl_risk_profile(df_chain: pd.DataFrame,
                           legs: list[OptionLeg]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Example of profiles https://www.optionstaxguy.com/risk-profiles
    Explanation https://www.investopedia.com/trading/options-risk-graphs/

    Option risk PNL Profile on expiration date and for current
    Index is Strike values
    """

    legs_dfs = []
    for idx, leg in enumerate(legs):
        df_leg = _chain_leg_expiration_risk_profile(df_chain, leg)
        df_leg.loc[:, RCl.LEG_ID.nm] = f'#{idx}_{leg.type.value}_{leg.strike}_{leg.lots}'
        legs_dfs.append(df_leg)
    if len(legs_dfs) == 0:
        raise ValueError(f'Can not prepared risk profile for {len(legs)} legs number')
    df_legs_risk_profile = pd.concat(legs_dfs, axis='rows', ignore_index=True) if len(legs_dfs) > 1 else legs_dfs[0]
    df_legs_risk_profile.sort_values(by=[OCl.STRIKE.nm, RCl.LEG_ID.nm], inplace=True)
    df_risk_profile = df_legs_risk_profile.groupby(OCl.STRIKE.nm, group_keys=False)[
        [RCl.RISK_PNL.nm, RCl.RISK_PNL_PREMIUM.nm]] \
        .agg({RCl.RISK_PNL.nm: 'sum', RCl.RISK_PNL_PREMIUM.nm: 'sum'})\
        .reset_index(drop=False)
    return df_risk_profile, df_legs_risk_profile
