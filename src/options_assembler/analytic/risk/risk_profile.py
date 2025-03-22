"""Option Risk Profile functions"""
import pandas as pd

from options_assembler.entities import OptionColumns as OCl, OptionType, LegType, OptionLeg
from options_assembler.analytic.risk._risk_entities import RiskColumns as RCl
from options_assembler.enrichment import add_intrinsic_and_time_value

def _get_premium(df_chain_type_opt: pd.DataFrame, strike: float, leg_type: LegType | None = None) -> float:
    if leg_type is None and OCl.OPTION_TYPE.nm not in df_chain_type_opt.columns:
        raise ValueError('Data frame should be with one option type or ser leg_type')
    if leg_type == LegType.FUTURE:
        raise ValueError('Future do not have premium')
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


def _calc_profile(df_opt_type: pd.DataFrame, leg: OptionLeg, premium: float) -> pd.DataFrame:
    """Calc P&L profile"""
    if leg.type == LegType.OPTION_CALL:
        if leg.lots > 0:
            df_opt_type.loc[:, RCl.RISK_PNL.nm] = df_opt_type[OCl.STRIKE.nm] - leg.strike - premium
            df_opt_type.loc[df_opt_type[OCl.STRIKE.nm] <= leg.strike, RCl.RISK_PNL.nm] = -premium
        else:
            df_opt_type.loc[:, RCl.RISK_PNL.nm] = premium - (df_opt_type.loc[:, OCl.STRIKE.nm] - leg.strike)
            df_opt_type.loc[df_opt_type[OCl.STRIKE.nm] <= leg.strike, RCl.RISK_PNL.nm] = premium
    else:
        if leg.lots > 0:
            df_opt_type.loc[:, RCl.RISK_PNL.nm] = leg.strike - df_opt_type[OCl.STRIKE.nm] - premium
            df_opt_type.loc[df_opt_type[OCl.STRIKE.nm] >= leg.strike, RCl.RISK_PNL.nm] = -premium
        else:
            df_opt_type.loc[:, RCl.RISK_PNL.nm] = premium - (leg.strike - df_opt_type.loc[:, OCl.STRIKE.nm])
            df_opt_type.loc[df_opt_type[OCl.STRIKE.nm] >= leg.strike, RCl.RISK_PNL.nm] = premium
    df_opt_type.loc[:, RCl.RISK_PNL.nm] *= abs(leg.lots)
    return df_opt_type


def _calc_premium_profile(df_opt_type: pd.DataFrame, leg: OptionLeg, premium: float) -> pd.DataFrame:
    """Calc premium P&L profile"""
    if OCl.INTRINSIC_VALUE.nm not in df_opt_type.columns:
        df_opt_type = add_intrinsic_and_time_value(df_opt_type)
    print('!', df_opt_type)
    if leg.type == LegType.OPTION_CALL:
        if leg.lots > 0:
            df_opt_type.loc[:, RCl.RISK_PNL_PREMIUM.nm] = df_opt_type[OCl.STRIKE.nm] - leg.strike + \
                                                          (df_opt_type[OCl.PRICE.nm]) - premium
            # df_opt_type.loc[df_opt_type[OCl.STRIKE.nm] <= leg.strike,
            # RCl.RISK_PNL_PREMIUM.nm] = df_opt_type[OCl.STRIKE.nm] - leg.strike + \
            #                            (df_opt_type[OCl.PRICE.nm]) - premium
            df_opt_type.loc[df_opt_type[RCl.RISK_PNL_PREMIUM.nm] < -premium, RCl.RISK_PNL_PREMIUM.nm] = -premium
            # df_opt_type.loc[:, RCl.RISK_PNL_PREMIUM.nm] = (df_opt_type[OCl.STRIKE.nm] + df_opt_type[
            #     OCl.PRICE.nm] - leg.strike - premium) * leg.lots
            # loss_strike_filter = df_opt_type[OCl.STRIKE.nm] <= leg.strike
            # df_opt_type.loc[loss_strike_filter, RCl.RISK_PNL_PREMIUM.nm] = (df_opt_type.loc[
            #                                                                     loss_strike_filter, OCl.PRICE.nm] -
            #                                                                 (leg.strike - df_opt_type.loc[
            #                                                                     loss_strike_filter, OCl.STRIKE.nm])
            #                                                                 - premium) * leg.lots
        else:
            df_opt_type.loc[:, RCl.RISK_PNL_PREMIUM.nm] = premium - (df_opt_type.loc[:, OCl.STRIKE.nm] - leg.strike)
            df_opt_type.loc[df_opt_type[OCl.STRIKE.nm] <= leg.strike, RCl.RISK_PNL_PREMIUM.nm] = premium


    else:
        df_opt_type.loc[:, RCl.RISK_PNL_PREMIUM.nm] = (leg.strike - df_opt_type[OCl.STRIKE.nm] + df_opt_type[
            OCl.PRICE.nm] - premium) * leg.lots
        loss_strike_filter = df_opt_type[OCl.STRIKE.nm] >= leg.strike
        df_opt_type.loc[loss_strike_filter, RCl.RISK_PNL_PREMIUM.nm] = (df_opt_type.loc[
                                                                            loss_strike_filter, OCl.PRICE.nm] -
                                                                        (df_opt_type.loc[
                                                                             loss_strike_filter, OCl.STRIKE.nm] - leg.strike)
                                                                        - premium) * leg.lots
        df_opt_type.loc[
            df_opt_type[RCl.RISK_PNL_PREMIUM.nm] < -premium * leg.lots, RCl.RISK_PNL_PREMIUM.nm] = -premium * leg.lots
    df_opt_type = _calc_profile(df_opt_type, leg, premium) # TODO remove
    print('#>', df_opt_type)
    df_opt_type.loc[:, RCl.RISK_PNL_PREMIUM.nm] *= abs(leg.lots)
    return df_opt_type


def _chain_leg_expiration_risk_profile(df_chain: pd.DataFrame, leg: OptionLeg) -> pd.DataFrame:
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
        df = _calc_profile(df, leg, premium)
        df = _calc_premium_profile(df, leg, premium)
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
        .agg({RCl.RISK_PNL.nm: 'sum', RCl.RISK_PNL_PREMIUM.nm: 'sum'}) \
        .reset_index(drop=False)
    return df_risk_profile, df_legs_risk_profile
