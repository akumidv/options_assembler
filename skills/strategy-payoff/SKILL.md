---
name: strategy-payoff
description: Compute an option strategy's payoff / P&L profile with alphavar — single-leg (long/short call/put) and multi-leg strategies (e.g. straddle) built from OptionsLeg and aggregated by OptionsAnalyticRisk.chain_payoff. Use when a user asks for a payoff diagram, P&L at expiration across underlying prices, or to model a strategy like a straddle.
when_to_use: Use when a user asks for a payoff diagram, P&L at expiration across underlying prices, or to model a strategy like a straddle.
owner: alphavar
---

# Compute strategy payoff / P&L in alphavar

USAGE skill (concept → function → how to apply). Verified against `src/` — re-confirm the
API before relying on it.

## Concept

A strategy's **payoff** is its P&L at expiration across underlying prices. Single legs
(premium = price paid/received), scaled by `abs(lots)`:

- **Long call** `max(0, S−K) − premium` · **Short call** `premium − max(0, S−K)`
- **Long put** `max(0, K−S) − premium` · **Short put** `premium − max(0, K−S)`

Multi-leg strategies combine legs — e.g. a **long straddle** = long call + long put at the
**same strike & expiry** (profits on a large move either way; max loss = total premium).

> Sources: Investopedia (risk graphs, straddle). In-repo `lib/analytic/risk/payoff.py`.

## Implementing functions

- **Leg** — `alphavar.options.entities.OptionsLeg` (pydantic): fields `strike: float`,
  `lots: int`, `type: LegType`.
- **Aggregate** — `alphavar.options.analytic_risk_class.OptionsAnalyticRisk.chain_payoff(legs)`
  → `tuple[pd.DataFrame, pd.DataFrame]` (payoff profile + premium profile), built from
  `OptionsData`.
- **Primitives** — `alphavar.options.lib.analytic.risk.payoff` (`_calc_profile`,
  `_calc_premium_profile`).

## How to apply

```python
from alphavar.options.analytic_risk_class import OptionsAnalyticRisk
from alphavar.options.entities import OptionsLeg
# LegType from the options entities/dictionary (call/put, long via sign of lots)

risk = OptionsAnalyticRisk(options_data)

# Long straddle: a call + a put at the SAME strike, both long (positive lots)
legs = [
    OptionsLeg(strike=100.0, lots=1, type=CALL),
    OptionsLeg(strike=100.0, lots=1, type=PUT),
]
payoff_df, premium_df = risk.chain_payoff(legs)
```

(Build other strategies the same way — different strikes/lots/types: short straddle =
negative lots; spreads = two legs at different strikes.)

## Failure modes

- The mark-to-market ("today") payoff variant uses current prices across strikes and is
  **owner-verification pending (D2)** — prefer the at-expiration profile until confirmed.
- Legs must reference strikes that exist in the chain data; missing strikes yield gaps.
- `lots` sign encodes long/short — a wrong sign silently flips the profile.
