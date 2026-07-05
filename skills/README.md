# `skills/` — alphavar USAGE layer

How an AI assistant **uses alphavar** to solve a user's task. This is the **USAGE** layer
(see [_aitna/akmon/README.md](../_aitna/akmon/README.md) §2): it points *outward* —
built to travel into a downstream project that consumes alphavar, not to develop alphavar
itself (that is `_aitna/`).

alphavar's archetype is **`package`**, so a USAGE skill is a **domain-concept →
implementing-function map** (the usage end of the knowledge → impl → usage chain — see
[akmon README §3b](../_aitna/akmon/README.md) and
[ARCHETYPES.md](../_aitna/akmon/ARCHETYPES.md)). Not a bare API reference.

## The unit of USAGE: concept → function → how to apply

Each skill connects three things, all **verified against `src/`**:

1. **The domain concept** — what it is. For a rich concept (theory/sources), the full
   description lives in a [`../knowledge/`](../knowledge/) leaf and the skill links it; for a
   light concept, state it briefly here and in the function docstring (knowledge is
   optional, §3b).
2. **The implementing function** — the real public class/function in `alphavar.*` that
   computes it.
3. **How to apply it** — inputs, units/conventions, failure modes, a worked example.

A concept with **no** implementing function yet gets **no skill** — it is documented in
`../knowledge/` (if rich) with an impl task in [`../_aitna/TASKS.md`](../_aitna/TASKS.md),
or simply catalogued; add the skill only once the code exists.

## Map (domain concept → alphavar entry point)

> Verify each entry against `src/` before relying on it — the public surface drifts.

| Domain concept | alphavar entry point (verified) | Skill |
|---|---|---|
| An option (single contract) | `alphavar.Option` | — |
| Option chain (strikes × expiries) | `alphavar.options.chain_class.OptionsChain` | [option-chain](option-chain/SKILL.md) |
| Pricing & implied volatility (Black-76) | `alphavar.options.pricer_class.OptionsPricer` (`add_iv`/`add_price`/`get_iv`); `lib.pricer.black_scholes` (`bs_forward_price`, `bs_vega`, `implied_vol`) | [pricing-and-iv](pricing-and-iv/SKILL.md) |
| Volatility smile fit | `alphavar.options.lib.pricer.smile` — `SVISmile` / `SABRSmile` / `QuadraticSmile` (or `OptionsPricer.fit_smile`) | [fit-volatility-smile](fit-volatility-smile/SKILL.md) |
| Intrinsic / time value, moneyness | `lib.enrichment.price.add_intrinsic_and_time_value`; `lib.chain.price_status` | [option-chain](option-chain/SKILL.md) |
| Strategy payoff (single-leg, straddle, …) | `alphavar.options.analytic_risk_class.OptionsAnalyticRisk.chain_payoff(legs)` + `OptionsLeg` | [strategy-payoff](strategy-payoff/SKILL.md) |
| Exchange data (Deribit / Binance / MOEX) | `alphavar.io.exchange.*` — `DeribitExchange` / `BinanceExchange` / `MoexExchange` | [data-sources](data-sources/SKILL.md) |

### Planned concepts — documented in `knowledge/`, **no skill yet**

These are **planned but not yet coded**, so per the akmon rule they get **no USAGE
skill** until the function exists. They live as `../knowledge/` leaves (marked "planned")
with impl tasks:

| Concept | `knowledge/` leaf | Task | Status in `src/` |
|---|---|---|---|
| Full Greeks (delta/gamma/theta/rho) | `knowledge/options/pricing/greeks.md` | T36 | only `bs_vega` exists |
| VaR (historical / parametric / MC) | `knowledge/risk/var/methods.md` | T31–T33 | not implemented |
| CVaR / Expected Shortfall | `knowledge/risk/var/cvar-expected-shortfall.md` | T31–T33 | not implemented |
| Sortino ratio | `knowledge/risk/ratios/sortino.md` | T36 | not implemented |

(When one lands, add a `concept → function → how to apply` skill and move it up into the
map above.)

## Authoring a USAGE skill

A skill is `<name>/SKILL.md` (frontmatter + instruction), the cross-agent format
([akmon §7](../_aitna/akmon/README.md)). For a `package`, **no USAGE `tools/`** —
the skill calls the public API directly. Keep it: the concept (sourced), the exact
function (verified), inputs/units/failure modes, and one runnable example.
