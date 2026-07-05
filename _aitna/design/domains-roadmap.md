# Design — planned domains, capabilities & infrastructure roadmap

Forward-looking work, distinct from the refactoring/remediation backlog (largely done). Each
needs an owner scoping pass (and, where it touches R1/R2/R4 invariants or adds an entity, an
ADR) before code. The implementation backlog (`_aitna/TASKS.md`) carries a one-line entry per
item linking here. Math → **D2** (Type C) for every item.

## T28. Unify provider ↔ exchange data path (single normalization contract)

The *same* logical data (options/futures history, books) can arrive **live from an exchange** or
**stored via a file provider**; today both must yield a schema-identical DataFrame but the
normalization/column-mapping is split between the exchange layer (`RAW_SUFFIX`,
`INSTRUMENT_KIND_MAP`, `resolve_instrument_kind`, snapshot) and the file providers, with no
single place that pins "any source → one canonical frame". Consolidate that contract so callers
stay source-agnostic (the R1/R2 promise: *new source = new provider, no caller changes*).
**Touches binding invariants R1/R2 → requires an ADR** before code.

Plan:
1. Audit the two paths: what `AbstractExchange` normalizes vs what file providers assume
   already-normalized; list every column-mapping / dtype / tz / currency-raw (`_raw`) step and
   where it lives. Document the present `AbstractExchange(AbstractProvider)` relationship.
2. ADR: where the canonical-normalization boundary belongs (shared normalizer in `core`/`io`
   consumed by both source kinds vs. exchange-only) + the source taxonomy (`DataSource.LOCAL/
   S3/API` × engine) it implies.
3. Extract the shared normalization into one contract/module; exchanges and file providers both
   route through it; remove the duplicated/implicit mapping.
4. Characterization tests: a recorded exchange snapshot and a stored parquet of the same
   asset/period produce **identical** canonical frames (column set, dtypes, tz, ordering).

Architecture change → **D2 owner-verify** + ADR. Keep R2.1/R2.2 (identity & wire-format
translation stay at the exchange boundary) and R4 (columns only via dictionary enums) intact.
Relates to T37: the `load` producer is the P-data graph node; T28 changes its **internals**.

## T29. Surface **fitting** model (pricer-side, R5 — extends T21 smile fit)

Distinct from the forecast surface (which *forecasts* the surface): this *calibrates* a whole
vol **surface** to a market snapshot, not just independent per-slice smiles.

A `make_surface_model` in `options/lib/pricer/` (sibling of `make_smile_model`) that fits all
expirations **jointly** with a calendar-no-arbitrage coupling (total variance non-decreasing in
τ), e.g. an SVI-surface (SSVI / θ-interpolated raw-SVI) parametrization. Reuse the
constant-maturity total-variance interpolation already built for the forecast
(`forecast.surface._interpolate` — likely promote it to a shared `pricer` location). Output a
`SurfaceFit` yielding `iv(k, τ)` anywhere + butterfly (per slice) **and** calendar no-arb checks.
Facade: `OptionsPricer.fit_surface(...)` (vs the per-slice `fit_smile`). SSVI is the natural
first parametrization.

## T31. **Spot** domain (R4.3 / R4.5 — neutral-core reuse)

The core vocabulary/classification split (T26) already separated a neutral core (`Term`,
`InstrumentKind`, `AssetClass`) from the options extensions, so a spot domain can be added
without dragging in derivatives terms.

A `spot` capability area parallel to `options` — an `alphavar.spot` package (entities + lib + a
`Spot` facade over a `SpotData`) for cash instruments (no expiration / strike / greeks). Reuses:
the `Col` registry + neutral `Term`, the provider/exchange data path (R1/R2), reference-vs-series
(T25, asset-level `AssetMeta`), the forecast **price**/**vol** targets (no smile/surface). Plan:
define the spot entity/schema (identity + OHLC/price/volume, no contract layer), wire a provider,
add `SpotForecast` (price/vol only). New domain → confirm the neutral-core boundary holds (no
options leakage); ADR if it reshapes R4.

## T32. **Bonds** domain (R4.3 / R4.5)

A `bonds` capability area — `alphavar.bonds` (entities + lib + facade) for fixed-income
instruments: identity + coupon schedule, day-count, yield/price/duration/convexity, and a
**rates / yield-curve** layer (the curve is the bond analogue of the vol surface). Reuses the
neutral core (T26) + provider path (R1/R2) + reference-vs-series (T25, schedule/credit as
reference). New domain math (yield↔price, accrued interest, curve bootstrap/interpolation) → its
own pure lib (R5, pure-numpy per repo convention). Forecast targets: `price`/`yield`, and a
`curve` target analogous to `surface` (reuse the constant-maturity interpolation idea on the
yield curve). Needs an ADR (new asset class + rates entity); confirm shared vs bond-specific.

## T33. **Portfolio** management (cross-domain)

Position/portfolio aggregation, P&L, and **risk (VaR/CVaR)** fed by the forecast distributions
(T27) across instruments. Owner split:
- **Options portfolios** — likely **based in `options` itself** (the existing chain/leg/payoff
  machinery — `analytic/risk/payoff` — already models multi-leg structures; a portfolio is the
  natural extension of a strategy/desk). Build the options-portfolio layer where that lives.
- **General portfolio management** (for **bonds** + **spot**, and ultimately mixed books) — a
  **cross-domain** portfolio layer holding positions across asset classes, aggregating exposure,
  computing portfolio VaR/CVaR by combining per-instrument forecast distributions (cross-asset
  correlations). Likely a new top-level `alphavar.portfolio` (depends on the domain packages,
  not vice-versa).

Plan: start from the options side (reuse payoff/greeks aggregation), then lift the
asset-class-neutral pieces (position book, exposure roll-up, distributional VaR/CVaR) into the
shared portfolio layer as spot/bonds land. Depends on T27 (distributions) + T31/T32 (domains);
ADR for the cross-domain portfolio boundary.

## T35. **Risk** domain / layer (cross-domain, consumes forecasts)

A dedicated **risk** capability area — distinct from **portfolio** (T33, which holds positions /
aggregates exposure): risk *measures* the loss distribution. The existing
`options/lib/analytic/risk` (payoff / P&L profiles) is the seed; this lifts risk into a
first-class, asset-class-neutral layer that turns **forecast distributions (T27)** into risk
numbers.

A `risk` layer (likely `alphavar.risk`, neutral; or `options/lib/risk` first, then lifted)
computing, from a position/portfolio + per-instrument forecast distributions:
- **VaR / CVaR (expected shortfall)** at a confidence + horizon — analytic (from the lognormal /
  parametric terminal) **and** empirical (from MC / bootstrap scenarios, reusing
  `ForecastResult.scenarios` / `SmileForecast` / `SurfaceForecast` draws). Pure-numpy quantiles.
- **Scenario / stress** — revalue under shifted forecast inputs (spot ±, vol ±, smile/surface
  shift — composes with T30) and report the P&L distribution; greeks-based (delta/vega/gamma)
  **and** full-reval modes.
- **Aggregation** — combine instrument distributions with a **correlation / copula** assumption
  across assets; marginal vs component VaR.

Plan (sketch): `risk/_measures.py` (`var`, `cvar`, `expected_shortfall` over a distribution or a
scenario array), `risk/_scenario.py` (stress grid → reval via the pricer/payoff),
`risk/_aggregate.py` (correlated scenario combination); facade `OptionsRisk(OptionsData)` →
`Option.risk` for the single-instrument case, lifted to the portfolio layer (T33) for books.
Depends on T27; pairs with T33 — keep the **measure** (risk) and the **position book**
(portfolio) separate. ADR if it introduces a cross-asset correlation entity.

## T36. Implement the planned `knowledge/` concepts that have no code yet

`agents/shared/knowledge/` was lifted to the root `knowledge/` layer (akmon three-layer
model: knowledge → `src/` → USAGE `skills/`). Concepts whose code exists got a `knowledge/` leaf
+ a USAGE skill (chain, pricing/IV, smile, payoff, exchanges). The concepts below are **planned
but not yet coded**, so per the akmon rule ([akmon README §3b](../akmon/README.md))
they stay in `knowledge/` **with this task** and get **no** USAGE skill until the code lands.
For each: implement under `src/alphavar/...`, point the `knowledge/` leaf **down** to it, then
add a `concept → function` USAGE skill. **D2 owner-verify the math** for each.

- **Full Greeks** — `knowledge/options/pricing/greeks.md`. Only `bs_vega` exists; add delta
  `∂V/∂S`, gamma `∂²V/∂S²`, theta `∂V/∂t`, rho `∂V/∂r` (closed-form Black-76). Source: Hull.
- **Sortino ratio** — `knowledge/risk/ratios/sortino.md`. `(Rp − T)/DD`, downside deviation
  `DD = sqrt(mean(min(0, Rᵢ − T)²))`. Source: Sortino & Price (1994).

**VaR / CVaR** (`knowledge/risk/var/methods.md`, `cvar-expected-shortfall.md`) are **already
planned** under **T31/T32/T33/T35** (the risk / portfolio domain). Do not duplicate — when those
land, point those `knowledge/` leaves down to the new code and add the USAGE skills.

Until coded, mark each affected `knowledge/` leaf "**planned — not yet implemented (T36 /
T31–T33)**" so the knowledge↔impl chain stays honest.
