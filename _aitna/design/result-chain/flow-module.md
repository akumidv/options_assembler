# Layer A — the `alphavar.flow` module (structure & chain-exchange)

> Hub: [`README.md`](README.md). This file = the **build-now** layer: the mechanical pieces and the
> I/O contract that makes entities compatible. Dead-ends → [`rejected-branches.md`](rejected-branches.md).

## A0. Module home & name — DECIDED

Root package in `alphavar/` (siblings: `core`, `io`, `options`; planned `spot`, `bonds`, `portfolio`,
`risk`) — the **cross-domain orchestration spine**, so not under `options`.

**Decision (owner, 2026-06-20): `alphavar.flow`.** Noun, idiomatic for a package, honest about the
DAG-dataflow, reads cleanly (`flow.Result`, `flow.resolve(...)`). `compose`/`composer` is kept for the
Layer B assembler entity *inside* `flow`, so both words work:

```
alphavar/flow/
  result.py  contract.py  registry.py  resolver.py  run_record.py   # Layer A — mechanics (forward)
  plan.py    composer.py                                            # Layer B — composer builds Plan (backward)
```

Reads as `composer.compose(subject) -> Plan` → `resolver.run(plan) -> Result`. Rejected names: `chain`
(taken — `options/chain_class.py` is the option chain/board), `compose`/`pipeline` as the package name
(see [`rejected-branches.md`](rejected-branches.md) R2/R3).

## A7. `flow` is NOT privileged — compatibility lives in the description

The domains (`core`/`io`/`options`/`spot`/`portfolio`/`risk`) **know nothing about `flow`**. They do
their narrow task and expose a **described contract** (below). Therefore compatibility cannot live
*inside* `flow` — it lives in the **descriptions**. Consequence: a chain can be assembled by

- **`flow`** — reads contracts, wires outputs→inputs, demand-driven (Layer B) + caching; **or**
- **a user** — by hand in plain code (call a producer, take its result, pass to the next).

`flow` is **one** assembler, ergonomic but optional. This is the R1/R2 spirit (new source = new
provider, no caller change) applied to calculations.

## Data vs contract — there is NO `ResultMeta` entity (the pivot)

Earlier drafts put a `ResultMeta` provenance entity beside every frame. **Rejected** (owner,
2026-06-20) — see [`rejected-branches.md`](rejected-branches.md) R1 (params-in-frame) and R4
(meta-entity). The question that killed it: *what is in `meta` that is not already in the schemas?*

- **Schema = type/shape** (class-level): "a `forecast_distribution` frame has columns
  `quantile, value`". No values.
- A would-be `meta` = **the instance's scalar values** (`as_of`, `spot`, `model`, `seed`). But those
  are **just values** — already carried as the **ergonomic object's fields** (`ForecastResult.spot`,
  `.horizon_years`) / function returns, and their **name+type is describable in the contract** next to
  the frame schema.
- **`kind`** = which result this is = a **reference to its schema** (a frame "is a"
  `forecast_distribution` by conforming). Not a separate field to carry.
- **Lineage / provenance** = **not a domain concern** → `flow.RunRecord` (owned by the run, optional).

**Conclusion: no domain `meta` entity.** Instead, the **contract** describes the full I/O (frame
columns **+** scalar fields); scalar values ride as ordinary result fields / returns; provenance is
`flow`'s. A `meta` object would only re-bundle schema-described values and risk drift.

> Honest caveat: pandera describes **frames**, not scalars. So a contract = a pandera frame-schema
> **plus** a small typed **scalar-field spec**. That spec lives in the registry (A2), not as an object
> on the result.

## Layering & the two shapes of a lib function (A9)

Three tiers, each depends only downward; each usable alone:

```
options/lib   — pure functions:  df + params → df          (functional, standalone, quant-friendly)
class layer   — binds / combines: df → ergonomic object + recomputed scalars + bound eval funcs
flow          — on top: contracts, Registry, Plan, resolver
```

- **lib is functional and flow-agnostic.** A quant can use it in a notebook with no class/flow.
  If lib emits a **tidy pinned df, that df *is* the interchange** — no separate `to_interchange()`
  needed at lib level; the contract's frame-schema pins the lib output directly.
- **Scalars live at the class layer / as flow edges, not in lib output.** `as_of`/`spot` are
  recomputable from the input (the class computes & carries them once); `model`/`engine`/`seed` are
  input params the caller already holds. So "lib returns df only" and "no scalars in the data frame"
  are both satisfied.
- **Classes are the in-process exchange (default, fast); the interchange df is the boundary form**
  (declarative Plan / serialization / cross-domain fan-in / inspection) — materialized opt-in (A8).

### Two shapes of a lib function (the cardinality test)

A lib function is one of two shapes — test: **"one output value per input row?"**

| | **Shape 1 — enrichment (row-aligned)** | **Shape 2 — reduction / re-projection** |
|---|---|---|
| output | `df + new column(s)` or a `Series` the caller appends | a **new tidy frame of its own kind** |
| when | output aligned to input rows | output is a different axis/cardinality |
| examples | validate · clean · `add_iv`/`add_greeks` · recompute a column · **fill** gaps · **evaluate a smile onto a strike grid** (`svi_iv(params, k_df)→k_df+iv`) | `fit_smile` (N strikes → 5 params) · `forecast_*` (T timestamps → Q quantiles) · `risk_var` (→ var/cvar) · `aggregate` (legs → one distribution) · **PnL** · any new entity-frame |
| compatibility | **column presence** — generalize `OPTION_COLUMN_DEPENDENCIES` (`column → required columns`, already in `_terms.py`) | **kind / frame-schema** (the contract) |

The early pipeline (load → validate → clean → enrich → fill) is mostly **Shape 1**: column accretion
on one evolving board df — exactly the existing `add_<x>` + `OPTION_COLUMN_DEPENDENCIES` idiom. The
analytic half (fit → forecast → risk → aggregate) is **Shape 2**: axis-changing, emits a new-kind
frame. `flow` carries **both** compatibility mechanisms.

This **dissolves most of the earlier "non-df output" worry**: evaluation (smile/greeks onto a grid)
is a Shape-1 column-add, not an object; only genuine **reductions** (a fit's `params_df`, a forecast's
`distribution_df`) are new frames. A callable like `_predict_iv` = a Shape-2 `params_df` **+** a pure
eval function (itself a Shape-1 column-add); the `SmileResult` object that bundles them is class-layer.

> **Confirmed (owner, 2026-06-20):** a **new entity-dataframe = a new schema/kind** — PnL, var,
> forecast, surface, smile-fit, … each is a Shape-2 kind with its own pinned schema. Row-aligned
> enrichments are *not* new kinds; they extend the board's column set (Shape 1).

> **Refactor implication:** today lib returns **classes** (`SmileResult`, `ForecastResult` — with
> methods/callables), and `SmileResult` even lives *in* `options/lib`. Target: lib exposes the pure
> primitives (`fit → params_df`, `predict → df`/Series, `forecast → distribution_df`); the classes
> that wrap them are the class layer. Migrate incrementally (the first vertical slice does price).

## The I/O contract (A2) — basis of compatibility

Per calculation **kind**, the contract declares:

```
Contract(kind) = inputs:  { frame-schema(s) by input-kind, param-spec (typed scalars) }
                 outputs: { frame-schema (tidy, R4 terms), scalar-spec (typed scalars) }
```

Compatibility = an output's `(frame-schema + scalar-spec)` satisfies a downstream input's
requirements ⇒ they compose. This is checkable **statically from the contracts**, by `flow` or by a
reading human. Examples:

```
forecast_price : in  frames {price_series}        params {horizon, model, engine, seed, source}
                 out frame  {quantile, value, change}   scalars {as_of, spot, horizon_years}
risk_var       : in  frames {forecast_distribution} params {confidence}
                 out frame  {confidence, var, cvar}     scalars {as_of}
```

**A2 open:** how formal is the registry — a plain `dict` kind→Contract the resolver reads, vs. a
first-class entity (validated, acyclicity-checked, serializable). Must be expressive enough for Layer
B to read requirements backward (A6).

## The registry — open, domains plug in (multi-domain)

`flow` defines only domain-agnostic pieces (`Producer`, `Result`, `Contract`, `Registry`, `Resolver`,
`RunRecord`). **Domains register their producers** into the shared registry:

- `options` → `fit_smile` / `fill_surface` / `forecast_smile` / `forecast_surface` / …
- `spot` → `forecast_price` / `forecast_vol` only (no smile/surface — *much* simpler; do it cheaply now)
- `bonds` → `forecast_yield` / `curve`

Adding a domain = register its producers, **no `flow` change**. Kinds are **neutral-first**
(`price_series`, `vol_series`, `forecast_distribution`, `var`); domain-specific only when genuinely
bound (`smile_fit`/`surface_fit` = derivatives, `yield_curve` = bonds). A spot and an options *price*
forecast share the **same** `price_series → forecast_distribution` contract — only the producer
differs. Uniformity for a fan-in node comes from the **shared output schema of the kind**, not from a
shared meta base class.

**Portfolio** splits (resolves the open question): a **neutral distributional aggregation**
(`forecast_distribution` legs of any domain + cross-leg coupling → aggregate distribution → VaR/CVaR)
at chain/portfolio level; vs. an **options payoff/greeks aggregation** reusing
`options/analytic/risk/payoff`, staying in the `options` domain. Two concerns, kept apart (mirrors T35
risk *measure* vs position *book*).

## Universal I/O requirements (every node, every domain)

```
producer(*input_frames, **params) -> (frame, *scalars)     # described by Contract(kind)
```

**Inputs**
1. **Typed kinds** (neutral-first). The kind, not a positional dataframe, is the contract.
2. **Three natures, separated:** *data* (upstream result frames) · *params* (typed scalars; validated;
   never injected into a frame) · *context/envelope* (a data-context kind — "the board over period P",
   A6/B6).
3. **Provide-or-compute:** an input is passed in, or auto-resolved from its producer (`flow`) —
   generalizes the forecast `source=` / T30 `prior=`.
4. **Validated at the boundary:** frames via pandera (R4.4 toggle), params via the param-spec.

**Outputs**
1. **`Result = frame + scalars`**, described by the contract. The ergonomic rich object (with
   `.point()/.quantiles()`) is a **view** over that, for humans; the wide `to_frame()` is a **render**
   for display — neither is the interchange form.
2. **Frame = data only, schema-pinned, tidy.** Per-row dims (`quantile`, `k`, `tenor`) are columns;
   scalars are **not** in the frame.
3. **Scalars = ordinary fields**, typed by the contract's scalar-spec.
4. **Closure:** every output kind is a legal input kind → the chain composes.

**Cross-cutting:** neutral-first (R0) · reproducibility via `seed` + `flow.RunRecord` · D2 surface =
the frame-schema + param-spec per kind (verify once, not per call site) · no math in `flow` (R3
orchestration; math in the domains' R5 `lib`).

### A4. Interchange frame form — leaning tidy

Wide `to_frame()` has **dynamic** columns and can't be pinned: `ForecastResult.to_frame()` →
`quantile | <target> | change` (level column named by target); `SmileForecast.to_frame(quantiles=…)`
→ `k | iv | iv_q0.05 | iv_q0.5 | …` (count+names depend on the quantiles); surface adds `tenor`. Fix =
a **tidy** interchange (`quantile|value`, `k|quantile|iv`) so the column set is fixed. Keep the
ergonomic wide `to_frame()` as the human render; add a canonical `to_interchange()`. The boundary:
**per-row varies → column; scalar over the frame → a contract scalar** (not a frame column).

### A5. Where neutral terms live — pending

Chain terms (`quantile`, `value`, `horizon_years`, `confidence`, `target`, `model`, `engine`) are
**neutral** (forecast/risk also serve spot/bonds) → `core.dictionary` (R0 neutral core), not
`OptionsTerm`. New neutral registry vs. extend `Term` — TBD.

### A6. Input-contract expressiveness — pending

An input kind must be able to declare "**the board over period P**" (a data envelope), not only "these
legs", so Layer B can derive the data requirement backward. Shapes A2.

## Grounding in the current code (gap analysis, 2026-06-20)

What the code does **today** (the producers we must adapt):

1. **Outputs = heterogeneous rich objects.** `OptionsForecast.price/vol → ForecastResult` (scalars
   already fields: `.target/.model/.engine/.horizon_years/.spot`); `.smile → SmileForecast`;
   `.surface → SurfaceForecast`; `OptionsPricer.fit_smile → SmileResult`. **Scalars are already
   fields** → confirms: no `meta` entity needed.
2. **Inputs = raw DataFrames + kwargs, not results.** The facade pulls raw `df_hist`/`df_fut` from
   `OptionsData` and converts to numpy via bespoke adapters inside the lib: `forecast_class._price_series
   → _series.futures_price_series → (prices, timestamps)`; smile → `_theta.build_theta_history →
   (theta, timestamps, expiration)`.
3. **The chain is re-derivation, not composition.** `build_theta_history` (`_theta.py`) **re-fits**
   an SVI smile per timestamp (`make_smile_model` directly), instead of consuming `SmileResult`s. The
   "fit → forecast" link is rebuilt from the raw frame — exactly the ad-hoc plumbing ADR 0003 removes.

**Three gaps vs the contract model:**

| gap | today | needed |
|---|---|---|
| G1 outputs not interchange | wide `to_frame()`, dynamic columns, unpinnable | per kind: `to_interchange()` (tidy) + pandera schema + scalar-spec |
| G2 inputs are raw, not kinds | `_series`/`_theta` bespoke adapters in the lib | make `price_series` / `theta_history` named **kinds** with a pinned schema |
| G3 chain = re-derivation | smile-forecast refits internally | express the intermediate as a kind so it *can* compose & be shared (opt-in) |

## Concrete function shapes — interchange & kind by example

**`interchange` = a tidy DataFrame with a fixed column set for a kind.** Contrast:

```
ForecastResult.to_frame()  →  quantile | <target> | change      # wide, columns float (price/vol/iv_q…)
to_interchange()           →  quantile | value | change          # kind=forecast_distribution, columns PINNED
                              0.05      98.1    -1.9              # quantile is a ROW, not a column
                              0.50     100.2     0.2
```

**`kind` = a string label for a contract** ("this producer outputs a `forecast_distribution`").

A **producer is a plain function with a declared contract** — input = an interchange frame of the
required kind + params; output = a result with `to_interchange()` and a registered kind:

```python
# producer: price_series   (board → price_series)   = the former _series.py, now with a pinned schema
def price_series(board, *, source="front", expiration=None) -> PriceSeries:
    prices, ts = futures_price_series(board, expiration) if source == "future" else front_price_series(board)
    return PriceSeries(frame=pd.DataFrame({"timestamp": ts, "price": prices}))   # kind="price_series"

# producer: forecast_distribution   (price_series → forecast_distribution)
def forecast_price(series, *, horizon, model="gbm", engine="montecarlo", seed=None) -> ForecastResult:
    prices = series["price"].to_numpy(); ts = pd.DatetimeIndex(series["timestamp"])
    fitted = make_forecast_model("price", model).fit(prices, median_dt_years(ts), to_horizon_years(horizon, ts.max()))
    return make_engine(engine, seed=seed).run(fitted)        # kind="forecast_distribution"
```

The **contract is registered as data** (this is the "согласование / mapping model"):

```python
register(kind="forecast_distribution",
         inputs={"series": "price_series"},                 # input = the price_series kind
         params=ParamSpec(horizon=..., model=str, engine=str, seed=(int, None)),
         frame=ForecastDistributionSchema,                  # pandera: quantile|value|change
         scalars=("as_of", "spot", "horizon_years", "model", "engine"))
```

Reconciliation `price_series → forecast_distribution` holds because **one kind = one schema = the
output of one and the input of the other** (closure), checkable from the registry without running.

## Invocation is NOT necessarily dotted — a transformation model

The contract decouples the *function* from *how it is invoked*. The **same** producer, three surfaces:

```python
# 1) facade, dotted (today)
fc = Option(data).forecast.price(horizon="30d", model="gbm", source="front")
# 2) plain function call
fc = forecast_price(price_series(board, source="front"), horizon="30d", model="gbm")
# 3) DECLARATIVE — a transformation model (data; flow interprets it)
plan = Plan(nodes=[
    Node("ps",  "price_series",          inputs={"board": board}, params={"source": "front"}),
    Node("fc",  "forecast_distribution", inputs={"series": "ps"}, params={"horizon": "30d", "model": "gbm"}),
    Node("var", "var",                   inputs={"dist": "fc"},   params={"confidence": 0.99}),
])
result = flow.run(plan)        # resolver wires by kind, caches shared nodes
```

Style (3) is the "transformation model": the chain is **described as data** (kinds + edges), not as
`obj.m().m()`. All three share one contract.

## Efficiency: a contract is NOT forced decomposition

> A `kind` is a **contract**, not an obligation to decompose. A producer may be **one consolidated
> algorithm** that **reuses lib code** from other elements internally while exposing a single contract
> (`board → surface_forecast`). Intermediate interchange frames need **not** be materialized.

`forecast_surface` is already such a consolidated producer and stays one:

```python
def forecast_surface(board, *, horizon, model="svi_surface", ...) -> SurfaceForecast:
    theta, ts, nodes = constant_maturity_theta_history(board, DEFAULT_TENOR_NODES, "svi")  # reuses make_smile_model + interp
    fitted = make_surface_forecast_model(model).fit(theta, median_dt_years(ts), to_horizon_years(horizon, ts.max()), ...)
    return make_surface_engine(engine, nodes).run(fitted)     # kind="surface_forecast" — no per-slice interchange
```

**Granularity is a choice:**
- **Coarse producer** — the default; speed; reuse at the **R5 lib-function** level, not via
  materializing intermediates.
- **Fine producers** (`fit_smile`, `theta_history` as separate kinds) — only when the intermediate is
  genuinely **shared across branches** (one `theta_history` feeds both smile- and surface-forecast) or
  must be inspected.

So code-reuse and efficiency do not conflict with the contract: reuse lives in `lib`; materializing
interchange is **opt-in**, enabled only when sharing/inspection pays for it.

## First vertical slice (proposed)

Prototype the contract on **price** — it is ~90% there:
`price_series (timestamp|price) → forecast_distribution (quantile|value|change) + scalars{as_of,spot,horizon_years}`.
Needs only: a tidy `to_interchange()` + the pandera schema + registering `price_series` as a producer.
Then replicate to smile/surface (where G3 bites). No behavior change to the existing facade.
