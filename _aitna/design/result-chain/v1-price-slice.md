# V1-lc — price vertical slice (implementation spec)

> **Scope:** the first concrete contract path — `load → price_series → forecast_distribution` — built
> as **three autonomous producers** (P-autonomy) that the caller wires (P-selfdesc). Proves
> A4a/Inp/Load/A5 on one path before generalizing (A9c). **Spec only — no code yet** (owner-owned).
> Relates to [README](README.md), [TASKS](TASKS.md) (V1-lc), [ADR 0003](../../../docs/dev/decisions/0003-composable-result-chain.md), T27.

## The chain (each box = an autonomous producer; arrows = wiring done by the caller)

```
 {asset_code, period, timeframe}            futures_history|options_history     price_series
            │                                          │   {source, expiration}     │  {model, engine, horizon, n, seed}
            ▼                                          ▼                              ▼
   ┌──────────────────┐   futures_history   ┌──────────────────┐  price_series  ┌────────────────────────┐
   │  P1  load        │ ──────────────────► │  P2 price_series │ ─────────────► │ P3 forecast_distribution│
   │  (io/provider)   │   options_history   │  (lib, R5)       │  (ts|price)    │ (lib+class)             │
   └──────────────────┘                     └──────────────────┘                └────────────────────────┘
```

**Autonomy invariant (P-autonomy):** no box reaches back to the one on its left. P2 does **not** load;
P3 does **not** build a series. The composition (which output feeds which input, and computing a
prerequisite if absent) is the **assembler's** job — user code, an AI agent, or `flow` — never inside a
producer. Every box is callable alone with explicit inputs.

## P1 — `load` (data acquisition is a graph node, P-data)
- **Kind(s) produced:** `futures_history` (schema `FuturesHistory`) · `options_history`
  (`OptionsHistory`) — the existing entity frames, **unchanged**.
- **Inputs (params):** `asset_code: str`, `period: (from, to)`, `timeframe: Timeframe`.
- **Today:** `OptionsData.df_fut` / `.df_hist` → `provider.load_futures_history(...)` /
  `load_options_history(...)`. **Fetch internals stay R1/R2** (wire format, exchange specifics hidden);
  only the **output frame-kind** is exposed to the chain. No code change needed for V1 beyond
  *registering* these as named producers in the self-describing surface (Disc) — the frames already
  exist and are schema-pinned.
- **Autonomy:** knows only the provider contract; nothing about series/forecast.

## P2 — `price_series` (pure lib, R5; Shape 2 reduction → a new tidy kind)
- **Kind produced:** `price_series` — a tidy frame **`timestamp | price`**, chronological.
- **Input kinds:** one of `futures_history` / `options_history` (passed in as a frame).
- **Params:** `source: {future|front|underlying}`, `expiration: pd.Timestamp | None`.
- **Today:** `lib/forecast/_series.py` (`futures_price_series`, `front_price_series`,
  `underlying_price_series`). **Change:** return a tidy `timestamp|price` frame instead of a
  `(np.ndarray, DatetimeIndex)` tuple — the frame **is** the interchange (A4).
- **Moves OUT to the assembler (Inp):** the facade's current `_price_series` *fallback* ("if `df_fut`
  empty → use underlying") **decides which upstream to use** ⇒ that is **assembly**, not a producer
  concern. The producer just transforms the frame it is handed. `source=` (choosing among `future`/
  `front`/`underlying`) stays a legit producer param — it selects the producer's *own* construction,
  not who loaded the data.
- **Schema `PriceSeriesSchema`** (new, pandera; neutral terms from A5):
  | column      | term                | dtype           | rule              |
  |-------------|---------------------|-----------------|-------------------|
  | `timestamp` | `Term.TIMESTAMP`    | datetime64[ns]  | sorted, unique    |
  | `price`     | `Term.PRICE`        | float64         | > 0, finite       |

## P3 — `forecast_distribution` (lib factory + class result; Shape 2 reduction)
- **Kind produced:** `forecast_distribution` — a tidy frame **`quantile | value | change`** + scalars.
- **Input kind:** a `price_series` frame.
- **Params:** `target=price` (fixed for V1), `model`, `engine`, `horizon`, `n`, `seed`, and the
  `quantiles` grid for the render.
- **Today:** `make_forecast_model(PRICE, model).fit(prices, dt_years, horizon_years)` +
  `make_engine(engine, n, seed).run(fitted)` → `ForecastResult`. The math is **unchanged** (already
  T27 D2-pending).
- **Change — add `ForecastResult.to_interchange(quantiles)`** → tidy `quantile | value | change`.
  Differs from the existing `to_frame()` only in the **neutral** level column name: `to_frame` names it
  by target (`price`); `to_interchange` uses **`value`** so *any* target shares one schema. (`to_frame`
  stays as the ergonomic human render — A4.)
- **Scalars (ride on the result object / contract scalar-spec, NOT in the frame):** `as_of`, `spot`,
  `horizon_years`, `target`, `model`, `engine`. Per the "params don't go in the frame" rule; `as_of` is
  new on `ForecastResult` (= `timestamps.max()`, today computed in the facade and dropped).
- **Schema `ForecastDistributionSchema`** (new, pandera; neutral terms A5):
  | column     | term                  | dtype   | rule                |
  |------------|-----------------------|---------|---------------------|
  | `quantile` | `ResultTerm.QUANTILE` | float64 | in (0,1), ascending |
  | `value`    | `ResultTerm.VALUE`    | float64 | finite              |
  | `change`   | `ResultTerm.CHANGE`   | float64 | = value − spot      |

## A5 — neutral chain/result terms — DECIDED (owner, 2026-06-21): **sibling `ResultTerm`**
The result vocabulary (`quantile`, `value`, `change`, `horizon_years`, `confidence`, `target`, `model`,
`engine`) is **not market-data** — it is chain/result vocabulary. A **sibling registry `ResultTerm` in
`core.dictionary`** (next to `Term`) holds it, so the market-data `Term` (identity/time/price/OHLC)
stays clean. `Term.TIMESTAMP` / `Term.PRICE` are reused for the `price_series` frame; `ResultTerm`
covers only the result/chain terms.

## Assembler — the minimal `flow` prototype (DECIDED: reads the Disc contracts)
The end-to-end call is **assembly**, not a producer. It is realized as the **first, smallest `flow`**:
a tiny **contract-reading interpreter** in `alphavar/flow/` that takes a chain of `kind`s, reads each
producer's **self-description** (Disc, `kind → I/O`), and wires `output → input` — exactly what a user
or an AI agent would do by hand off the same surface. This is the seed that proves `flow` is
**non-privileged** (A7): it reads contracts, it does not hardcode the steps.

```python
# by hand / by an agent (off the contracts) — what the prototype mechanizes:
fut = load(asset_code="BTC", period=(t0, t1), timeframe=DAY)   # P1 → futures_history frame
px  = price_series(fut, source="future")                       # P2 → timestamp|price frame
fc  = forecast_distribution(px, model="gbm", horizon=30)       # P3 → ForecastResult
fc.to_interchange()                                            #    → quantile|value|change frame

# the flow prototype does the same by reading kind→I/O and matching kinds:
flow.run(["load", "price_series", "forecast_distribution"], params={...})
```
**Scope of the prototype = minimal:** read the three Disc contracts + match output-kind→input-kind +
run forward. The full `flow` (formal `Contract` dataclass A2a, acyclicity A2b, `RunRecord`, the Layer-B
demand-driven planner) stays **Phase 2** — the prototype is a thin, contract-reading executor, not the
planner.

## Fate of `Option.forecast.price()` — DECIDED (owner, 2026-06-21): **(a) reduce to a producer**
Today `OptionsForecast.price(...)` does **all three** (loads via `self.data`, builds the series,
forecasts) — an **assembler hidden inside a domain class**, which strict P-autonomy disallows for a
*producer*. Decision: **reduce it to the autonomous `forecast_distribution` producer** (takes a
`price_series` frame in; no loading, no series-building). The end-to-end convenience **moves out** —
realized as the **minimal `flow` prototype** (next section), **not** a facade method or a loose recipe.
This brings a small, intended interactive-API change: the one-call `Option.forecast.price()` is replaced
by the three explicit producers wired by the flow prototype (or by hand / an agent off the contracts).

## Code touch-list (once the open point is settled)
1. `core/dictionary/_result_terms.py` — `ResultTerm` registry (+ export). *(A5)*
2. `options/schemas/_schemas.py` (or a chain schemas module) — `PriceSeriesSchema`,
   `ForecastDistributionSchema`. *(A4a)*
3. `lib/forecast/_series.py` — return tidy `timestamp|price` frames (+ keep thin numpy accessors if
   needed internally). *(Inp)*
4. `lib/forecast/_base.py` — `ForecastResult.as_of` field + `to_interchange(quantiles)`. *(A4a)*
5. `forecast_class.py` — **(a)**: reduce `OptionsForecast` to the `forecast_distribution` producer
   (takes a `price_series` frame; drop the loading + series-building); move the `df_fut`-empty fallback
   out to the assembler.
6. Self-describing surface (Disc) — each producer carries its `kind → (input_kinds, params, output
   schema + scalar names)` as **introspectable data, off the contracts** (no `flow` import); a small
   `describe(kind)` / listing so a user, an agent, **and** the flow prototype read the same surface.
7. `alphavar/flow/` — the **minimal flow prototype**: a contract-reading interpreter
   (`flow.run([kinds], params)`) that wires output-kind→input-kind off Disc. *(Seed only; formal
   Contract/registry/planner = Phase 2.)*
8. Tests: each producer in isolation + the flow-prototype chain == today's `Option.forecast.price()`
   output (same seed, bit-for-bit).

## D2 / verification surface
- **No new math.** V1-lc is *structural* (frame shaping + schemas); the numbers (`quantiles`,
  lognormal terminal, `norm_ppf`, etc.) are the **existing** T27 producers, already D2-pending under
  their ledger rows. `to_interchange()` only re-renders `quantiles()`.
- **D2 owner-verify** the *shape* decisions: `value = quantiles(q)`, `change = value − spot`, and that
  `as_of` = last series timestamp. No new `4VERIFY` math headers introduced.
- **Acceptance:** `uv run pytest` green incl. the new isolation + equivalence tests; `ruff` clean; the
  hand-wired chain reproduces the current facade output bit-for-bit (same seed).
