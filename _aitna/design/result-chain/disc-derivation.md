# Disc — the producer contract is DERIVED from the function, not re-declared

> Hub: [`README.md`](README.md). Refines **A2** (the I/O contract / registry) and the
> [`flow-module.md`](flow-module.md) `register(kind=…, inputs=…, params=…, frame=…, scalars=…)` sketch.
> Owner-scoped 2026-06-21; recorded before locking the V1-lc Disc surface into the wider chain.

## Problem

The first Disc (V1-lc) made every producer **re-declare** its whole contract as hand-written tuples at
registration:

```python
register(Disc(
    kind="forecast_distribution",
    produce=forecast_distribution,
    inputs=("price_series",),
    params=("horizon", "target", "model", "engine", "n", "seed"),
    output_schema=ForecastDistributionSchema,
    scalars=(AS_OF, SPOT, HORIZON_YEARS, TARGET, MODEL, ENGINE),
    interchange=lambda r: r.to_interchange(),
))
```

The owner's objection (2026-06-21): **the Python function already IS the contract.** Almost every field
above duplicates something the signature or the return type already states — so the registry can drift
from the code, and adding a producer means maintaining the same facts twice. Also: smile/surface carried
`output_schema=None` + an explicit `interchange=lambda r: r.to_frame()` — a missing schema *and* a redundant
renderer, when `to_frame` is already a method on the result type.

## Analysis — what is redundant vs irreducible

| Disc field | Already stated by | Verdict |
|---|---|---|
| `kind` | `fn.__name__` | derive (override only for a private wrapper) |
| `params` | the signature (params after the input frames) | derive |
| `inputs` | the **first parameter** (the *subject*, D7 convention); its *kind* = the param name | derive — except sources / alternatives / multi |
| `output_schema` | the **return type**: `DataFrame[Schema]` carries it; a result class exposes `interchange_schema` | derive |
| `scalars` | the result type's `interchange_scalars` (the values are already its fields) | derive |
| `interchange` | the result type's own `to_interchange` / `to_frame` method | derive (a frame is its own interchange) |

**One thing is genuinely not in the types:** the *semantic kind* of a DataFrame input. `price_series:
pd.DataFrame` — the annotation is just `pd.DataFrame`; that it must be fed a `price_series`-kind frame,
and that `price_series` itself accepts `futures_history` **or** `options_history`, is graph-edge knowledge
types can't express. Where the parameter is **named after its kind** (`forecast_distribution(price_series,
…)`, `forecast_smile(options_history, …)`) the edge is recoverable from the name; the **alternatives** slot
is the only case that must be declared.

The original "register so an assembler can read the surface without importing the producer" goal is met by
Python's own reflection — `inspect.signature` + `typing.get_type_hints` read the function statically,
without calling it or instantiating the result.

## Decision

**The contract is a derived view over the function.** The registry stores only the callable plus the two
things the function can't state: an optional `kind` override and the optional `consumes=` edge (alternatives
only). `core.disc.Disc` computes `inputs / params / output_schema / scalars / interchange` on demand:

```python
register(forecast_distribution)                                  # edge inferred from the `price_series` param
register(price_series, consumes=[("futures_history", "options_history")])   # the one alternatives edge
register(_load_futures_history, kind="futures_history")          # private wrapper → kind override
```

To make derivation real, the **schema lives in the type**:
- frame producers annotate the return as `pandera.typing.DataFrame[Schema]` (e.g. `price_series ->
  DataFrame[PriceSeriesSchema]`);
- result producers carry `interchange_schema` (+ `interchange_scalars`) as `ClassVar` on the result type
  (`ForecastResult`, `SmileForecast`, `SurfaceForecast`), beside the existing `to_interchange`/`to_frame`.

This also **closes the smile/surface gap**: pinning `SmileForecastSchema` / `SurfaceForecastSchema` on the
types removes both the `output_schema=None` and the explicit `interchange=` lambda — they derive like
everything else.

## Consequences

- **No duplication / no drift.** A producer's contract has one source: its code. Adding a producer = one
  `register(fn)` line (plus `consumes=` only for an alternatives edge).
- **Supersedes the `flow-module.md` A2 sketch** (`register(kind=…, inputs=…, params=ParamSpec(…), frame=…,
  scalars=…)`): those become derived. The **Contract** concept (A2) stays — it is now a *read* of the
  function, not a hand-authored record.
- **Input/param split is positional (D7 convention): `f(subject, *rest)`.** The input edge is the
  **first parameter**; everything after it is a param. Its *kind label* is the param name (or `consumes=`).
  This needs **no per-parameter type resolution** (so `core.disc` no longer imports pandas / probes for a
  DataFrame type — types are read only for the *output* schema) and no dependence on registration order or
  on a name matching a kind. The exceptions are declared via `consumes=`: an *alternatives* slot, *several*
  inputs, or `consumes=[]` for a **source** (a load node with no upstream edge — e.g. the `OptionsData`
  handle is a param, not an edge). A purely-positional rule is both simpler and the exact D7 shape.
- **D2 surface unchanged:** the pandera frame-schema + scalar-spec per kind is still the verify-once
  surface — it just lives on the type now, not in the registry call.
- **Phase 2 (formal `Contract` / `Plan` / planner) reads the same derived view** — no re-declaration to
  keep in sync with the functions.
