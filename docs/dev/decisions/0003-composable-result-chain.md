# 0003 — Composable result-chain: calculations feed calculations

- **Status:** Proposed (owner-scoped; architecture recorded, implementation phased)
- **Owner:** akuminov@gmail.com
- **References:** R3 (facade components), R5 (pure lib), R4 (term registry), D2; backlog T27
  (forecast — factor-conditional price models), T29/T30 (fitting), T33 (portfolio), T35 (risk).
  Generalizes ADR 0002 (the forecast factory is one *producer* in this chain).

> **Design evolution (2026-06-20, in progress — see [`design/result-chain/`](../../../_aitna/design/result-chain/README.md)).**
> Active co-design widened this ADR's Phase 1 from "pin the existing `to_frame()` schemas" into a full
> pipeline contract. Firmed up so far (not yet folded into the Decision below — the concept folder is
> the live source until locked):
> - **Module = `alphavar.flow`** (root, cross-domain spine; `composer` = the demand-driven assembler
>   inside it). `chain` was taken (option chain); `pipeline`/`compose` rejected as the package name.
> - **No `ResultMeta` provenance entity** (the earlier "structured frame + meta" idea, Decision §1, is
>   superseded): the **contract** describes the full I/O (frame schema + scalar-spec); scalars ride as
>   class-layer fields / flow edges; provenance → `flow.RunRecord`. (Rejected with a *revisit-if*; the
>   most likely branch to flip back if results must travel outside a run.)
> - **Compatibility lives in the descriptions, not in `flow`** → a chain is assemblable by `flow`
>   (declarative `Plan`, YAML/dict) **or** by a user in plain code; domains stay flow-agnostic.
> - **Layering:** `lib` (pure `df+params→df`, functional) / class layer / `flow`. **Two shapes of a
>   lib function:** Shape 1 enrichment (row-aligned `df+cols`/Series; compat = column presence,
>   generalizing `OPTION_COLUMN_DEPENDENCIES`) vs Shape 2 reduction (new-kind tidy frame; compat =
>   kind/schema). A new entity-frame (PnL/var/forecast/surface/fit) = a new Shape-2 kind.
> - **Registry = Python code** (binds functions + pandera schemas); **Plan = declarative data**.
> - The pipeline is a **branching DAG over two axes** (calc chain × multi-leg structures with a
>   collective correlated fan-in), with a **demand-driven planner** (Layer B) that derives the data
>   envelope + params backward from the *subject of analysis* (the alpha-search framing).
> - **Priority reframe (2026-06-21, owner):** the product is the **lib/class contracts**, not `flow`.
>   `flow` is **one of three** consumers (flow auto-assembly · a developer by hand · an AI agent) of
>   the same self-describing descriptions. Build order = **options lib/class contracts → other domains
>   → `flow` last**; `flow`'s implementation is **not a priority**.
> - **Strict component autonomy (2026-06-21, owner — supersedes Decision §2 placement):** a component
>   (lib fn or domain class, incl. etl/exchange) knows **only its own contract** and **never invokes its
>   upstream producer**. So **`resolve` / "provide-or-compute" does NOT live in the domain classes** —
>   components take inputs **explicitly**; the resolver is an **assembler** concern (user / AI agent /
>   `flow`), one level up. The factor-conditional / risk / portfolio models still build directly on the
>   class contracts (**superseding the Rollout note that they "remain `NotImplementedError` until phase
>   2 lands"**) but receive their upstream frames **passed in by the caller**, not derived internally.
> - **Data acquisition is in the unified graph:** etl/exchange/provider register **producer(s)** in the
>   same vocabulary (a *load* = `{symbol, period → canonical frame-kind}`), so an assembler can plan the
>   whole path including what to load; fetch internals stay R1/R2.
> - **Contract is DERIVED from the function (2026-06-21, owner) — supersedes the A2 `register(kind=…,
>   inputs=…, params=…, frame=…, scalars=…)` sketch:** a producer re-declares nothing; `core.disc.Disc`
>   reads `inputs/params/output_schema/scalars/interchange` off the signature + return type (schema lives
>   in the type: `DataFrame[Schema]` / `interchange_schema` ClassVar). Registration supplies only what the
>   function can't state — a `kind` override and the one `consumes=` *alternatives* edge. Closes the
>   smile/surface `output_schema=None` gap. See [`design/result-chain/disc-derivation.md`](../../../_aitna/design/result-chain/disc-derivation.md).

## Context

Capability areas already compute reusable intermediate results: a **smile/surface fit** (T21/T29),
a **price/vol forecast distribution** (T27), a **shifted live smile** (T30). The owner's intent is
that these are not dead-ends but **inputs to further calculations**:

- a fit feeds a forecast (forecast the calibrated θ — already done for smile/surface, ADR 0002);
- a forecast distribution feeds **VaR/CVaR** (T35) and **portfolio** risk (T33);
- a **factor-conditional** price model (`factor_linear` / `var`, T27 it.5) needs *other* series as
  inputs — rates, futures-spot basis, realized vol, macro — and a **horizon scenario** for each
  factor, which is *itself a forecast*. So a forecast can depend on other forecasts.

Today each producer is invoked imperatively and its output is an ad-hoc object. There is no shared
contract for "the result of calculation X, consumable by calculation Y", and no story for
**auto-computing a missing upstream input** when a downstream calculation needs it. Without one, the
factor-conditional models (and later the portfolio/risk layers) would each hand-roll their input
plumbing and re-implement "compute the prerequisite if absent".

## Decision

Adopt a **composable result-chain**: calculation outputs are **structured, schema-pinned DataFrames**
(per R4 term registry) that can be passed as inputs to downstream calculations, with a thin
**resolver** that auto-computes a missing input from its producer.

1. **Results are structured frames.** Each producer (fit, forecast, shift, risk measure) exposes a
   canonical `to_frame()` with a registered column schema (R4 terms; pandera model per result kind).
   Rich result objects (`SmileResult`, `ForecastResult`, `SmileForecast`, …) **stay** as the
   ergonomic API; the frame is the *interchange* form between capability areas — the same role the
   canonical book frame plays between providers (R1/R2).
2. **Inputs are "provide-or-compute".** A downstream calculation accepts an upstream result either
   **passed in** (a frame / result object) **or** computed on demand from a declared producer +
   params. One resolver (`resolve(kind, *, given=None, **how)`) returns the given input when present,
   else invokes the producer — the same ergonomics as the forecast `source=` param and the T30
   `prior=` (caller-supplied, else derived), generalized.
3. **Dependencies are explicit and acyclic.** A calculation declares the input kinds it needs (e.g.
   `factor_linear` needs `{price_series, factor_series[], factor_scenario[]}`); resolution walks the
   declared producers. Cycles are an error; a factor that is itself a forecast composes one level at
   a time (a factor's horizon scenario is produced by *its* forecast before the dependent one runs).

The chain crosses facade components over the shared `OptionsData` (R3), so it stays source-agnostic
and adds **no new stored columns** — these are computed outputs, like the rest of `forecast`.

## Consequences

- **Unblocks the factor-conditional forecast models** (`factor_linear` / `var`, T27 it.5): they
  become "consume a factor frame + a factor horizon-scenario frame", with the scenario auto-resolved
  to a forecast when not supplied — no bespoke plumbing per model.
- **Gives T33 (portfolio) / T35 (risk) their input contract**: VaR/CVaR consume `ForecastResult`
  frames; a portfolio aggregates per-instrument result frames; stress reval consumes a shifted-smile
  (T30) frame. All via the one resolver.
- **Frames are R4/pandera-pinned and D2-verified** per result kind (Type C where math is involved).
  The interchange schemas are the verification surface, not each call site.
- Pure transforms stay in `lib` (R5); the resolver is facade-level orchestration (R3), holding no
  math.

## Rollout (phased; each gates on `uv run pytest`)

1. **Contract first (no behavior change):** pin the `to_frame()` schema for the existing producers
   (`ForecastResult`, `SmileForecast`, `SurfaceForecast`, `SmileResult`) as registered R4 term sets +
   pandera models. This is the interchange format; nothing consumes it cross-area yet.
2. **Resolver + factor inputs:** add `resolve(kind, given=, **how)` and the factor-input frames
   (factor series + horizon scenario), then build `factor_linear` / `var` on top (closes the T27 it.5
   factor deferral).
3. **Downstream consumers:** T35 risk (VaR/CVaR over a result frame) and T33 portfolio (aggregate
   result frames) consume the chain; T30 smile-shift `prior=` resolution folds in as a special case.

Until phase 2 lands, the factor-conditional models remain `NotImplementedError` in the forecast
factory (ADR 0002), explicitly pointing here for the missing input contract.
