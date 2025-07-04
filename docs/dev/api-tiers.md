# API tiers — the concrete symbol map

This document is the **realization of [R9](ARCHITECTURE_REQUIREMENTS.md#r9-product-api-tiers-one-quant-core-several-entrypoint-layers)**:
R9 states the *invariant* (one quant core, several entrypoint layers, one-way inward dependency); this
page is the *inventory* — which concrete modules and symbols sit in which tier today, their import
path, and their stability. When the surface changes, update this map; R9 stays as the rule.

> One-way dependency direction (R9): `adapters → services → facade/lib → providers/io`, and
> `researchers / agents → (flow | facade) → facade/lib → providers/io`. An inner tier never imports an
> outer one; `flow` never imports `services`.

## The tiers at a glance

| Tier | Package / symbols | Import path | Stability |
| --- | --- | --- | --- |
| **1 · Core / domain API** | `Disc`, `register`, `describe`, `catalog`, `kinds` · dictionary/registry · `entities`, `schemas` (Terms, `ResultTerm`, pandera models) · `options.lib.*` pure functions · `options.producers` | `alphavar.core.disc`, `alphavar.options.{dictionary,entities,schemas,lib}`, `alphavar.options.producers` | Stable contract (schema/Term-pinned) |
| **2 · Research API** | `Option` + components: `OptionsData`, `OptionsEnrichment`, `OptionsChain`, `OptionsAnalytic`, `OptionsAnalyticPrice`, `OptionsAnalyticRisk`, `OptionsPricer`, `OptionsForecast`, `OptionsValidation`, `ChartClass`, `ChartPriceClass` | `alphavar.options` (`Option` also re-exported at `alphavar`) | Stable facade |
| *(beside the tiers)* **Assembly mechanism** | `alphavar.flow` — `run`, `describe`, `catalog`, `kinds` | `alphavar.flow` | **Prototype / unstable** (ADR 0003) |
| **3 · Service API** | `alphavar.services` | *(not built — deferred)* | Target only (A2) |
| **4 · Adapters** | FastAPI/HTTP, workers, CLIs, dashboards, notebooks, agents; the ETL scheduler entrypoint | outside the domain core | Out of the library's contract |

## Consumers → how they assemble

ADR 0003 names three interchangeable assemblers of the same producer contracts — `flow`, a developer
in plain code, and an AI agent. They map onto consumers by intent:

| Consumer | Assembles via | Uses `flow`? |
| --- | --- | --- |
| **Researcher** (notebook / script) | `flow` and/or the `Option` facade & components | yes |
| **Service / backend** (e.g. FastAPI) | **direct library calls** (pure `lib` + facade), possibly component classes — deterministic, parameter-driven, end-to-end logic | **no** |
| **AI agent** | component classes **or** `flow` | optional |

The distinction: a **service is deterministic** — its cross-cutting processing is fixed code driven by
request parameters, so it calls the library directly (or via classes) and does **not** route through
`flow`. `flow` is the **research/agent** assembler — demand-driven wiring off the contracts, where the
chain is explored rather than fixed. The dependency-direction rule still holds (`flow` never imports
`services`; ADR 0005); this table records the *expected* usage on top of it.

## Tier 1 — Core / domain API

The most reusable surface and the target for tests, services, and `flow`.

- **Producer-contract surface** — `alphavar.core.disc`: `Disc` (a producer's self-description read off
  its signature + return type), `register`, `describe`, `catalog`, `kinds`. Domain-neutral: no
  options/futures knowledge, only the *shape* of a producer contract.
- **Domain foundation** — `alphavar.options.dictionary` (column registry / Terms), `.entities`,
  `.schemas` (pandera models, `ResultTerm`). Serialized tables use `Term`/`OptionsTerm`/`ResultTerm`
  names (R10).
- **Pure logic** — `alphavar.options.lib.*` (`analytic`, `chain`, `chart`, `enrichment`, `forecast`,
  `normalization`, `pricer`, `validation`): DataFrame in → DataFrame/result out, no I/O (R1).
- **Producer registration** — `alphavar.options.producers`: importing it publishes the options
  producers onto the `core.disc` surface so any assembler can wire them. Domain code registers; it
  never imports `flow` (non-privileged `flow`, ADR 0003).

## Tier 2 — Research API

`Option` and its component facades over a shared `OptionsData`, optimized for quant users, notebooks,
and exploratory scripts. The full component set wired by the facade is `data`, `enrichment`, `chain`,
`analytic`, `chart`, `pricer`, `validation`, `forecast`.

`Option` is the main ergonomic quant facade — **not** the only product API. It is re-exported at the
top level (`from alphavar import Option`); the individual component classes live under
`alphavar.options`. Server-side code must not be forced to drive the product through a mutable research
object when a stateless service contract fits better (R9).

> **Top-level namespace.** `alphavar` re-exports only `Option` as the headline entry. The component
> classes are imported from `alphavar.options`. Widening the top-level namespace is a separate API
> decision, deliberately not taken here.

## Assembly mechanism — `alphavar.flow` (beside the tiers, not a tier)

`flow` plans and runs a chain of producer steps off their self-describing `core.disc` contracts
(`flow.run(chain, params=…, inputs=…)`). It is **one of three interchangeable assemblers** — `flow`, a
developer in plain code, or an AI agent — over the same contracts. It spans the core/research surfaces,
carries **no stability or serialization guarantee of its own**, and **never imports `services`**.
Its consumers are **researchers and AI agents** (see [Consumers](#consumers--how-they-assemble));
**backend services do not use it** — they call the library directly. Placement and the
`flow`↔`services` boundary: [ADR 0005](decisions/0005-flow-services-etl-boundaries.md).

> **State today:** a functional **V1 prototype** (a forward chain interpreter), not an empty stub. The
> formal planner (`Contract` dataclass, acyclicity checks, `RunRecord`, demand-driven prerequisite
> computation) is Phase 2 and non-priority (ADR 0003).

## Tier 3 — Service API (deferred)

A framework-neutral use-case layer (`alphavar.services`) with typed request models, typed/serializable
results, explicit failure modes, and dependency injection of providers/storage/cache/clocks. A service
is **deterministic**: it implements fixed, parameter-driven cross-cutting logic by **calling the
library directly** (pure `lib` + facade), possibly via component classes — it does **not** route
through `flow`. **It is not built now** — a deferred future task (A2). R10 is the *compatibility
constraint* the present quant core must satisfy so this layer can be added later without rework. See
[R10](ARCHITECTURE_REQUIREMENTS.md#r10-server-ready-contracts-services-are-framework-neutral-and-explicit)
and [ADR 0005](decisions/0005-flow-services-etl-boundaries.md).

## Tier 4 — Adapters

FastAPI/HTTP handlers, workers, schedulers, CLIs, dashboards, notebooks, and AI agents. They may call
services or explicit producer/facade steps but must not own domain formulas, DataFrame semantics,
provider normalization, or schema compatibility (R9). The ETL scheduler entrypoint is an adapter around
the ETL job; `options/etl/` internals stay R6 core (ADR 0005).

## Examples

### Quant / research path (tier 2)

```python
from alphavar import Option
from alphavar.io.provider import PandasLocalFileProvider, RequestParameters
from alphavar.options.dictionary import OptionsTerm, Timeframe

provider = PandasLocalFileProvider(exchange_code="DERIBIT", data_path="/path/to/data")
opt = Option(provider, asset_code="BTC", params=RequestParameters(timeframe=Timeframe.EOD))

chain = opt.chain.select_chain()
enriched = opt.enrichment.enrich_options(
    [OptionsTerm.UNDERLYING_PRICE, OptionsTerm.INTRINSIC_VALUE, OptionsTerm.TIMED_VALUE]
)
```

### Server-embedding path (today: direct, deterministic library calls)

A backend (e.g. FastAPI) runs **fixed, parameter-driven** logic — it calls the library **directly**
(facade and/or pure `lib`), **never `flow`**. The use-case takes its `provider` as a parameter: the
**composition root** builds the concrete provider from the environment (no secret reads in the core)
and injects it (R10 DI). This is the shape a future `alphavar.services` use-case will formalize into a
typed request/result — R10/ADR 0005.

The external contract is user intent, not an internal chain: an endpoint accepts `asset_code` plus a
strategy/legs spec or other workflow parameters, resolves any policy-driven shorthand (`nearest`,
`atm`, model defaults), and returns the result together with the resolved inputs and context.

```python
from alphavar import Option
from alphavar.io.provider import AbstractProvider, PandasLocalFileProvider, RequestParameters


def build_chain_snapshot(
    provider: AbstractProvider,
    asset_code: str,
    params: RequestParameters,
) -> dict[str, object]:
    """Deterministic use-case: same domain steps every call, driven by request params + injected provider."""
    opt = Option(provider, asset_code=asset_code, params=params)
    chain = opt.chain.select_chain()
    return {
        "asset_code": asset_code,
        "request": params.model_dump(mode="json"),
        "resolved": {
            "row_count": len(chain),
        },
        "warnings": [],
        "rows": chain.to_dict(orient="records"),
    }


# Composition root (app startup): build the provider from the environment, inject it downstream.
provider = PandasLocalFileProvider(exchange_code="DERIBIT", data_path="/path/to/data")
result = build_chain_snapshot(provider, "BTC", RequestParameters())
```

A FastAPI handler (a tier-4 adapter) maps the request to `params` and calls this function with the
injected `provider`. The same library powers both the notebook user (who may also use `flow`) and the
server caller (who does not); a future service wraps this in a typed request/result with explicit
failure modes, without changing the domain computation.
