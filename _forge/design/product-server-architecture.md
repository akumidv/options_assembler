# Design — product and server architecture

`alphavar` is a quant library first, but its product architecture must also support server-side
solutions: APIs, workers, batch jobs, dashboards, agent runtimes, and downstream applications. The
architecture target is one domain core with several entrypoint layers, not separate implementations
for research and production.

The binding requirements are `ARCHITECTURE_REQUIREMENTS.md` R9 and R10. This document is the rollout
backlog for those requirements.

All tasks below touch architecture, public contracts, or DataFrame/result semantics. Per D2, they are
not "done" until the owner verifies the architecture and any changed data contracts.

> **The server product is a deferred future task — do not build it now.** R10 is a *compatibility
> constraint on present work*, not a build order: keep the quant core shaped so the server layer can be
> added later without rework. So **A1 (declare tiers)** and **A3 (result/interchange contracts)** are
> active now — they are the compatibility surface and also serve quant users — while **A2 (service
> layer)** and **A4 (deployment boundaries)** are held as future work and built only when the server
> product is actually scheduled. The `flow`↔`services` boundary and the ETL framing are settled in
> [ADR 0005](../../docs/dev/decisions/0005-flow-services-etl-boundaries.md).

## A1. Define product API tiers

Goal: make the public surface explicit for quant users and server builders.

Current issue: the code already has pure `lib` functions, the `Option` facade, producer contracts,
and early `flow`, but the project does not yet declare which surface is stable for which user:
notebook/research users, service/application code, CLI/batch, or web adapters.

Plan:
1. Document the API tiers in user-facing and dev-facing docs:
   - core/domain API: pure functions, typed entities, schemas, result classes;
   - research API: `Option` and component facades over `OptionsData`;
   - service API: use-case functions/classes that assemble providers, validation, computations, and
     serializable outputs;
   - adapters: FastAPI, workers, CLIs, notebooks, and agents outside the domain core.
2. Mark the intended stability level of each tier.
3. Add import/export guidance so users do not reach into private modules for common workflows.
4. Keep `Option` as the research facade, not the only product API.
5. Place `flow` as an in-process assembly mechanism beside the tiers (not a tier of its own) and
   record the `flow`↔`services` direction per [ADR 0005](../../docs/dev/decisions/0005-flow-services-etl-boundaries.md).

Acceptance:
- README/docs describe the tiers with one runnable quant example and one server-embedding example.
- Public exports match the documented surface.
- Architecture review confirms the tiers preserve R1/R2/R3 boundaries.

## A2. Design server-ready service layer

> **Deferred — future server task.** Do not build now; the plan below is the target the present quant
> core must stay compatible with (per the note above and [ADR 0005](../../docs/dev/decisions/0005-flow-services-etl-boundaries.md)).

Goal: add a framework-neutral service/use-case layer over the quant core.

Current issue: server code would currently have to orchestrate providers, validation, enrichment,
pricing, forecasting, and serialization directly through `Option` or low-level functions. That works
for notebooks, but it spreads product workflows across API handlers or workers.

Plan:
1. Define `alphavar.services` as an application/use-case layer with no FastAPI, Celery, database, or
   web-framework dependency.
2. Start with options workflows that match real product actions:
   - build an option-chain snapshot;
   - price/imply IV for a chain;
   - fit a smile/surface slice;
   - produce a forecast distribution;
   - compute payoff/risk outputs.
3. Inputs are typed request models or explicit function parameters that express user intent:
   `asset_code`, strategy legs/specs, `as_of`, timeframe, horizon, model selectors, pricing/risk
   parameters, and output options. They do not expose provider classes, storage paths, raw venue
   symbols, DataFrame column names, `flow` plans, or producer chains.
4. Services are deterministic by workflow: each service is a fixed, named use-case with explicit
   domain steps. Request values choose parameters and documented policy options, not the computation
   graph. Any resolution such as `nearest` expiration or `atm` strike is part of the service contract.
5. Services receive providers/storage/cache handles, clocks, settings, and strategy-resolution policy
   by dependency injection. They do not instantiate exchange clients, read environment variables, or
   own runtime policy.
6. Service functions orchestrate explicit producer steps; they do not hide domain computation inside
   HTTP or worker adapters. FastAPI handlers only parse transport input, enforce HTTP/auth policy, map
   errors to status codes, and call the service.

Acceptance:
- At least one server-ready workflow exists without importing a web framework.
- The workflow can be called from a notebook, a CLI, and a hypothetical API handler with the same
  input/output contract.
- Validation and failure modes are explicit and test-covered.
- User-facing requests can be expressed as product intent (`asset_code` + strategy/params) and the
  service response includes resolved inputs, context/provenance, warnings, and typed errors.

## A3. Pin server serialization and result contracts

Goal: make every server-facing output stable, typed, and portable.

Current issue: many results are still raw DataFrames or class-layer objects. That is fine inside
research code but weak for server APIs, caches, jobs, and downstream consumers.

Plan:
1. Inventory outputs used by service workflows: chains/desks, validation reports, pricing/IV frames,
   smile/surface fits, forecast distributions, payoff/risk frames.
2. For each server-facing output, define one contract:
   - pandera schema or typed result class for in-process computation;
   - JSON/table serialization shape for API/job boundaries;
   - error/report shape for validation failures.
3. Reuse `Term`/`OptionsTerm`/`ResultTerm` names in every serialized table.
4. Keep DataFrame-engine details out of the serialized contract.
5. Include reproducibility context in every server-facing result: `asset_code`, provider/exchange or
   source identity, `as_of`/data timestamp or snapshot id, model/policy identifiers, request
   parameters, resolved strategy/legs when applicable, warnings, and typed validation/error details.
6. Add compatibility tests for contract shape, required fields, enum spellings, nullability, and the
   resolved-input/context fields.

Acceptance:
- Server-facing outputs have explicit interchange schemas or result classes.
- JSON/table representations are documented and tested.
- No adapter layer needs to infer domain semantics from arbitrary DataFrame columns.
- Server responses are auditable: a caller can see what request was resolved, what data/context was
  used, and which warnings/errors apply.

## A4. Document deployment boundaries for server products

> **Deferred — future server task.** Do not build now; produced when the server product is scheduled.
> The dependency-direction and ETL framing it will document are already settled in
> [ADR 0005](../../docs/dev/decisions/0005-flow-services-etl-boundaries.md).

Goal: keep production runtime concerns outside the quant core while making integration obvious.

Current issue: the library has ETL and provider infrastructure, but there is no product-level map for
where server concerns belong: API authentication, request validation, background jobs, storage,
caching, scheduling, observability, and secrets.

Plan:
1. Document what belongs inside `alphavar`:
   - domain logic;
   - provider/exchange abstractions;
   - framework-neutral services;
   - schemas and result contracts.
2. Document what belongs outside `alphavar` or in thin adapters:
   - FastAPI/HTTP routing;
   - auth and user/session policy;
   - job queues and schedulers;
   - database-specific repositories;
   - deployment config and secrets.
3. Provide a reference integration sketch for an API handler and a worker job.
4. Document the composition root: it resolves `asset_code` to provider/source, storage/cache, clock,
   feature flags, and strategy-resolution policy, then injects them into services.
5. Align ETL, provider, and service boundaries so a server can share storage without bypassing
   provider contracts.

Acceptance:
- Docs show the allowed dependency direction from API/worker adapters into services into core.
- No service-layer design requires a concrete web framework, database, or scheduler.
- Security expectations from R7 remain the runtime boundary for adapters.
- FastAPI examples keep domain/use-case code framework-neutral and place provider construction in the
  composition root, not inside the service.
