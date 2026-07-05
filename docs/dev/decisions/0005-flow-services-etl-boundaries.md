# 0005 — `flow` assembles, `services` contracts; ETL is a job-shaped service

- **Status:** Accepted (architecture D2-verified by owner)
- **Owner:** akuminov@gmail.com
- **References:** R1.1 (lib contract shapes / resolver one level up), R3 (facade composition),
  R4.4 (schemas bind to registry), R6 (ETL isolation), R9 (product API tiers), R10 (server-ready
  contracts) — `ARCHITECTURE_REQUIREMENTS.md`; D2 (owner verifies architecture). ADR 0003 (defines
  `alphavar.flow` as the demand-driven assembler), ADR 0004 (`lib`/class contracts). Backlog A1-A4
  (product/server architecture), T28 (unify provider ↔ exchange path).

## Context

A review of R9/R10 surfaced two unresolved placements that block the A1-A4 rollout:

1. **`flow` vs `services` overlap.** R1.1 names `alphavar.flow` (with a user script or an AI agent) as
   the assembler that resolves "provide-or-compute" one level above the domain classes. ADR 0003
   defines `flow` as the demand-driven planner over self-describing producer contracts. But R9 does
   **not** place `flow` in its four-tier model, and R10 names **services** as the layer that
   "orchestrates explicit producer steps". Two concepts now claim the role of *the thing that
   assembles producer steps* — `flow` (R1.1/0003) and `services` (R10) — with no stated relationship.
2. **ETL framing collides.** R6 treats ETL (`options/etl/`) as its own I/O-orchestration layer
   (exchanges via `AbstractExchange`, a fixed storage layout, `AbstractMessanger` notifications). R10
   says long-running work "(ETL …) is modeled as a service call … queues/schedulers are adapters
   around that service." It is unstated whether ETL *is* a service, an adapter, or a third thing.

Both gaps are about the same axis: **what assembles/invokes work, vs. what computes it.** Resolving
that axis once settles both.

## Decision

### 1. `flow` is a mechanism; a `service` is a contract — they do not compete

- **`flow` assembles.** `alphavar.flow` is the in-process, demand-driven **assembler/planner**: from
  the self-describing producer contracts (derived from signatures + return schemas, ADR 0003) it
  plans and runs a DAG of producer steps. It is one of **three interchangeable assemblers** of the
  same contracts — `flow`, a developer in plain code, or an AI agent (ADR 0003). `flow` carries **no
  stability or serialization guarantee of its own**; compatibility lives in the producer contracts
  (R4.4), not in `flow`.
- **A `service` contracts.** A service (R9 tier 3 / R10) is a **named, coarse-grained product
  use-case** with a typed request model and a typed, serializable result + explicit error/report
  shape. The service *contract* is the stable server surface; *how* it assembles its steps is an
  implementation detail.
- **Relationship — services call the library directly; `flow` is the research/agent assembler.** A
  service is **deterministic**: it implements its use-case by **explicit producer/facade steps in plain
  code** (fixed, parameter-driven), possibly via component classes — it does **not** route through
  `flow`. `flow` is the demand-driven assembler for the other two consumers, **researchers and AI
  agents**, who explore the chain off the same contracts. The dependency direction holds regardless and
  `flow` never imports `services`:

  ```
  adapters → services → facade/lib → providers/io
  researchers / agents → (flow | facade) → facade/lib → providers/io
  ```

  `flow` sits **beside** the tiers as the research/agent assembly mechanism, never above `services`.
  `flow` must not import `services`. (Architecturally a service *could* call `flow`, but the product
  decision is that it does not — services stay explicit and deterministic.)
- **A service owns its boundary.** A service must expose typed request/result/error (R10) and must
  **not** leak a raw `flow.Plan`, `flow.RunRecord`, or an unpinned DataFrame across its boundary.
  Inside a process, research users and AI agents may use `flow` (or the facade) directly without a
  service; the service is the contract you reach for when the boundary is a server/API/job/cache.

  > **One-line rule:** *`flow` assembles, `services` contracts.* The same producer contracts feed
  > both; neither is the other.

### 2. ETL is a job-shaped service; the scheduler is an adapter; R6 keeps the internals

The collision dissolves by separating **what computes** from **when it runs**:

- **`options/etl/` stays the I/O-orchestration core, governed by R6 unchanged** — exchanges only via
  `AbstractExchange`, the agreed storage layout, `AbstractMessanger` for notifications. R6 is the
  invariant for the *internals* of an ETL job.
- **An ETL run is a write-side, job-shaped service** under R9/R10: typed inputs (what to fetch, time
  range, the target **storage handle**), a typed result/report output (what was written, counts,
  per-asset failures), and **dependency injection** of the exchange, storage, messenger, and clock
  handles. It reads no secrets and instantiates no concrete exchange itself (R10); the composition
  root builds those from the environment (R7) and injects them.
- **The scheduler/queue is an adapter** around that service (apscheduler today, behind the `etl`
  extra): it owns *when* to run, not *what* is computed (R10). Swapping apscheduler for a queue
  changes no ETL computation.
- **Generalization.** The same shape covers the other long-running jobs R10 lists — forecast
  batches, surface calibration, portfolio/risk jobs: each is a job-shaped service (typed in/out, DI),
  with the scheduler/queue as its adapter. ETL is the existing **write-side** instance; the compute
  jobs are **read-side** instances of one pattern.

So ETL is **not** a fourth concept: it is a service (write/job-shaped) whose computation core already
exists as `options/etl/` under R6, with the scheduler as its adapter.

## Consequences

- **A1** gains the missing tier placement: `flow` is documented as an assembly mechanism spanning
  Core/Research (for researchers and agents), not a tier; services are tier 3; the consumer→assembler
  map is recorded in [`api-tiers.md`](../api-tiers.md).
- **A2** can design `alphavar.services` without re-litigating whether it competes with `flow`: a
  service is a typed, deterministic contract implemented by **direct library calls** (facade/`lib`,
  possibly classes), **not** via `flow`. The first ETL service is the concrete write-side example; a
  chain/price/fit/forecast service is the read-side example.
- **A4** gets a clean ETL boundary: scheduler = adapter, ETL job = service, `options/etl/` = R6 core
  — the deployment map (queue/scheduler outside, computation inside) follows directly.
- **R6 is untouched as an invariant**; this ADR only fixes how an ETL run is *invoked and scheduled*,
  not how it fetches/normalizes/persists. No data-shape or storage-layout change → no migration.
- `flow`'s implementation remains **non-priority** (ADR 0003): this ADR places it in the model but
  does not pull its build-out forward. The product is still the lib/class + producer contracts.
- **The server side is not built now.** `alphavar.services`, adapters, and a deployed product are a
  **deferred future task** (A2/A4). This ADR settles their *shape* so the present quant core stays
  compatible — it is a constraint to design against, not a signal to implement. Build the
  quant/research path; only keep the door open for the server path.

## Rollout (phased; each gates on `uv run pytest`; architecture owner-verified per D2)

1. **Docs-only (no code):** fold this placement into R9/R10 (one cross-reference each: R9 names
   `flow` as the in-process assembler beside the tiers; R10 names ETL as a job-shaped service +
   scheduler-as-adapter, pointing here). Update A1/A2/A4 plans to reference this ADR. No behavior
   change.
2. **Service contract seam (with A2/A3):** when `alphavar.services` lands, the first write-side
   (ETL) and one read-side (chain/forecast) use-case adopt the typed request/result + DI shape; the
   apscheduler entrypoint becomes a thin adapter calling the ETL service.
3. **flow alignment (non-priority):** when `flow` build-out resumes, confirm `flow` serves researchers
   and agents off the producer contracts and carries **no `services` import** (an architecture-guard
   test, alongside the R1/T41 boundary guard). Services stay on direct library calls — they are not
   expected to route through `flow`.

Until phase 2, the placement is documentation: existing `options/etl/` and the `flow/` V1 prototype
stay as-is, now with a defined target role.
