# 0004 — `lib` / class contracts and architecture remediation

- **Status:** Accepted
- **Owner:** akuminov@gmail.com
- **References:** R1, R3, R4.4, R8 (`ARCHITECTURE_REQUIREMENTS.md`); D2, D7
  (`DEVELOPMENT_REQUIREMENTS.md`); backlog T41-T44; ADR 0003 (result-chain contracts).

## Context

An architecture review of `alphavar.options` found that the target architecture is sound:
domain-first layout, stateful facade classes, pure `options/lib` functions, provider-based I/O,
term registry, pandera schemas, and the emerging `Disc`/`flow` producer surface.

The weak spots are boundary drift:

- `options/lib` mostly stays isolated, but the reference sidecar storage adapter currently reads
  and writes files from inside `lib`.
- Some facade methods hide material policy such as lazy load, reference reattachment, and price
  null dropping. Those policies can be valid, but they must be named as facade contracts rather
  than treated as generic pure transforms.
- Several `lib` functions still use pandas-specific mutation patterns (`inplace=True`,
  `groupby.apply`, temporary helper columns), which makes the Polars target harder.
- The data model is mid-migration from legacy enum/code vocabulary to `Term`/`OptionsTerm`,
  `StrEnum`, category schemas, and schema-pinned interchange frames.
- The result-chain work established the right direction, but not every capability area exposes
  explicit producer contracts yet.

## Decision

Lock the following remediation principles as architecture:

1. **`lib` is pure computation.** It contains DataFrame transforms, reductions/producers,
   factories, and numerical kernels. It does not contain storage adapters. Reference split/SCD/as-of
   logic belongs in `lib`; reference read/write belongs outside `lib`.
2. **Inputs are explicit.** A `lib` function or domain class consumes the frame/result/model it is
   given. It never invokes an upstream producer implicitly. A user, AI agent, or `alphavar.flow`
   may assemble upstream producers from the self-described contracts.
3. **Facade classes are bindings.** They own `OptionsData` state, choose frames, call `lib`, validate
   boundaries, and assign results. They should not accumulate computational business logic.
4. **Schemas are the chain contract.** Entity frames and result/interchange frames are pinned by
   pandera schemas or result classes exposing `interchange_schema`. Compatibility belongs there, not
   in an assembler.
5. **New DataFrame code is Polars-aware.** New/touched `lib` code avoids caller-owned in-place
   mutation and hard-to-port pandas idioms unless there is an explicit, tested reason.

## Consequences

- Existing transitional code is not automatically wrong, but each exception needs a remediation task.
- Architecture cleanups that touch DataFrame logic, math, schema semantics, storage layout, or public
  contracts are D2-gated and stay pending owner verification until reviewed.
- The product surface is the `lib` + class contract. `flow` remains one consumer of those contracts,
  not the owner of the domain semantics.

## Rollout

1. Move reference sidecar persistence out of `options/lib/reference/_store.py` into an I/O/storage
   adapter, keeping split/SCD/as-of in `lib`.
2. Add an architecture guard that catches forbidden `lib` dependencies and I/O APIs.
3. Register/pin producer contracts for the important options capability areas: enrichment, chain,
   pricer, validation, payoff/risk, and forecast outputs.
4. Harden DataFrame contracts: remove pandas mutation hotspots when touched, fix known facade
   dependency-planning bugs, and define validation boundaries.
5. Finish the data model cleanup: complete legacy enum/code retirement where it affects schemas,
   exchange normalization, analytics, and stored parquet migration.

Each rollout item is tracked in `_aitna/TASKS.md` and detailed in
`_aitna/design/architecture-remediation.md`.
