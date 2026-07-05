# Design — architecture remediation backlog

This backlog turns the architecture review into implementation tasks. The target invariants
are in `docs/dev/ARCHITECTURE_REQUIREMENTS.md` R1/R3/R4/R8 and
`docs/dev/decisions/0004-lib-class-contract-boundaries.md`.

All tasks below are architecture / DataFrame-contract work. Per D2, they are not "done" until
the owner verifies the changed boundary, schema, storage layout, or DataFrame logic.

## T41. Enforce the pure `lib` boundary

Goal: make `options/lib` a pure computation layer again.

Current issue: `options/lib/reference/_store.py` is a storage adapter inside `lib`; it creates
directories, writes JSON, and reads/writes parquet. The reference idea is valid, but persistence
belongs outside `lib`.

Plan:
1. Move reference sidecar persistence (`_asset.json`, `_meta.parquet`) to `io/provider`,
   `options/etl`, or a dedicated storage adapter outside `options/lib`.
2. Keep pure reference functions in `options/lib/reference`: `split_reference`,
   `apply_reference`, SCD-2 `append_on_change`, `as_of`, `join_reference_asof`.
3. Update `AbstractFileProvider.load_reference` and ETL callers to use the new adapter.
4. Add a lightweight architecture guard test that fails if `options/lib` imports facade modules,
   provider/exchange modules, or uses file/network I/O APIs.

Acceptance:
- `rg` / a guard test shows no `alphavar.io`, facade imports, `read_parquet`, `to_parquet`,
  `open`, `os.makedirs`, or network clients under `src/alphavar/options/lib`.
- Existing reference split/round-trip/as-of tests still pass.
- D2 owner verification covers the storage boundary and on-disk layout.

## T42. Pin producer contracts for options capability areas

Goal: make the class/lib contracts usable by humans, AI agents, and `alphavar.flow` without
hardcoding domain knowledge in the assembler.

Current issue: `Disc` / `flow` exists for the V1 forecast price slice, but enrichment, chain,
pricer, validation, and payoff/risk are still mostly class-only or ad-hoc function calls.

Plan:
1. Inventory each public capability method and its pure `lib` function: enrichment, chain,
   pricer, validation, analytic risk/payoff, analytic price/time value, forecast.
2. Classify each as Shape 1 transform (`df -> df` / row-aligned Series) or Shape 2 producer
   (`df -> tidy frame` / typed result).
3. Add or tighten return annotations and schemas for outputs intended as chain inputs.
4. Register producer contracts where useful; registration supplies only the exceptions that
   the function signature cannot state.
5. Keep domain classes as bindings over `OptionsData`; do not add upstream auto-resolution
   to the classes.

Acceptance:
- `core.disc.catalog()` can describe the important options producer surface.
- Each registered producer has explicit inputs, params, output schema or interchange renderer.
- No class method silently invokes an unrelated upstream producer; dependencies are explicit
  enrichment/producer steps.
- D2 owner verification covers any public contract or schema semantics change.

## T43. Harden DataFrame transform contracts

Goal: make DataFrame transformations predictable, testable, and Polars-portable.

Current issues:
- Some `lib` functions mutate caller-owned frames in place.
- Some functions rely on hard-to-port pandas patterns such as `groupby.apply`, implicit index
  alignment, temporary helper columns, and `inplace=True`.
- `OptionsEnrichment.enrich_options(force=True)` has a likely nested-list `drop(columns=...)`
  bug and dependency ordering is not a proper graph planner.

Plan:
1. Fix the enrichment `force=True` drop bug and add a focused regression test.
2. Replace the enrichment dependency-order helper with a deterministic graph/topological planner.
3. For touched `lib` transforms, return a new frame/series instead of mutating inputs in place.
4. Prioritize hotspots used in chains: intrinsic/time value, ATM/ITM/OTM, time-value series,
   chain payoff aggregation, normalization/resampling.
5. Preserve existing math behavior unless the owner explicitly approves a changed formula.

Acceptance:
- Focused tests cover `force=True`, dependency ordering, and unchanged output for representative
  transforms.
- New/touched transforms are data-first, no caller-owned in-place mutation, and avoid new
  `groupby.apply` / implicit-index dependencies unless explicitly justified.
- D2 owner verification covers every changed DataFrame operation or math expression.

## T44. Finish schema and vocabulary migration

Goal: make the data model internally consistent: `Term`/`OptionsTerm`, `StrEnum` values,
category schema dtypes, and schema-pinned interchange frames are the single contract.

Current issue: the target model is documented and partly implemented, but legacy
`EnumCode`, `.code`, `.value`, `OptionsType`, and old short-code assumptions still appear in
normalization, analytics, tests, and migration compatibility paths.

Plan:
1. Audit remaining legacy vocabulary uses and split them into: live contract, migration shim,
   exchange wire mapping, or test fixture.
2. Move true live contracts to `Term`/`OptionsTerm` + `StrEnum` values and pandera category
   schemas.
3. Keep exchange API spellings and legacy parquet spellings only at boundaries/migration shims.
4. Pin missing result/interchange schemas, especially for smile/surface and payoff/risk frames.
5. Update tests to assert canonical values while retaining explicit migration tests for legacy
   inputs.

Acceptance:
- Live analytics and schemas use canonical terms/values.
- Legacy code is isolated in migration or exchange-boundary modules.
- Missing chain/result schemas are pinned before downstream consumers rely on them.
- D2 owner verification covers data schema/column semantics and stored-data migration impact.
