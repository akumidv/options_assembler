# 0001 — InstrumentKind/ContractKind as the canonical instrument kind everywhere

- **Status:** Accepted
- **Owner:** akuminov@gmail.com
- **References:** R2.2, R4.1, R4.3, R4.4, R4.5 (`ARCHITECTURE_REQUIREMENTS.md`); D2, D4
  (`DEVELOPMENT_REQUIREMENTS.md`); backlog T23.6 (data migration).

## Context

The instrument-kind concept is currently split across incompatible encodings:

- **Legacy `AssetKind`** (`EnumCode`, plural values `"options"`/`"futures"` + short codes
  `o/f/s`) is used for the dataframe `asset_type` column (stores the *code*), for the
  file/dir layout (`DERIBIT/BTC/options/…`), and across provider/ETL APIs.
- **`DeribitAssetKind`** mixes layers: `OPTION` inherited the internal `"options"`,
  while `FUTURE="future"` and `*_combo` are venue spellings — which forced the
  `_DERIBIT_API_KIND` shim (R2.2) and broke updates→history migration (the raw update
  store uses singular venue dirs, `EtlHistory` filtered by the plural internal value, so
  nothing matched).
- R4.5 already designs the target — `InstrumentKind` (`StrEnum`, singular
  `option`/`future`/`spot`), `ContractKind` (`vanilla`/`combo`/…), `AssetClass` — with
  columns in the plain `Term` registry (R4.3), no `.code`/`.value` duality, and combos as a
  separate axis. Only `options/schemas/_schemas.py` adopts it so far.

## Decision

Adopt R4.5 **fully and canonically everywhere** — code, dataframe columns, file/dir
paths, and the on-disk history layout — and **retire** legacy `AssetKind` (as a kind) and
`DeribitAssetKind`'s "our-kind" role:

1. `InstrumentKind` (StrEnum, **singular**) is the one canonical instrument kind. The same
   token appears in the `instrument_kind` column, in file paths, and matches the venue API
   (`kind="option"`). No plural/singular or value/code duality (R4.5).
2. **Combos are not a kind:** Deribit `option_combo`/`future_combo` →
   `instrument_kind ∈ {option, future}` + `contract_kind=combo` (`ContractKind`). They are
   **not** migrated into the kind-partitioned history (not `vanilla`).
3. The **venue→(InstrumentKind, ContractKind)** mapping lives on the exchange/provider
   (R2 provider pattern) and is exposed through the `AbstractExchange` contract.
   `DeribitAssetKind` survives only as the **venue-wire encoding** of the raw snapshot
   (R2.2: project enum ≠ API parameter).
4. **Paths use the singular `instrument_kind` value.** The raw **update** store keeps the
   venue-native tokens (for vanilla these already equal the canon `option`/`future`);
   normalization to the canon happens at the updates→history migration boundary.
5. Retire `EnumCode` short codes for these axes — `StrEnum` value + the schema `category`
   dtype (R4.4) give readability and compactness (R4.5).

"Canon everywhere" was chosen over a half-measure (keep history plural, map only at the
boundary) because the half-measure preserves the very name duality this decision removes.

## Consequences

Existing parquet stores short codes and the plural dir layout, so **stored data must be
migrated**: rename history dirs `options→option`, `futures→future`, and expand column
codes `o/f/s`→values. Tooling:
- `src/alphavar/core/migration/legacy_parquet.py` — parquet **contents** (column renames +
  value expansion); the only module that knows the legacy names/codes. Already exists.
- a **`tools/`** data-layout migrator — the **dir** rename + a wrapper over
  `legacy_parquet`, runnable from a console by the owner (see `tools/README.md`).

## Rollout (phased; each phase gates on `uv run pytest`)

1. **Kind type + venue mapping (exchange).** `AbstractExchange` contract +
   Deribit/MOEX maps venue→(InstrumentKind, ContractKind); drop `_DERIBIT_API_KIND`.
2. **Paths + ETL.** Provider path building and `EtlHistory` use the singular
   `InstrumentKind` value; combos skipped from kind-history. Update save stays venue-native.
3. **Columns + schemas + analytics.** `asset_type`(code) → `instrument_kind`(value),
   pandera schemas, analytics; retire `AssetKind`/`AssetType`-as-kind across the codebase.
4. **Data migration + tests.** Run the `tools/` migrator on the local store; turn the
   update/migration tests to the singular venue layout; full suite green.

Phase 3 touches DataFrame/math/schema code → **owner-verified per D2** before "done".
