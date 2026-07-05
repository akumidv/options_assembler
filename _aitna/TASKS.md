# Tasks — alphavar

Implementation backlog. Format: [akmon tasks convention](akmon/pipelines/tasks.md) —
one line per task, **detail by reference**, no dates, `done` → [TASKS_ARCHIVE.md](TASKS_ARCHIVE.md).

> Constraints: [ARCHITECTURE_REQUIREMENTS.md](../docs/dev/ARCHITECTURE_REQUIREMENTS.md) (R0…R10) ·
> [DEVELOPMENT_REQUIREMENTS.md](DEVELOPMENT_REQUIREMENTS.md) (D1…D7). Every task: `pytest` +
> `ruff` green; any math/DataFrame/architecture change is **not done until owner-verified**
> ([D2 ledger](D2_VERIFICATION.md)).

## Status

Suite green, ruff clean. Forecast (T27) is code-complete & D2-pending; result-chain V1 (T37)
landed. Active focus: the **dataset storage/identity foundation** (T45→T46, ADR 0006) leads
architecture remediation — it precedes the vocabulary/contract pinning (T44→T42) that would
otherwise be frozen on the old identity model. Also ongoing: D2 verification, forecast, server tiers.
Top-down architecture review (N1) added A5–A7 / C1–C2; A5 (io neutrality) must land before spot/bond.

## Active / blocked / deferred

- A5 · restore `io` domain-neutrality · active · architect · move `Timeframe`/`AssetMeta`/generic normalization to `core`; domain-owned (or kind-generic) provider port — **before spot/bond (T31/T32)** · [review N1](artifacts/architecture-review-top-down.md) (local, untracked)
- A6 · pin public package surface · active · architect · tier-1 exports/`__all__` (R5/R9, api-tiers); rename `io/messanger`→`messenger` + `expiation_date` typo while API is unstable · [review N1](artifacts/architecture-review-top-down.md) (local, untracked)
- A7 · facade loading/state contract · active · architect · explicit lazy-load semantics for `df_hist` (side-effecting property vs `load()`); `select_chain` provider-vs-history source policy · [review N1](artifacts/architecture-review-top-down.md) (local, untracked)
- C1 · facade hygiene · active · engineer · use injected provider's column defaults (not `AbstractProvider` class attrs); drop `load_reference` duck-typing; delete dead commented code; annotate public surfaces · [review N1](artifacts/architecture-review-top-down.md) (local, untracked)
- C2 · `Disc.register` validation · active · engineer · validate `consumes` against the signature at `register`; guard the params-slice convention (registry assembly strategy stays result-chain Phase 2) · [review N1](artifacts/architecture-review-top-down.md) (local, untracked)
- T27 · forecast capability area · active · engineer · D2-verify math; build factor-conditional + analogue models · [design](design/forecast/README.md)
- T40 · pin interchange schemas for smile/surface · active · engineer · scalar-less θ output needs pinned interchange schemas · [design](design/forecast/README.md)
- T41 · enforce pure `lib` boundary · active · architect/engineer · move reference storage out of `lib`; add architecture guard · [design](design/architecture-remediation.md)
- T45 · dataset storage layout v2 · active · architect/engineer · **identity/storage foundation — precedes T44/T42** (they pin the vocabulary/reference shape this reshapes); replace `{exchange}/{asset}` + `_asset.json`/`_meta.parquet` with dataset→exchange→asset YAML/reference tables · [ADR 0006](../docs/dev/decisions/0006-dataset-storage-and-resolution.md)
- T46 · dataset resolver · active · architect/engineer · after T45; discover datasets from filesystem/YAML, parse `{exchange}:{asset}` shorthand, build generated cache, resolve asset/exchange/provider/dataset to provider context · [ADR 0006](../docs/dev/decisions/0006-dataset-storage-and-resolution.md)
- T44 · finish schema/vocabulary migration · active · architect/engineer · after T45 — absorb ADR 0006 identity fields (`listing_id`/`economic_asset_id`/`contract_id`); canonical Terms/StrEnum/schemas; isolate legacy shims · [design](design/architecture-remediation.md)
- T42 · pin options producer contracts · active · architect · after T44/T45; enrichment/chain/pricer/validation/risk contracts for users/AI/flow · [design](design/architecture-remediation.md)
- T43 · harden DataFrame transform contracts · active · engineer · parallel — no identity/reference coupling; fix enrichment force/drop; graph deps; reduce in-place pandas hotspots · [design](design/architecture-remediation.md)
- T47 · provider/ETL storage migration · active · engineer · after T45/T46 (owner-run data migration); make file provider and ETL consume resolved dataset paths and write target quote/reference layout · [ADR 0006](../docs/dev/decisions/0006-dataset-storage-and-resolution.md)
- T48 · economic asset catalog · active · architect/engineer · last of the block (additive/optional); `_catalog/assets.yaml`; resolver-only `economic_asset_id` lookup returning all datasets/listings related to a concrete symbol · [ADR 0006](../docs/dev/decisions/0006-dataset-storage-and-resolution.md)
- A3 · server result contracts · active · pin serializable output/error shapes (compat surface; overlaps T40/T42) · [design](design/product-server-architecture.md)
- A2 · server service layer · deferred · future server task — keep core compatible, don't build now (ADR 0005) · [design](design/product-server-architecture.md)
- A4 · deployment boundaries · deferred · future server task — direction/ETL framing already in ADR 0005 · [design](design/product-server-architecture.md)
- T29 · surface fitting model (pricer) · active · architect · joint SVI-surface calibration with calendar no-arb · [design](design/domains-roadmap.md)
- T30 · sparse/live smile-shift · active · architect · low-DoF additive-`w` shift of a prior smile; lock open Qs · [design](design/pricer/smile-shift.md)
- T36 · implement planned `knowledge/` concepts · active · engineer · full Greeks + Sortino, then their USAGE skills · [design](design/domains-roadmap.md)
- T28 · unify provider ↔ exchange data path · blocked · architect · one canonical-normalization contract; needs an ADR · [design](design/domains-roadmap.md)
- T31 · spot domain · deferred · architect · `alphavar.spot` over the neutral core · [design](design/domains-roadmap.md)
- T32 · bonds domain · deferred · architect · fixed-income + yield-curve layer · [design](design/domains-roadmap.md)
- T33 · portfolio management · deferred · architect · cross-domain position book + VaR/CVaR · [design](design/domains-roadmap.md)
- T35 · risk layer · deferred · architect · VaR/CVaR/stress from forecast distributions · [design](design/domains-roadmap.md)
- T23.6c · stored-parquet column migration · deferred · engineer · owner-run; tooling ready · [archive](TASKS_ARCHIVE.md)
- T25 · flip `slim_series` on · deferred · engineer · owner-run; shrinks stored files · [archive](TASKS_ARCHIVE.md)

## Done

See [TASKS_ARCHIVE.md](TASKS_ARCHIVE.md).
