# Decision records (ADR)

Short records of **accepted architectural decisions** and the reasoning behind
them. They complement the requirement docs (`../ARCHITECTURE_REQUIREMENTS.md` R#,
`../../../_aitna/DEVELOPMENT_REQUIREMENTS.md` D#): the R#/D# rules say *what the invariant is*; an ADR
records *a decision to act* (a migration, a retirement, a phased rollout) and why, so the
choice isn't re-litigated later.

Convention:
- One file per decision: `NNNN-kebab-title.md` (zero-padded, monotonic).
- Header: `Status` (Proposed / Accepted / Superseded by NNNN), `Owner`. No dates — git
  history is the timeline (see `_aitna/memory/no-dates-in-planning-docs.md`).
- Body: Context → Decision → Consequences (incl. data/migration impact) → Rollout →
  References (R#/D#, backlog ids).
- Keep it short; link to R# rules instead of repeating them.

## Index
- [0001 — InstrumentKind/ContractKind as the canonical instrument kind everywhere](0001-instrument-kind-canon.md)
- [0002 — Forecast as a model factory: target × process × engine](0002-forecast-model-factory-axes.md)
- [0003 — Composable result-chain: calculations feed calculations](0003-composable-result-chain.md)
- [0004 — `lib` / class contracts and architecture remediation](0004-lib-class-contract-boundaries.md)
- [0005 — `flow` assembles, `services` contracts; ETL is a job-shaped service](0005-flow-services-etl-boundaries.md)
