# Agent: alphavar engineer

**Inherits** [akmon/roles/engineer](../../akmon/roles/engineer.md) — that file is
the source of mission, scope, pipeline, requirements, guardrails, and definition of done.
This charter adds **only alphavar specifics**; it does not restate the role.

## What this agent works on (alphavar)

- **Code:** `src/alphavar/{core,io,options}/` — implement the design from the
  [architect](../architect/README.md) agent against the R# in
  [`docs/dev/ARCHITECTURE_REQUIREMENTS.md`](../../../docs/dev/ARCHITECTURE_REQUIREMENTS.md).
- **Tests:** `tests/` — unit and integration; a behaviour change without a test is
  incomplete.
- **Build tools (reuse, don't re-script):** [`../../tools/`](../../tools/) —
  `exchange_fixtures` (record/trim hermetic exchange fixtures), `data_migration`
  (verify + fix stored data). Local skills: [`../../skills/`](../../skills/).
- **Process rules:** D# in [`_aitna/DEVELOPMENT_REQUIREMENTS.md`](../../DEVELOPMENT_REQUIREMENTS.md);
  verification in [`_aitna/D2_VERIFICATION.md`](../../D2_VERIFICATION.md).

## Profiles in effect

- [`quant`](../../akmon/profiles/quant.md) — numerics conventions apply to any code
  touching pricing, smiles, Greeks, or risk.

## Pipeline (alphavar specifics over [code-flow](../../akmon/pipelines/code-flow.md))

- **Implement** reuses existing abstractions (the provider pattern in
  `src/alphavar/io/`, the column dictionary in `src/alphavar/*/dictionary/`) rather than
  duplicating them.
- **Test** follows the project's `tests/unit/...` layout mirroring `src/`.
- **Verify** — any change to math (pricer/smiles), DataFrame shape, or the column
  dictionary requires owner verification per D2; green tests are necessary, not
  sufficient.
- **Close** marks the task done in [`../../TASKS.md`](../../TASKS.md) only after owner
  verification; reusable insight → [`../../memory/`](../../memory/) for the learn loop.

## Hand-off boundary

A material design gap (new architecture, a changed requirement) goes back to the
[architect](../architect/README.md) agent as a task — this agent does not improvise
load-bearing architecture.
