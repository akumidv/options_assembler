# Rejected branches & dead-ends (with why + revisit-if)

> Hub: [`README.md`](README.md). Rejected ideas are kept here, **not deleted** — a later pass under
> different constraints may revive one. Each: what it was, **why dropped**, and **revisit-if**.

## R1. Input params as frame columns (provenance columns)
- **Idea:** carry `target/model/horizon/seed/...` inside the result DataFrame as per-row columns, so
  the frame is self-describing.
- **Why dropped (owner, 2026-06-20):** redundant (a scalar repeated on every row); conflates *data*
  (rows) with *description of the calculation* (different lifecycles/consumers); pandas has no clean
  home for a whole-frame scalar (`.attrs` is non-standard/lossy).
- **Revisit-if:** we ever need a fully self-contained single-artifact frame for export where carrying
  a sidecar is impossible (e.g. a flat CSV handoff). Even then prefer a sidecar over per-row scalars.

## R2. Module name `chain`
- **Idea:** name the module `alphavar.chain` (matches "result chain"/"цепочка").
- **Why dropped:** **taken** — `options/chain_class.py` already means *option chain* (the board);
  semantic clash.
- **Revisit-if:** never (the collision is permanent). Chosen instead: `alphavar.flow`.

## R3. Module name `compose` / `pipeline`
- **Idea:** `alphavar.compose` (composition) or `alphavar.pipeline`.
- **Why dropped:** `compose` is verb-y for a package and carries a docker-compose shadow; `pipeline`
  connotes a *linear* ETL while this is a branching DAG. Picked `flow` (noun, DAG-honest) and kept
  `composer` for the Layer B assembler *inside* `flow`.
- **Revisit-if:** the linear connotation of `pipeline` turns out not to mislead and readers prefer it;
  or `compose` is wanted as a sub-name. Low priority.

## R4. `ResultMeta` provenance entity on every result  ← most recent, watch this one
- **Idea:** every result is `frame + meta`, where `ResultMeta` carries `kind`, `params`, `upstream`
  (lineage), `schema`. A shared meta type (variant "C": core + per-target subtypes) so a fan-in node
  reads N heterogeneous results uniformly.
- **Why dropped (owner, 2026-06-20):** the decisive question — *what is in `meta` that is not in the
  schemas?* Answer: nothing the **domain** needs. `kind` = a reference to the schema; scalar values
  are already carried as the ergonomic object's fields and **describable in the contract**; lineage is
  a **run** concern (`flow.RunRecord`), not a domain one. A meta entity would only re-bundle
  schema-described values and risk drift. Replaced by: **the contract describes the full I/O (frame +
  scalar-spec)**; scalars ride as ordinary result fields; provenance → `flow.RunRecord`. Uniformity
  for a fan-in node comes from the **shared output schema of the kind**, not a shared meta base class.
- **Revisit-if:** results must travel **outside a `flow` run** (serialized across processes / stored)
  carrying their own provenance, where re-running the `Plan` to reconstruct lineage is not viable.
  Then a *lightweight* self-describing tag (kind + schema ref + the scalar values) may need to attach
  to a frame after all — but as an export envelope, still not as the in-process interchange. This is
  the branch most likely to flip back; keep it visible.

## R5. (placeholder) — add future dead-ends here
Record any rejected hypothesis from later passes with the same shape (idea / why / revisit-if).
