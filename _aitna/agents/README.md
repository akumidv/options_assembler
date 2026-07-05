# `_aitna/agents/` — alphavar's development agents

Concrete agents that develop **this** project. Each inherits a cross-project **role**
from akmon ([`../akmon/roles/`](../akmon/roles/)) and adds only alphavar
specifics — which modules, docs, tests, and profiles it works with.

> Role = the definition (in akmon, shared). Agent = the incarnation (here, local).
> See the model in [`../akmon/MODEL.md`](../akmon/MODEL.md) §3.

## Roster

The DEVELOP triad — **analysis → synthesis → realization** (route by operation: decompose →
`review` · construct → `architect` · realize → `engineer`). The cross-cutting `learn` and
`release` roles ([akmon/roles/](../akmon/roles/)) are applied directly from akmon — no
local incarnation until alphavar needs project specifics for them.

| Agent | Inherits role | Works on |
|---|---|---|
| [review](review/README.md) | [akmon/roles/review](../akmon/roles/review.md) | evidence-first review of architecture, decisions, trade-offs, isolation, domain model, and risks |
| [architect](architect/README.md) | [akmon/roles/architect](../akmon/roles/architect.md) | `docs/dev/` (R#), architecture, the column dictionary, ADRs in `docs/dev/decisions/` |
| [engineer](engineer/README.md) | [akmon/roles/engineer](../akmon/roles/engineer.md) | `src/alphavar/`, `tests/`, the `_aitna/tools/` build tools |

## Applied baseline (alphavar)

- **Archetype:** `package` (a Python library; will split into packages later) →
  USAGE = root `skills/` for the public API, **no USAGE `tools/`**.
- **Language guardrails (automatic):** [`_common`](../akmon/guardrails/_common.md) +
  [`python`](../akmon/guardrails/python.md).
- **Profiles (opted in):** [`quant`](../akmon/profiles/quant.md) — numerics
  (pricing, smiles, risk).
- **Single backlog:** [`../TASKS.md`](../TASKS.md). Project requirements: R# in
  [`docs/dev/ARCHITECTURE_REQUIREMENTS.md`](../../docs/dev/ARCHITECTURE_REQUIREMENTS.md),
  D# in [`docs/dev/DEVELOPMENT_REQUIREMENTS.md`](../DEVELOPMENT_REQUIREMENTS.md).

## Adding an agent

Add a folder here only when a new role applies, or when one role needs more than one
incarnation in alphavar (e.g. a second `engineer` for a distinct subsystem). Otherwise
extend an existing charter or add a local skill.
