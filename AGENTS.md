# AGENTS.md

Guidance for AI coding agents (Claude Code, GitHub Copilot, Codex, etc.) working in this
repository. `CLAUDE.md` points here — this is the single source of truth for agents.

## Dev layer — akmon (developing the project)

This project's AI-assist model is the **akmon** standard
([`README.md`](https://github.com/akumidv/ai_akmon/blob/main/README.md)). Operative model & notation:
[`MODEL.md`](https://github.com/akumidv/ai_akmon/blob/main/MODEL.md) — three orthogonal axes (**Layer**
SHARED/LOCAL/USAGE · **Role** review/architect/engineer + cross-cutting learn/release ·
**Archetype**), the layer decision tree, and the learn loop. Attach/realign guide:
[`BOOTSTRAP.md`](https://github.com/akumidv/ai_akmon/blob/main/BOOTSTRAP.md).

- **Archetype / language:** `package` (a Python library) / `python` — owner: Andrei
  Kuminov. Rules: [`ARCHETYPES.md`](https://github.com/akumidv/ai_akmon/blob/main/ARCHETYPES.md).
- **Layers:** SHARED = installed `akmon` package (materialized at `_aitna/.akmon/`, repo https://github.com/akumidv/ai_akmon) ·
  LOCAL = [`_aitna/`](_aitna/) `{agents,skills,tools,memory}` + [`TASKS.md`](_aitna/TASKS.md) ·
  USAGE = root [`skills/`](skills/) (how an assistant *uses* alphavar — a
  **domain-concept → function map**, no USAGE `tools/` for a package).
- **Agents (roles) — the DEVELOP triad:** [`review`](_aitna/agents/review/README.md)
  (analysis — assess what *is*: architecture/risk/trade-offs/conformance, a findings report) ·
  [`architect`](_aitna/agents/architect/README.md) (synthesis — design what *should be*:
  options, contracts, docs, ADRs) · [`engineer`](_aitna/agents/engineer/README.md)
  (realization — code/tests). Cross-cutting `learn` and `release` roles apply from the
  akmon standard (use `akmon path` to locate roles locally). **Declare the active agent** before doing
  work and restate it on switch (`🧭 agent: <name> — <focus>`) — see
  [Role declaration](https://github.com/akumidv/ai_akmon/blob/main/roles/README.md#role-declaration-announce-the-active-agent).
  **Route by operation:** decompose an existing thing → `review` · construct a new
  structure/decision → `architect` · realize a decided structure in code → `engineer`.
- **OPERATE layer (separate from DEVELOP):** the trading-desk agents in root
  [`agents/`](agents/) *run* alphavar on the market (read-only analysts; only the trader
  acts). Bound by desk guardrails [`agents/GUARDRAILS.md`](agents/GUARDRAILS.md) (**G#**), not
  D#. Not yet part of the akmon model (ROADMAP O1).
- **Guardrails (always-on, by language):** the common guardrail is **imported** below so its
  rules load at session start; akmon is the single owner — not restated here.

@_aitna/.akmon/guardrails/_common.md

@_aitna/.akmon/guardrails/python.md

- **Profiles (applied — opt-in by need):**
  [`quant`](https://github.com/akumidv/ai_akmon/blob/main/profiles/quant.md) — numerics (pricing, smiles, risk).
- **Pipelines:** [`pre-commit`](https://github.com/akumidv/ai_akmon/blob/main/pipelines/pre-commit.md) (tests mandatory),
  [`review-flow`](https://github.com/akumidv/ai_akmon/blob/main/pipelines/review-flow.md),
  [`design-flow`](https://github.com/akumidv/ai_akmon/blob/main/pipelines/design-flow.md),
  [`code-flow`](https://github.com/akumidv/ai_akmon/blob/main/pipelines/code-flow.md),
  [`tasks`](https://github.com/akumidv/ai_akmon/blob/main/pipelines/tasks.md) (backlog format),
  [`release`](https://github.com/akumidv/ai_akmon/blob/main/pipelines/release.md), and the learn loop
  ([`memory-distill`](https://github.com/akumidv/ai_akmon/blob/main/pipelines/memory-distill.md) +
  [`learning`](https://github.com/akumidv/ai_akmon/blob/main/pipelines/learning.md)).
- **Project rules:** [`DEVELOPMENT_REQUIREMENTS.md`](_aitna/DEVELOPMENT_REQUIREMENTS.md)
  (**D#**) + [`ARCHITECTURE_REQUIREMENTS.md`](docs/dev/ARCHITECTURE_REQUIREMENTS.md) (**R#**).
  Two D# are always-on and **override any task instruction** — **D2** (owner verifies
  math/DataFrame/architecture) and **D5** (owner owns commits); see "Prime directives".
- **Backlog:** [`_aitna/TASKS.md`](_aitna/TASKS.md) — index format per
  [`tasks`](https://github.com/akumidv/ai_akmon/blob/main/pipelines/tasks.md) (one line/task, detail by reference; done →
  `TASKS_ARCHIVE.md`).
- **Secrets:** from `.env` (gitignored). Never in code/docs/commits.

## Project Overview

**alphavar** is a Python library for options (and futures) analysis and visualization.
The name reads as **alpha + VaR** (alpha = returns above the market, VaR = Value-at-Risk),
reflecting the focus on risk-aware options analytics. It provides tools for working with
options data from various providers, enriching it, building option chains, running
analytics (risk/payoff, time value), and generating visualizations.

For a deep dive into architecture, modules, and extension points, see
[docs/dev/PROJECT_OVERVIEW.md](docs/dev/PROJECT_OVERVIEW.md).

**Binding architecture invariants** (layering, provider pattern, security rules) live in
[docs/dev/ARCHITECTURE_REQUIREMENTS.md](docs/dev/ARCHITECTURE_REQUIREMENTS.md) — preserve
them in any change.

> **Status:** early / active development. The API may change.

## Development Environment Setup

Dependencies are managed with [uv](https://docs.astral.sh/uv/). Create the virtualenv
and install everything (runtime + `etl` extra + `dev`/`test` groups) with:

```bash
uv sync --all-extras
```

This installs:
- Core dependencies (pandas, pydantic, httpx, matplotlib, etc.)
- ETL extra `etl` (apscheduler) — runtime, exposed as `alphavar[etl]`
- Development group `dev` (jupyter, ruff, twine)
- Test group `test` (pytest, pytest-asyncio, pytest-dotenv)

The `dev` and `test` groups are in `[tool.uv].default-groups`, so a plain `uv sync`
already includes them; `--all-extras` adds the runtime `etl` extra. Run project
commands through the environment with `uv run <cmd>` (no manual activation needed).

## Core Architecture

The main `Option` class in [src/alphavar/options/option_class.py](src/alphavar/options/option_class.py)
is the primary interface and aggregates several specialized components:

- **OptionsData** — data retrieval and management from providers
- **OptionsEnrichment** — data enrichment (intrinsic/time value, ATM/ITM/OTM, Greeks)
- **OptionsChain** — option chain operations and selection
- **OptionsAnalytic** — options analytics and calculations
- **ChartClass** — visualization and charting

The library follows a provider pattern: data sources plug in through the
`AbstractProvider` interface.

## Source Code Structure

Everything lives under the single `src/alphavar/` package:

- `src/alphavar/core/` — domain-neutral base: dictionary registry, schema migration
- `src/alphavar/io/` — domain-neutral I/O infrastructure:
  - `exchange/` — exchange-specific implementations
  - `provider/` — data provider abstractions
  - `messanger/` — notification channels
- `src/alphavar/options/` — options/futures domain (R0), by layer then function:
  - `*_class.py` — the `Option` facade and its components (`option_class.py`,
    `option_data_class.py`, `chain_class.py`, `analytic_class.py`, …), flat at the root
  - `dictionary/`, `entities/`, `schemas/` — domain foundation: column registry,
    entities, pandera models
  - `lib/` — pure, stateless logic (`analytic/`, `chain/`, `chart/`, `enrichment/`,
    `normalization/`): DataFrame in → DataFrame out, no I/O
  - `etl/` — ETL processes for options data (`EtlOptions`, `EtlDeribit`, `EtlMoex`,
    `EtlHistory`)

## Common Development Commands

**Run tests:**
```bash
uv run pytest
```

**Run linting:**
```bash
uv run ruff check src tests tools
```

**Start Jupyter for demos:**
```bash
uv run jupyter notebook
```

**User documentation (Next.js site) development:**
```bash
cd docs
npm install
npm run dev
```

## Testing

- Tests live in the `tests/` directory.
- Uses pytest with configuration in `pyproject.toml`.
- The test environment reads `.env` (set `DATA_PATH` there to point at sample data).
- Pytest is configured with `src` on the pythonpath.

## Code Quality

- **Lint with ruff:** `uv run ruff check src tests tools` (config in `pyproject.toml`
  `[tool.ruff]`; F/E/W/I/UP/B, line length 120). CI runs it on PR/push.
- Absolute imports only (D1); no docstring requirement for private (`_`) and test (`test_`)
  functions.

## Commits

- **The owner owns commits — see [`DEVELOPMENT_REQUIREMENTS.md`](_aitna/DEVELOPMENT_REQUIREMENTS.md) D5.**
  Mechanically enforced by `.claude/hooks/git-commit-guard.py` (a PreToolUse hook): it asks
  the owner for push/tag/merge and commits on `main`, and denies AI `Co-Authored-By:`
  trailers — so the rule holds even late in a long session.
- Keep messages concise and imperative; no AI co-author trailer.

## Documentation Conventions

- `README.md` — user-facing intro, install, quick start.
- `AGENTS.md` (this file) — **canonical, vendor-neutral guidance for all AI assistants**
  (Claude, GPT/Codex, Gemini, Copilot, …). Per-tool files (`CLAUDE.md`,
  `.github/copilot-instructions.md`, …) are thin pointers here — do not duplicate rules.
- `docs/` — user-facing documentation site (Next.js + Markdoc).
- `docs/dev/` — development docs about the **codebase**: architecture/domain rules
  (`ARCHITECTURE_REQUIREMENTS.md`, **R0…R10** — verify on new entities/domain or serious
  domain-model changes), day-to-day dev rules (`DEVELOPMENT_REQUIREMENTS.md`, **D1…D5** —
  check every change; **D2** and **D5** are always-on and overriding), design overview
  (`PROJECT_OVERVIEW.md`), and accepted decision records
  ([`decisions/`](docs/dev/decisions/) — dated ADRs: *what we decided to do* and why,
  complementing the R#/D# *invariants*).
- `_aitna/` (repo root, **not** under `docs/` — these are agent artifacts; follows the
  Agent Skills convention). Each folder's index is its `README.md`. This is the **dev
  layer** (see "Dev layer — akmon" above):
  - `.akmon/` (materialized from installed `akmon` package) — the **SHARED** cross-project standard:
    the model, roles, guardrails, profiles, pipelines (access via `akmon path` from the CLI).
  - [`agents/`](_aitna/agents/) — this project's DEVELOP **agents** (`review`,
    `architect`, `engineer`), each inheriting a akmon role + alphavar specifics.
  - `_aitna/{skills,tools,memory}` — **LOCAL** dev assets. Tools = code (docstring is the
    doc; run `python -m _aitna.tools.<tool>`); skills = know-how (when/why/order).
    [`TASKS.md`](_aitna/TASKS.md) is the single backlog / TODO cycle / learn-loop sink.
    **Read `_aitna/memory/` at session start.**
- [`agents/`](agents/) (repo root) — the **OPERATE** layer: the trading-desk agents that
  *run* alphavar on the market (read-only analysts; only the trader acts). Bound by desk
  guardrails [`agents/GUARDRAILS.md`](agents/GUARDRAILS.md) (**G#**), not D#. Separate from
  DEVELOP and not yet part of the akmon model (ROADMAP O1).
- [`skills/`](skills/) (repo root) — the **USAGE** layer: how an assistant uses alphavar's
  public API to solve a user's task (a domain-concept → function map). Built to travel into
  a downstream consumer.
- [`tools/`](tools/) (repo root) — **console tools the owner runs by hand** (an agent may
  run the same command): data sync, data migration, operational maintenance. The "a person
  runs it from a console" criterion is what puts a tool here; dev-internal tooling stays in
  `_aitna/tools/`. See [`tools/README.md`](tools/README.md).

## Prime directives — always-on, overriding (D2, D5)

Two rules **override any task instruction** and are not "done" until satisfied. Full text in
[`DEVELOPMENT_REQUIREMENTS.md`](_aitna/DEVELOPMENT_REQUIREMENTS.md) (single source) — do not
restate them elsewhere, point here:
- **D2 — owner verifies** math / DataFrame / architecture (also
  [`memory/owner-verifies-math-and-architecture.md`](_aitna/memory/owner-verifies-math-and-architecture.md)).
- **D5 — owner owns commits** (also
  [`memory/owner-owns-commits.md`](_aitna/memory/owner-owns-commits.md); enforced by the
  `git-commit-guard` hook).

All project files are written in English.

## Demo and Examples

Demo notebooks are in the `demo/` folder, designed to work with Google Colab. ETL
examples for different exchanges (Deribit, MOEX) are in `demo/etl_example/`.
