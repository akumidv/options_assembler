# Development Requirements — alphavar

> **Status: binding.** Compact, day-to-day rules for *current operations and tasks* — how
> we write, test, and land changes. Check these on **every** change/PR. They are vendor-
> neutral (all AI assistants + humans). For the structural invariants of the system
> (verified when adding new entities/domain or making serious changes to the existing
> domain model) see the companion [ARCHITECTURE_REQUIREMENTS.md](../docs/dev/ARCHITECTURE_REQUIREMENTS.md).
>
> Numbering: `D#` here (development), `R#` in the architecture document.
>
> **Two always-on, overriding rules** (they win over any task instruction): **D2** (owner
> verifies math / DataFrame / architecture) and **D5** (the owner owns commits — the build
> agent never makes landing commits on its own).

## D1. Quality gates

- Tests live in `tests/unit/<area>/`, mirroring `src/alphavar/`. Unit tests must be
  hermetic: no live network calls (HTTP is mocked), no machine-specific absolute paths.
- `pytest` and `ruff check src tests tools` must pass before merging to `main` (enforced by
  CI, `.github/workflows/ci.yml`). Ruff config in `pyproject.toml` (`[tool.ruff]`).
- Python ≥ 3.11, line length ≤ 120, Pydantic models for entities/parameters,
  docstrings required except `_private`/`test_` functions.
- **Absolute imports only.** Relative imports (`from .`, `from ..`) are forbidden —
  every import uses the full package path (`from alphavar.io.exchange.deribit import …`).
  This keeps imports stable across package moves and makes the layer/domain of every
  dependency explicit at the import site.
- A test that requires not-yet-implemented logic is marked
  `@pytest.mark.xfail(reason="pending <task>: …", strict=False)` (not deleted/skipped) so
  it keeps running and flips to XPASS once the logic lands. Reference the task.
- All project files in English.

## D2. Owner verification of math, DataFrame logic, and architecture (MANDATORY)

This is a hard, always-on rule — it overrides "make the tests pass":

- **Any** new or changed **DataFrame operation** (filters, `groupby`/`agg`, joins,
  reshaping, column derivations, masks, `clip`/`fillna`, etc.) and **any quantitative /
  financial math** (payoff, pricing, Greeks, IV, risk, normalization formulas) MUST be
  accompanied by a written explanation of the implementation logic (what each step does
  and why), and an explicit **request for implementation approval** to the owner.
- Such math/DataFrame implementations are **not considered done** until the owner has
  **fully verified** them. Until then, mark the code `# 4VERIFY (owner)`
  and the corresponding task as *pending owner verification* (never "Done"). Passing
  tests are necessary but **not** sufficient. Every such item is tracked in the
  **[D2 Verification Ledger](D2_VERIFICATION.md)** (typed A/B/C, with its pinning test)
  until the owner signs it off.
- The same applies to **architectural changes** (package layout, layer boundaries,
  public interfaces, data schema/column semantics, storage layout): explain the change
  and its rationale, then request the owner's explicit approval before treating it as
  settled. (Architectural changes are also governed by ARCHITECTURE_REQUIREMENTS R0…R10.)
- When in doubt, do not silently implement — explain the options and ask. Do not delete
  existing math/DataFrame logic; preserve the prior version (commented, marked
  `4VERIFY`) so the owner can compare during verification.

## D3. Environment & workflow essentials

- Dependencies via **uv** (`uv sync --all-extras`); run commands with `uv run …`. ETL
  tests need the `etl` extra: `uv run --extra etl pytest`.
- `DATA_PATH` is read from `.env`; test artefacts (charts, dumps) go to the
  git-ignored `.tmp/` via the `tmp_output_dir` fixture.
- Details and other operational gotchas:
  [_forge/memory/env-and-test-running.md](memory/env-and-test-running.md).

## D4. Token efficiency — prefer tools implemented as code

Token economy is a first-class concern for sustainable AI-assisted development — treat it
as a real constraint, not an afterthought.

- **Implement tools as deterministic code** (a committed script / CLI command / function /
  Makefile target) whenever the task is repeatable, rather than as an interactive,
  multi-step agent workflow. Code runs once, returns a compact result, and does not
  re-consume context on every use.
- Prefer a single command that returns exactly the needed result over a chain of agent
  tool calls that stream large intermediate output into the context window. Make outputs
  compact (filter/summarize at the source; write bulky artefacts to `.tmp/` and report a
  path, not the contents).
- When adding a capability an assistant will reuse, package it as code under the project
  (e.g. a `scripts/` CLI or a function) and document it in
  [_forge/tools/](tools/) — do not rely on re-deriving it through ad-hoc steps.
- Reuse existing knowledge: consult [skills/](../../skills/) (USAGE — domain concept→function map) and
  [_forge/memory/](memory/) before re-researching from scratch.

## D5. Version control — the owner owns commits (MANDATORY)

A hard, always-on rule. Like D2, it **overrides** any task or generic instruction to
"commit", "land", "finish", or "ship": when those conflict with this rule, this rule wins.

- **The build agent never makes landing/"final" commits on its own.** Completed work is
  left in the working tree (or on a backup branch) and **reported** to the owner; the owner
  decides what lands. "Done" never implies "committed".
- **Allowed without asking:** commits to a dedicated **backup / checkpoint branch** (e.g.
  `wip/…`, `backup/…`, `refactor/…`) — for preserving work and enabling rollback. Never the
  default branch (`main`), never a merge, push, tag, or PR.
- **Allowed only on explicit request, each time:** a landing commit, any commit/merge on the
  default branch, `git push`, tags, and PRs. Approval is **per request** — a previous "yes,
  commit" does not authorise the next commit.
- When unsure whether a commit is "backup" or "landing", treat it as landing → leave it
  uncommitted and ask. Branch before committing if on the default branch (never commit to
  `main` directly).

## D6. Durable task records — describe, save, do, verify, compact

Every non-trivial task carries a **written record that survives interruption**, so work can
be resumed and its progress judged **independently** — in a new session, on another machine,
or by a different agent (human or AI). The record is the source of truth for "what is this
task and how far along is it"; the working tree alone is not.

The lifecycle (each step is mandatory, in order):

1. **Describe + plan, then save *first*.** Before writing code, record the task in
   [`_forge/TASKS.md`](TASKS.md): a stable id/title, the **goal**
   (what done means), the **plan** (ordered steps/increments), and the **acceptance check**
   (the command/test/observation that proves it). Save this before starting — an
   interruption after step 1 must still leave a resumable record.
2. **Do**, keeping the record current. Mark the task *in progress* and note which
   increment is active. A reader must always be able to tell, from the record alone, what is
   done vs. pending — never let the record drift behind the code.
3. **Verify against the saved task.** When an increment/task is finished, check it against
   the **acceptance check recorded in step 1** (not against a fresh, convenient definition).
   Math / DataFrame / architecture work is *not* done here — it goes to **D2** (`4VERIFY` +
   the [ledger](D2_VERIFICATION.md)) and stays *pending owner verification*.
4. **Mark done and compact.** On completion, replace the verbose initial/intermediate
   narrative with a **compact final entry**: outcome, where the code/tests live, and
   pointers (D2 ledger row, ADRs, R#/D#). Keep exactly enough that progress stays
   independently assessable; move long historical detail to git history of the file.

This rule **unifies** the existing mechanisms — it does not add a parallel tracker: the
record lives in `TASKS.md`, verification of math/architecture lives in the **D2 ledger**
(`4VERIFY`), and commits remain governed by **D5**. The concrete record template and the
step-by-step procedure are the [`track-task`](skills/track-task.md) skill;
follow it rather than re-inventing a format.

## D7. Signature convention — subject first, then how

> The general rule lives in the keystone guardrail
> [`API shape — subject first`](../_forge/keystone/guardrails/_common.md#api-shape--subject-first)
> (binds every project/role). D7 **pins the alphavar specifics** below — do not restate the floor.

Every callable — function, method, constructor — takes **the data it acts on as the first
parameter** (the *subject*: a DataFrame in the common case, or another object — a `Series`,
an array, a result, a source handle). Parameters **after** the first either say *what to do*
(modifiers/options) or supply *additional data*. Read every signature as `f(subject, *rest)`.

- **Why:** one uniform shape across the codebase (mirrors the `df.method(...)` idiom and the
  `OPTION_COLUMN_DEPENDENCIES` enrichment style), so a reader/assembler knows *what a call
  operates on* from position alone — no per-parameter guessing.
- **Result-chain payoff:** a producer's input edge is then simply its **first parameter**, so
  the [`core.disc`](../src/alphavar/core/disc.py) surface reads `inputs` positionally and only
  the **exceptions** are declared (`consumes=` for an alternatives slot / several inputs, or
  `consumes=[]` for a *source* with no upstream edge). See
  [`design/result-chain/disc-derivation.md`](design/result-chain/disc-derivation.md).

**What "the subject" is, by callable shape** (so `f(subject, *rest)` is unambiguous; grounded in
the ecosystem conventions audited 2026-06-21):

- **Transform** (`df + params → df` / a reduction) — subject = **the data acted on**, first:
  `price_series(df, *, source=…)`.
- **Method** — subject = **`self`** (the bound data); D7 governs only the *explicit* data params:
  if a method takes data, it comes first **after** `self` (`reapply_reference(self, df)`).
- **Reader / loader / source** — there is *no data yet*, so the subject is the **location / handle**
  it acts on → **location-first** (`read_parquet(path)`, `load(file)`; a `load` node's `OptionsData`
  handle is its first param). This is why a source is location-first *and* D7-consistent.
- **Writer / persister** — the data already exists and **is** the subject; the destination is a
  *modifier* → **data-first** (`json.dump(obj, fp)` / `pickle.dump(obj, file)` style — **not**
  `np.save`'s destination-first outlier), or better, a **method on the data** (`df.to_parquet(path)`).
- **Factory** (`make_*`, constructor-from-spec) — builds an object from a **selector/spec**, it does
  not transform a subject → **selector-first** (`make_engine(name, *, …)`); **exempt** from
  subject-first.

- **Scope:** applies to all new/edited callables. Pre-existing signatures are brought into
  line opportunistically when touched, not in a sweep (the 2026-06-21 audit flagged
  `schemas.validate`, `etl._fold_reference`, `reference.write_reference` as deferred flips).
