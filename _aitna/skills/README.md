# Skills (task playbooks)

Reusable, LLM-agnostic playbooks for recurring tasks: each is plain Markdown with a clear
goal, preconditions, ordered steps, exact commands, and a done/verify checklist. Any
assistant or human can follow them; vendor-specific skill formats (e.g. a `.claude/skills/`
shim) reference the file here rather than duplicate its content.

## Skills vs tools — the split

- **`tools/` is code.** A tool is a deterministic, committed, reusable implementation
  (CLI/function) — run it, get a result, no re-deriving each time (D4). It does *one*
  mechanical thing.
- **`skills/` is know-how.** A skill is the *playbook* — when and why to do something, in
  what order, what to check. A skill is **not** required to call a tool:
  - **tool-driven skill** — orchestrates one or more tools (e.g. "refresh exchange
    fixtures" runs the recorder, then the trimmer, then the tests).
  - **knowledge skill** — pure procedure with no dedicated tool: how to work a pipeline
    safely (add a new exchange, run-and-verify the suite, do a DataFrame change under D2).
    The steps are ordinary edits/commands the assistant performs, captured so they are
    consistent and don't get re-figured-out every time.

Rule of thumb: if a step is mechanical and repeated, it belongs in a **tool** (code) and
the skill *calls* it; if a step is judgement/ordering/context, it stays **in the skill**.
A skill may reference tools, requirements (R#/D#), and knowledge — but never inlines a
tool's code.

## Index

- [`refresh-exchange-fixtures.md`](refresh-exchange-fixtures.md) — tool-driven: re-record
  + trim the hermetic exchange HTTP fixtures (uses `_aitna/tools/exchange_fixtures` and
  `tests/utils/exchange_fixtures`).
- [`add-exchange-source.md`](add-exchange-source.md) — knowledge: how to add a new
  exchange provider end-to-end (no single tool; an ordered pipeline across layers).
- [`track-task.md`](track-task.md) — knowledge: the durable task-record lifecycle behind
  **D6** (describe + plan → save → do → verify → mark done + compact) so an interrupted task
  is resumable in a new session / machine / agent. One home (`TASKS.md`); points at the D2
  ledger and D5, never a parallel tracker.
- [`migrate-stored-data.md`](migrate-stored-data.md) — tool-driven: convert accumulated
  history **and** updates to the current schema — column conversion
  (`alphavar.core.migration.legacy_parquet`) + reference meta formation
  (`alphavar.options.etl.reference_migration`). Codifies that new legacy/edge cases are
  encoded in the tools (+ a test), never a throwaway script.
