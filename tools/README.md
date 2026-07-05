# tools/ — console tools (user-runnable)

Tools in this root directory are **runnable by a human in the console**, not only by an
agent. They are the project's operator-facing utilities — the owner types
`tools/<name> …` directly, and an agent (dev **or** desk) may run the exact same command.
That "a person runs it from a console" criterion is what puts a tool here.

A tool is committed, deterministic code (CLI / script) that does one mechanical thing and
**reuses `alphavar.*`** where possible rather than re-implementing it
(`DEVELOPMENT_REQUIREMENTS.md` D4). Keep output compact; write bulky artefacts to `.tmp/`
and print the path.

## How to run
- Shell: `tools/<name>.sh …` (or `bash tools/<name>.sh …`).
- Python: `uv run python tools/<name>.py …`.

The same invocation works for the owner and for an agent, in dev and in desk mode.

## Where a tool belongs
- **`tools/` (here)** — anything a person would run from a console: data sync, data
  migration, one-off operational maintenance. User-facing **and** agent-facing.
- **`_aitna/tools/`** — internal to the **dev/build** agent (e.g. recording test
  fixtures); run as `python -m _aitna.tools.<tool>`. Not part of the operator surface.
- **`agents/tools/`** — shared across **desk** agents (common to the operate mode);
  **`agents/<agent>/tools/`** — internal to one specific desk agent.

If the owner is expected to run it by hand, it goes here; if it only serves agent-internal
workflow, it stays with that agent (shared desk tools in `agents/tools/`, otherwise
with the single agent that uses it).

## Index
- **`build_ci_fixtures`** — trim the full local data tree (`$DATA_PATH` / `./data`) into the
  committed hermetic fixture set `tests/fixtures/data/` the suite defaults to (T11).
  `uv run python -m tools.build_ci_fixtures [--option-timestamps N] [--update-files K]`.
- **`sync_test_data.sh`** — sync the minimal `DERIBIT/BTC` market-data slice the test suite
  needs into the local data store. `tools/sync_test_data.sh SRC [DST] [--apply]`.
- **`migrate_data_layout`** *(planned, ADR 0001)* — migrate the local store to the
  singular `instrument_kind` canon: rename legacy plural kind dirs (`options→option`,
  `futures→future`) and expand parquet column codes via
  `alphavar.core.migration.legacy_parquet`.
