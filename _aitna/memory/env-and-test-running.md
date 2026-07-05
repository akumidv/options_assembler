# Environment & test-running gotchas

The `alphavar` package was migrated from Poetry to **uv + hatchling** (2026-06-14),
targeting **Python 3.14**.

Non-obvious operational facts:
- `pyarrow` is pinned `>=21,<25` because Python 3.14 needs cp314 wheels — pyarrow 21 has
  none and falls back to a cmake source build; 24.0.0 ships the wheel. Bump the cap,
  don't install cmake.
- ETL (`alphavar.options.etl`) needs the optional `apscheduler` (extra `etl`). `uv run
  pytest` does NOT activate extras by default and prunes apscheduler → run tests with
  **`uv run --extra etl pytest`** (or `uv sync --all-extras` first).
- `DATA_PATH` (market-data root) is read from `.env` via pytest-dotenv (`pyproject.toml`
  `env_files=[".env"]`); when unset, conftest defaults it to the committed hermetic set
  `tests/fixtures/data/` (T11 done), so a clean checkout is green with no local data. Point
  `DATA_PATH` at the full machine-local `./data` tree (git-ignored) for richer local runs;
  rebuild the fixtures with `uv run python -m tools.build_ci_fixtures`.
- Test output artefacts (charts) go to project-root `.tmp/` (git-ignored) via the
  `tmp_output_dir` fixture / `ALPHAVAR_TMP_DIR` env var set in `tests/conftest.py`.
- Pytest `pythonpath` includes `.` (repo root) and `src`, so the dev tools import as
  `_aitna.tools.*` (e.g. `python -m _aitna.tools.data_migration`) and `tests.utils.*` as
  packages. `_aitna/` and `_aitna/tools/` carry `__init__.py` for this. Test-only helpers
  are NOT dev tools → they live in `tests/utils/`.

Tasks live in the repo at `_aitna/TASKS.md`; formal rules in
`../../../docs/dev/ARCHITECTURE_REQUIREMENTS.md`. See
[owner-verifies-math-and-architecture.md](owner-verifies-math-and-architecture.md).
