# Tools

A **tool is code**: a deterministic, committed, reusable implementation (CLI / function /
package) plus a short spec. Run it, get a compact result — no re-deriving the steps each
time (DEVELOPMENT_REQUIREMENTS **D4**). Reach for an MCP server / agentic workflow only
when code cannot do the job.

Layout:
- `<tool_pkg>/` — the executable code (importable package / `python -m …` CLI). It
  **reuses the project's own code** where possible (e.g. the exchange API in
  `alphavar.io.exchange`) rather than re-implementing it. **The package/`__main__` docstring
  is the documentation** — purpose + the exact run command live there, next to the code.
- A separate `<tool-name>.md` spec is **only** for tools whose config can't be read from
  the code: **MCP servers** (mirror the canonical config into a root `.mcp.json`) or
  external/HTTP tools. A self-documenting `python -m …` package does **not** get a `.md`
  (it would duplicate the docstring) — index it in this README and let the docstring +
  the skill carry the docs.

Make outputs compact (filter at source; bulky artefacts to `.tmp/`, report a path).

## Tools vs skills

- **tools/** = the *how-mechanically* (code that does one thing).
- **skills/** = the *when/why/in-what-order* (a playbook that may call tools, or may be
  pure procedure with no tool). See [`../skills/README.md`](../skills/README.md).

A skill calls a tool; a tool never contains a skill's judgement. If a step is mechanical
and repeated → make it a tool. If it is ordering/context/judgement → keep it in a skill.

## Index

- [`exchange_fixtures/`](exchange_fixtures/) (code; docs in its `__main__` docstring,
  run `python -m _aitna.tools.exchange_fixtures`): record live exchange API responses
  into hermetic test fixtures, reusing `alphavar.io.exchange`. Paired trimmer/mock live in
  `tests/utils/exchange_fixtures/` (test infrastructure, not an agent tool). Playbook:
  [`../skills/refresh-exchange-fixtures.md`](../skills/refresh-exchange-fixtures.md).

- [`data_migration/`](data_migration/) (code; docs in its `__main__` docstring, run
  `python -m _aitna.tools.data_migration`): the general **verify + fix** script for stored
  data — `verify {exchange_dir}` is a fast read-only diagnosis of **metadata / structure / types**;
  `fix … --apply` runs the conversions then re-verifies. It reuses the shipped library migration
  code (below). Playbook: [`../skills/migrate-stored-data.md`](../skills/migrate-stored-data.md).

The fixes themselves live in the shipped library (committed, self-documenting `python -m` CLIs,
docstring = docs), called by the tool above:
- `alphavar.core.migration.legacy_parquet` — column conversion (legacy names/codes → current
  dictionary; `source_*`→`*_raw`; value remaps; dtype coercion). Walks any `*.parquet` tree
  (history *or* updates). Idempotent; dry-run default, `--apply` writes (keeps `.bak`).
- `alphavar.options.etl.reference_migration` — reference meta formation (per asset: `AssetMeta`
  + contract-level SCD-2 history → `_asset.json`/`_meta.parquet`). Extract-only (wide series
  untouched). Dry-run default, `--apply` writes.
New legacy/edge cases are encoded **in these tools** (+ a pinning test), never a throwaway
script — see the skill.
