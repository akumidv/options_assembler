# Skill: refresh exchange test fixtures

**Goal.** Re-capture the hermetic HTTP fixtures the exchange tests replay, after an
exchange endpoint's response shape changed (or to add coverage). Tool-driven skill —
it orchestrates the recorder + trimmer tools, then verifies.

**When.** An exchange test fails with a parse/shape mismatch that reflects a *real* API
change (not a code bug); or you added a new captured call in
`_aitna/tools/exchange_fixtures/<exchange>.py`.

**Preconditions.**
- Network access to the live venue APIs (recording step only).
- `uv sync --all-extras` done. Recording reuses `alphavar.io.exchange` — do not write raw
  HTTP here.

## Steps

1. **Record** (needs network) — captures full responses keyed by path+query, with HTTP
   status, into `tests/unit/exchange/fixtures/<exchange>/`:
   ```bash
   uv run python -m _aitna.tools.exchange_fixtures            # both, or
   uv run python -m _aitna.tools.exchange_fixtures --only deribit
   ```
   A venue rejecting a call is logged + skipped (best-effort). Non-2xx (e.g. 422/400) is
   recorded *with its status* on purpose — error paths must be exercised too.
2. **Trim** (offline, idempotent) — shrink to a few diverse rows (multi-MB → ~40–50 kB):
   ```bash
   uv run python -m tests.utils.exchange_fixtures.trim --check   # preview sizes
   uv run python -m tests.utils.exchange_fixtures.trim           # apply
   ```
3. **Verify** — the exchange suite is hermetic and fast (no network):
   ```bash
   uv run --extra etl pytest tests/unit/exchange -q
   ```

## Done / verify checklist

- [ ] Exchange suite green and **fast** (~seconds, not minutes → no live calls leaked).
- [ ] Fixtures committed and small (`du -sh tests/unit/exchange/fixtures/*`).
- [ ] `index.json` carries `{file, status}` per key; error responses kept with real status.
- [ ] Heavy multi-asset walks still `@pytest.mark.integration` (deselected by default).
- [ ] If the API change reflects a venue-vs-project mismatch (e.g. a `kind=` value),
      the fix is a per-exchange API mapping (R2.2), not editing fixtures to hide it.

Tool code + docs (docstring): [`../tools/exchange_fixtures/`](../tools/exchange_fixtures/).
