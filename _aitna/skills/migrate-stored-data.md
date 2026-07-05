# Skill: migrate accumulated stored data (column conversion + reference meta)

**Goal.** Bring an existing local data tree (already-accumulated **history** *and* pending
**updates**) up to the current schema: (1) **column conversion** — legacy column names/codes →
the current dictionary; (2) **reference meta formation** — factor each asset's per-instrument
constants into the `_asset.json` + `_meta.parquet` sidecars (R4.6, T25). Tool-driven skill —
it orchestrates two committed migration CLIs, then verifies.

**When.** A clean checkout has data with old column spellings (`symbol`, `kind`, `source_*`,
the `exhchange_mark_price` typo, short codes `c`/`p`/`o`/`f`/`s`), or stored before the
reference layer existed (no `_meta.parquet` beside the series). Run once per data root.

**The two layouts (both must be converted).**
- **History** — `{root}/{EXCHANGE}/{asset}/{kind}/{timeframe}/{year}.parquet` (canonical store).
- **Updates** — `{update_root}/{EXCHANGE}/{asset}/{venue_kind}/{timeframe}/{date}.parquet`
  (raw snapshots awaiting merge into history).

Column conversion applies to **both** (any `*.parquet` tree). Reference meta is formed from the
**history** only: it is the canonical per-asset series. Updates do **not** get their own
sidecar — once column-converted, `EtlHistory` merges them into history and folds the reference
incrementally on write (`update_reference=True`, T25 inc.4B), so meta stays current go-forward.

## Tool — one general script (verify + fix)
`_aitna/tools/data_migration` (`python -m _aitna.tools.data_migration`) drives the
whole loop and reuses the shipped library code:
- **`verify {exchange_dir}`** — fast read-only diagnosis of **metadata / structure / types**
  (legacy/unknown columns; datetime tz-aware ms-resolution; price/iv/greeks numeric; reference
  sidecar present, matches the folder, covers every contract key). Exits non-zero on any error.
- **`fix {exchange_dir} [--apply]`** — runs column conversion (`core.migration.legacy_parquet`,
  any parquet tree) then reference-meta formation (`options.etl.reference_migration`, extract-only,
  wide series untouched), then re-verifies. Dry by default; `--apply` writes (`.bak` kept).

## Steps

1. **Verify first — see exactly what is wrong (metadata / structure / types):**
   ```bash
   uv run python -m _aitna.tools.data_migration verify {root}/{EXCHANGE}
   ```
2. **Fix history (dry run, then apply):**
   ```bash
   uv run python -m _aitna.tools.data_migration fix {root}/{EXCHANGE}           # DRY (plan)
   uv run python -m _aitna.tools.data_migration fix {root}/{EXCHANGE} --apply   # writes + re-verifies
   ```
3. **Convert the updates tree too** (so future merges into history are clean — updates carry no
   sidecar; the ETL folds the reference when it merges them, `update_reference=True`):
   ```bash
   uv run python -m alphavar.core.migration.legacy_parquet {update_root}/{EXCHANGE} --apply
   ```
4. **Confirm a load reconstructs the wide frame** (slim and legacy-wide read identically):
   ```bash
   uv run pytest tests/unit/options/lib/reference tests/unit/agents/tools -q
   ```
   Spot-check one asset: `Option(provider, asset).reference` is populated and `df_hist` carries
   every expected column.

## When conversion fails or a new case appears — extend the tools, never a throwaway script

A migration that hits an **unrecognized** legacy column, value code, dtype, or layout MUST be
fixed *in the tool*, so the fix is committed, tested, and reused — **not** patched with a
one-off script or a manual `rename`/`map` in a notebook. This is the whole point of a tool (D4).

- **New legacy column name** → add it to `COLUMN_RENAMES` in `legacy_parquet.py`.
- **New legacy value code** (e.g. another `option_right`/`instrument_kind`/asset-class spelling)
  → add it to the relevant `VALUE_MAPS` entry.
- **New raw/derived split, timestamp unit, or dtype quirk** → handle it in `_coerce_types` /
  the `_raw` logic (and respect the ms-resolution convention, R4.2 — never re-add nanoseconds).
- **A genuinely ambiguous / too-old file** is raised as `MigrationError` and listed for manual
  conversion — convert it, then if the pattern recurs, encode it in the tool so it stops being
  manual.
- **New reference column** that should live in the sidecar → add it to `ASSET_META_COLUMNS` /
  `CONTRACT_REF_COLUMNS` in `options/lib/reference/_split.py` (the split stays lossless: a
  candidate only moves to the reference when it is constant per key).

Then **reflect it here**: if the new case changes the order/preconditions/verification, update
this skill in the same change. Add a pinning test next to the tool (column conversion:
`core/migration/*test`; reference: `options/lib/reference/*test`). The rule: *every* conversion
behavior is committed code with a test — accumulated data and updates are migrated only by these
tools, evolved in place.

## Done / verify checklist
- [ ] `legacy_parquet` dry run clean on **both** history and updates (no `MigrationError`); applied.
- [ ] `reference_migration` applied — `_asset.json` + `_meta.parquet` beside each asset's series.
- [ ] Reference + provider + data-class suites green; a sample `Option(...).reference` resolves.
- [ ] Wide series files **unchanged** by meta formation (extract-only); `.bak` copies kept by
      column conversion (remove once verified).
- [ ] Any new legacy/edge case encountered was encoded in the tool + a test (no throwaway code),
      and this skill updated if the procedure changed.
