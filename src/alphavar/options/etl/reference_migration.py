"""One-off: extract reference sidecars from existing wide history parquet (R4.6, T25 inc.5).

For each asset under an exchange folder, reads the wide options history, factors out the
per-instrument constants (``extract_reference``), and writes the reference beside the series:
``_asset.json`` (asset-level ``AssetMeta``) + ``_meta.parquet`` (contract-level SCD-2 history).

**EXTRACT-ONLY.** The wide ``{kind}/{timeframe}/{year}.parquet`` series files are left
**unchanged** — reads keep working as-is. Slimming the series (dropping the now-extracted
reference columns) is deferred until the contract-level as-of rejoin exists on load. Re-running
overwrites the sidecars (the extraction is deterministic from the series).

    python -m alphavar.options.etl.reference_migration /path/to/data/DERIBIT --apply
"""
from __future__ import annotations

import argparse
import glob
import os
import sys

import pandas as pd

from alphavar.core.dictionary import InstrumentKind
from alphavar.io.provider import write_reference
from alphavar.options.dictionary import OptionsTerm
from alphavar.options.lib.reference import extract_reference
from alphavar.options.migration import rename_legacy_option_columns

# Option kind folder names to scan: the canonical singular (ADR 0001) + the legacy plural,
# so the meta tool works whether or not the kind-folder rename has run yet.
_OPTION_KIND_DIRS = (InstrumentKind.OPTION.value, "options")


def _read_asset_wide(asset_dir: str) -> pd.DataFrame | None:
    """Concat all options history parquet for one asset (the reference lives in options).

    Reads through ``rename_legacy_columns`` so legacy column spellings are tolerated — the
    reference extraction never needs a separate column-conversion pass to have run first.
    """
    files: list[str] = []
    for kind_dir in _OPTION_KIND_DIRS:
        pattern = os.path.join(asset_dir, kind_dir, "*", "*.parquet")
        files.extend(f for f in glob.glob(pattern) if not f.endswith(".bak"))
    files = sorted(set(files))
    if not files:
        return None
    frames = [rename_legacy_option_columns(pd.read_parquet(f)) for f in files]
    return pd.concat(frames, ignore_index=True)


def migrate_asset(asset_dir: str, *, apply: bool) -> bool:
    """Extract + write the reference for one asset folder. Returns True if a reference was found."""
    df = _read_asset_wide(asset_dir)
    if df is None or df.empty:
        return False
    when = (
        df[OptionsTerm.TIMESTAMP].min()
        if OptionsTerm.TIMESTAMP in df.columns
        else pd.Timestamp.now(tz="UTC")
    )
    asset, history = extract_reference(df, when)
    print(f"[{'APPLY' if apply else 'DRY'}] {asset_dir}: asset_code={asset.asset_code} contracts={len(history)}")
    if apply:
        write_reference(asset, history, asset_dir)
    return True


def migrate_exchange(exchange_dir: str, *, apply: bool) -> int:
    """Migrate every asset folder under an exchange folder. Returns count of assets processed."""
    count = 0
    for asset_code in sorted(os.listdir(exchange_dir)):
        asset_dir = os.path.join(exchange_dir, asset_code)
        if os.path.isdir(asset_dir) and migrate_asset(asset_dir, apply=apply):
            count += 1
    return count


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Extract reference sidecars from wide history parquet (T25).")
    parser.add_argument("exchange_dir", help="Exchange data folder (its sub-folders are asset codes).")
    parser.add_argument("--apply", action="store_true", help="Write the sidecars (default: dry run).")
    args = parser.parse_args(argv)
    n = migrate_exchange(args.exchange_dir, apply=args.apply)
    mode = "wrote reference for" if args.apply else "would write reference for"
    print(f"\n{mode} {n} asset(s).")
    if not args.apply and n:
        print("Re-run with --apply to write the sidecars (the wide series files are not touched).")
    return 0


if __name__ == "__main__":
    sys.exit(_main())
