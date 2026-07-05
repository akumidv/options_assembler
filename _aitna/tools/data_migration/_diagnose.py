"""Fast, read-only diagnosis of a stored exchange tree: metadata / structure / types.

Reuses the project's own vocabulary and migration rules — it never re-implements the schema:
- **structure** — legacy column names (would be renamed by the converter) and unknown columns,
  read cheaply from each parquet's Arrow schema (no data load);
- **types** — datetime terms must be tz-aware timestamps (epoch-int / naive / string break date
  joins); price/iv/greeks must be numeric; nanosecond units are flagged (ms-max convention, R4.2);
- **metadata** — the reference sidecars (`_asset.json` / `_meta.parquet`): present when needed,
  `asset_code` matches the folder, and every contract key in the series is covered by a reference
  version (a slim series with no sidecar is unrecoverable → error).

Pure functions returning `Issue` records; the CLI in ``__main__`` prints + drives the fix.
"""
from __future__ import annotations

import glob
import os
from dataclasses import dataclass

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from alphavar.core.dictionary import InstrumentKind, column_names
from alphavar.io.provider import read_reference
from alphavar.options.dictionary import OptionsTerm
from alphavar.options.lib.reference import CONTRACT_KEY_COLUMNS, CONTRACT_REF_COLUMNS
from alphavar.options.migration import OPTIONS_SPEC

_LEGACY_RENAMES = OPTIONS_SPEC.renames  # full core + derivatives legacy→canonical map

_SOURCE_PREFIX = "source_"
_RAW_SUFFIX = "_raw"

# Terms whose stored dtype we check (the ones that break joins / math when wrong).
_DATETIME_TERMS = frozenset(
    {
        OptionsTerm.TIMESTAMP,
        OptionsTerm.REQUEST_TIMESTAMP,
        OptionsTerm.EXCH_TIMESTAMP,
        OptionsTerm.EXPIRATION_DATE,
        OptionsTerm.UNDERLYING_EXPIRATION_DATE,
        OptionsTerm.VALID_FROM,
        OptionsTerm.VALID_TO,
    }
)
_NUMERIC_TERMS = frozenset(
    {
        OptionsTerm.PRICE, OptionsTerm.IV, OptionsTerm.EXCH_PRICE, OptionsTerm.EXCH_IV,
        OptionsTerm.EXCH_MARK_PRICE, OptionsTerm.EXCH_MARK_IV, OptionsTerm.STRIKE,
        OptionsTerm.ASK, OptionsTerm.BID, OptionsTerm.LAST, OptionsTerm.UNDERLYING_PRICE,
        OptionsTerm.DELTA, OptionsTerm.GAMMA, OptionsTerm.VEGA, OptionsTerm.THETA, OptionsTerm.RHO,
    }
)
# The full registry vocabulary (every term on OptionsTerm + inherited core terms), so "unknown"
# means "not a registry term at all" — not merely "absent from a per-dataset column set".
_KNOWN_TERMS = frozenset(column_names(OptionsTerm)) | frozenset(CONTRACT_REF_COLUMNS)
# Option-kind folder names: canonical singular (ADR 0001) + legacy plural.
_OPTION_KIND_DIRS = (InstrumentKind.OPTION.value, "options")
_KIND_DIRS = (InstrumentKind.OPTION.value, InstrumentKind.FUTURE.value, InstrumentKind.SPOT.value, "options", "futures")


@dataclass(frozen=True)
class Issue:
    """One diagnosed problem. ``severity`` error = breaks load/math; warn = drift/convention."""

    severity: str  # "error" | "warn"
    category: str  # "metadata" | "structure" | "types"
    location: str
    detail: str


def _is_known(col: str) -> bool:
    return col in _KNOWN_TERMS or (col.endswith(_RAW_SUFFIX) and col[: -len(_RAW_SUFFIX)] in _KNOWN_TERMS)


def diagnose_parquet(path: str) -> list[Issue]:
    """Structure + type checks from the Arrow schema only (no data load)."""
    issues: list[Issue] = []
    schema = pq.read_schema(path)
    cols = list(schema.names)

    legacy = [c for c in cols if c in _LEGACY_RENAMES or c.startswith(_SOURCE_PREFIX)]
    if legacy:
        issues.append(Issue("error", "structure", path, f"legacy column names → run fix: {legacy}"))
    unknown = [c for c in cols if not _is_known(c) and c not in _LEGACY_RENAMES and not c.startswith(_SOURCE_PREFIX)]
    if unknown:
        issues.append(Issue("warn", "structure", path, f"unknown columns (not in the registry): {unknown}"))

    for name in cols:
        t = schema.field(name).type
        if name in _DATETIME_TERMS:
            if not pa.types.is_timestamp(t):
                issues.append(Issue("error", "types", path, f"'{name}' is {t}, expected a tz-aware timestamp"))
            elif getattr(t, "tz", None) is None:
                issues.append(Issue("error", "types", path, f"'{name}' timestamp is tz-naive (expected UTC)"))
            elif getattr(t, "unit", None) == "ns":
                issues.append(Issue("warn", "types", path, f"'{name}' is nanosecond — ms-max convention (R4.2)"))
        elif name in _NUMERIC_TERMS and not _is_numeric(t):
            issues.append(Issue("error", "types", path, f"'{name}' is {t}, expected a numeric dtype"))
    return issues


def _is_numeric(t) -> bool:
    return pa.types.is_floating(t) or pa.types.is_integer(t) or pa.types.is_decimal(t)


def _option_series_files(asset_dir: str) -> list[str]:
    files: list[str] = []
    for kind in _OPTION_KIND_DIRS:
        files.extend(glob.glob(os.path.join(asset_dir, kind, "*", "*.parquet")))
    return sorted(f for f in files if not f.endswith(".bak"))


def diagnose_metadata(asset_dir: str) -> list[Issue]:
    """Reference-sidecar checks for one asset folder."""
    issues: list[Issue] = []
    files = _option_series_files(asset_dir)
    if not files:
        return issues  # no options series → no contract reference expected

    asset_code = os.path.basename(os.path.normpath(asset_dir))
    sample_cols = set(pq.read_schema(files[0]).names)
    is_slim = OptionsTerm.ASSET_CODE not in sample_cols or OptionsTerm.EXCH_SYMBOL not in sample_cols
    asset, history = read_reference(asset_dir)

    if asset is None:
        sev = "error" if is_slim else "warn"
        why = "slim series is unrecoverable without it" if is_slim else "run fix to form it"
        issues.append(Issue(sev, "metadata", asset_dir, f"no reference sidecar ({why})"))
        return issues
    if asset.asset_code != asset_code:
        detail = f"_asset.json asset_code={asset.asset_code!r} ≠ folder {asset_code!r}"
        issues.append(Issue("warn", "metadata", asset_dir, detail))

    # contract coverage: every (expiration, strike, right) in the series should exist in the reference
    key_cols = [c for c in CONTRACT_KEY_COLUMNS if c in sample_cols and c in history.columns]
    if key_cols and not history.empty:
        series_keys = pd.concat(
            [pd.read_parquet(f, columns=key_cols) for f in files], ignore_index=True
        ).drop_duplicates()
        merged = series_keys.merge(history[key_cols].drop_duplicates().assign(_ok=True), on=key_cols, how="left")
        missing = int(merged["_ok"].isna().sum())
        if missing:
            detail = f"{missing} contract key(s) in the series have no reference version"
            issues.append(Issue("warn", "metadata", asset_dir, detail))
    return issues


def diagnose_exchange(exchange_dir: str) -> list[Issue]:
    """Walk an exchange folder ({exchange}/{asset}/{kind}/{tf}/{year}.parquet)."""
    issues: list[Issue] = []
    for asset_code in sorted(os.listdir(exchange_dir)):
        asset_dir = os.path.join(exchange_dir, asset_code)
        if not os.path.isdir(asset_dir):
            continue
        for kind in _KIND_DIRS:
            for path in sorted(glob.glob(os.path.join(asset_dir, kind, "*", "*.parquet"))):
                if not path.endswith(".bak"):
                    issues.extend(diagnose_parquet(path))
        issues.extend(diagnose_metadata(asset_dir))
    return issues
