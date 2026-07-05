"""Split a wide quote frame into time-series + reference layers (R4.6, T25) — pure functions.

A quote frame repeats per-instrument constants on every row (asset_code, instrument_kind,
exch_symbol, option_style, …). ``split_reference`` factors them out into:
- ``asset``  — one ``AssetMeta`` (asset-level constants, per ``asset_code``);
- ``contracts`` — a deduplicated contract reference frame, keyed by
  ``(expiration_date, strike, option_right)`` (the venue ticker `exch_symbol`, `option_style`,
  underlying linkage);
- ``quotes`` — the slim time series: per-row data + the contract key (to rejoin).

``apply_reference`` is the exact inverse (lossless round-trip). Operates on a **single asset**
frame (storage is per-asset); a mixed-asset or non-constant asset-level column is an error.

The layer assignment and lossless split/rejoin are owner-verified through the round-trip
test.
"""
from dataclasses import dataclass

import pandas as pd

from alphavar.options.dictionary import OptionsTerm
from alphavar.options.entities import AssetMeta

# Asset-level: one value per asset_code within a stored series (R4.6).
ASSET_META_COLUMNS = (
    OptionsTerm.INSTRUMENT_KIND,
    OptionsTerm.ASSET_CLASS,
    OptionsTerm.CURRENCY,
    OptionsTerm.CONTRACT_KIND,
    OptionsTerm.TITLE,
)
# Contract-level reference: constant per option contract, keyed by:
CONTRACT_KEY_COLUMNS = (
    OptionsTerm.EXPIRATION_DATE,
    OptionsTerm.STRIKE,
    OptionsTerm.OPTION_RIGHT,
)
# Candidate contract-level columns. A candidate is moved to the reference only when it is
# actually constant within the contract key (some venue feeds carry a time-varying
# underlying link, e.g. `underlying_expiration_date` — those stay in the time series). This
# keeps the split lossless regardless of feed quirks.
CONTRACT_REF_COLUMNS = (
    OptionsTerm.EXCH_SYMBOL,
    OptionsTerm.OPTION_STYLE,
    OptionsTerm.UNDERLYING_CODE,
    OptionsTerm.UNDERLYING_ASSET_CLASS,
    OptionsTerm.UNDERLYING_EXPIRATION_DATE,
)


def _constant_per_key(df: pd.DataFrame, key_cols: list[str], col: str) -> bool:
    """True if ``col`` takes a single value (NaNs treated as one) within every key group."""
    return bool(df.groupby(key_cols, dropna=False)[col].nunique(dropna=False).max() <= 1)


@dataclass
class ReferenceSplit:
    """Result of factoring a wide quote frame into time-series + reference layers."""

    quotes: pd.DataFrame  # slim per-row time series (+ the contract key)
    asset: AssetMeta  # asset-level reference
    contracts: pd.DataFrame  # contract-level reference (deduplicated, keyed)


def split_reference(df: pd.DataFrame) -> ReferenceSplit:
    """Factor out the reference layers from a single-asset wide quote frame (R4.6)."""
    asset_codes = df[OptionsTerm.ASSET_CODE].dropna().unique()
    if len(asset_codes) != 1:
        raise ValueError(f"split_reference expects one asset_code, got {list(asset_codes)}")

    meta = {OptionsTerm.ASSET_CODE: asset_codes[0]}
    asset_drop = []
    for col in ASSET_META_COLUMNS:
        if col in df.columns:
            values = df[col].dropna().unique()
            if len(values) > 1:
                raise ValueError(f"asset-level column {col!r} is not constant: {list(values)}")
            meta[col] = values[0] if len(values) else None
            asset_drop.append(col)

    key_cols = [c for c in CONTRACT_KEY_COLUMNS if c in df.columns]
    ref_cols = [
        c for c in CONTRACT_REF_COLUMNS if c in df.columns and (not key_cols or _constant_per_key(df, key_cols, c))
    ]
    if ref_cols and key_cols:
        contracts = df[key_cols + ref_cols].drop_duplicates(subset=key_cols).reset_index(drop=True)
    else:
        contracts = df[ref_cols].drop_duplicates().reset_index(drop=True) if ref_cols else pd.DataFrame()

    # asset_code is the AssetMeta key — it is file-constant, so it also leaves the time series.
    quotes = df.drop(columns=[OptionsTerm.ASSET_CODE, *asset_drop, *ref_cols]).reset_index(drop=True)
    return ReferenceSplit(quotes=quotes, asset=AssetMeta(**meta), contracts=contracts)


def apply_reference(split: ReferenceSplit) -> pd.DataFrame:
    """Inverse of ``split_reference``: rebuild the wide frame from the slim layers (lossless)."""
    df = split.quotes.copy()
    key_cols = [c for c in CONTRACT_KEY_COLUMNS if c in df.columns and c in split.contracts.columns]
    if not split.contracts.empty and key_cols:
        df = df.merge(split.contracts, on=key_cols, how="left")
    for field, value in split.asset.model_dump().items():
        if value is None:  # asset_code is non-None and restored here too
            continue
        df[field] = value
    return df
