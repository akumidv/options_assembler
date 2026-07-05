"""Persist the reference layers to a per-asset folder (R4.6, T25) — storage adapter.

File-based storage adapter for the slowly-changing reference, kept in the I/O layer (T41: it
does file I/O, so it lives beside the file provider — not in the pure ``options/lib``). Storage
is per asset: ``{exchange}/{asset_code}/...`` holds the time series (one parquet per
``{kind}/{timeframe}/{year}``); the reference lives alongside it at the asset root:

- ``_asset.json``  — the asset-level ``AssetMeta`` (one record per ``asset_code``);
- ``_meta.parquet`` — the contract-level SCD-2 history (``valid_from``/``valid_to`` versions).

These functions are pure file I/O over an *asset directory* path — no provider coupling; the
``AbstractFileProvider`` wires them to an ``asset_code``. Reading an absent reference yields
``(None, empty frame)`` so an SCD history can be started from scratch with ``append_on_change``.

# 4VERIFY (owner, D2): the on-disk reference layout (sidecar ``_asset.json`` + ``_meta.parquet``
# at the asset root, beside the existing ``{kind}/{timeframe}/{year}.parquet`` series) and the
# lossless round-trip of AssetMeta + the tz-aware SCD history through it.
"""
import os

import pandas as pd

from alphavar.options.entities import AssetMeta

ASSET_FILENAME = "_asset.json"
META_FILENAME = "_meta.parquet"


def asset_meta_path(asset_dir: str) -> str:
    """Path of the asset-level reference sidecar."""
    return os.path.join(asset_dir, ASSET_FILENAME)


def contract_history_path(asset_dir: str) -> str:
    """Path of the contract-level SCD-2 history."""
    return os.path.join(asset_dir, META_FILENAME)


def write_reference(asset: AssetMeta, history: pd.DataFrame, asset_dir: str) -> None:
    """Persist the asset-level meta + contract-level SCD history under ``asset_dir`` (D7: data-first)."""
    os.makedirs(asset_dir, exist_ok=True)
    with open(asset_meta_path(asset_dir), "w", encoding="utf-8") as fh:
        fh.write(asset.model_dump_json())
    # Parquet's default coerces timestamps to milliseconds — intentional: ms-max resolution
    # (down to second-rounding) is the project's timestamp convention (R4.2); ns never needed.
    history.to_parquet(contract_history_path(asset_dir))


def read_reference(asset_dir: str) -> tuple[AssetMeta | None, pd.DataFrame]:
    """Load the reference under ``asset_dir``; ``(None, empty frame)`` when absent (start fresh)."""
    meta_path = asset_meta_path(asset_dir)
    asset = None
    if os.path.exists(meta_path):
        with open(meta_path, encoding="utf-8") as fh:
            asset = AssetMeta.model_validate_json(fh.read())

    hist_path = contract_history_path(asset_dir)
    history = pd.read_parquet(hist_path) if os.path.exists(hist_path) else pd.DataFrame()
    return asset, history
