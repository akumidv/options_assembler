"""Reference storage adapter: write/read AssetMeta + SCD history round-trip (T25; T41 home: io/provider)."""

import pandas as pd

from alphavar.io.provider import (
    asset_meta_path,
    contract_history_path,
    read_reference,
    write_reference,
)
from alphavar.options.dictionary import OptionsTerm
from alphavar.options.entities import AssetMeta
from alphavar.options.lib.reference import append_on_change

KEY = [OptionsTerm.EXCH_SYMBOL]
ATTR = [OptionsTerm.OPTION_STYLE]
T1 = pd.Timestamp("2025-01-01", tz="UTC")
T2 = pd.Timestamp("2025-02-01", tz="UTC")


def _snap(rows):
    return pd.DataFrame(rows, columns=[OptionsTerm.EXCH_SYMBOL, OptionsTerm.OPTION_STYLE])


def _history():
    hist = append_on_change(pd.DataFrame(), _snap([["A", "european"]]), T1, KEY, ATTR)
    return append_on_change(hist, _snap([["A", "american"]]), T2, KEY, ATTR)  # close A + open A'


def test_absent_reference_starts_fresh(tmp_path):
    asset, history = read_reference(str(tmp_path / "BTC"))
    assert asset is None
    assert history.empty  # an SCD history can be opened from this


def test_round_trip_preserves_asset_meta_and_tz_aware_history(tmp_path):
    asset_dir = str(tmp_path / "BTC")
    meta = AssetMeta(asset_code="BTC", instrument_kind="option", currency="USD", title="Bitcoin")
    history = _history()

    write_reference(meta, history, asset_dir)
    loaded_meta, loaded_history = read_reference(asset_dir)

    assert loaded_meta == meta
    # valid_from/valid_to survive the parquet round-trip tz-aware (UTC), NaT for the open record;
    # timestamps are stored at millisecond resolution by convention (the project never needs ns),
    # so compare instants, not the exact dtype unit.
    vf = loaded_history[OptionsTerm.VALID_FROM]
    assert str(vf.dtype).startswith("datetime64[") and vf.dt.tz is not None
    open_row = loaded_history[loaded_history[OptionsTerm.VALID_TO].isna()]
    assert open_row.iloc[0][OptionsTerm.OPTION_STYLE] == "american"
    closed_row = loaded_history[loaded_history[OptionsTerm.VALID_TO].notna()]
    assert closed_row.iloc[0][OptionsTerm.VALID_TO] == T2
    # same instants: cast loaded timestamps back to the source units, then full-frame equality
    normalized = loaded_history.copy()
    for col in (OptionsTerm.VALID_FROM, OptionsTerm.VALID_TO):
        normalized[col] = normalized[col].astype(history[col].dtype)
    pd.testing.assert_frame_equal(normalized, history)


def test_files_land_at_the_asset_root(tmp_path):
    import os

    asset_dir = str(tmp_path / "BTC")
    write_reference(AssetMeta(asset_code="BTC"), _history(), asset_dir)
    assert os.path.exists(asset_meta_path(asset_dir))  # _asset.json
    assert os.path.exists(contract_history_path(asset_dir))  # _meta.parquet


def test_write_then_append_then_rewrite(tmp_path):
    """The stored history is the input an ETL append-on-change folds the next snapshot into."""
    asset_dir = str(tmp_path / "BTC")
    write_reference(AssetMeta(asset_code="BTC"), _history(), asset_dir)
    _, history = read_reference(asset_dir)
    grown = append_on_change(history, _snap([["B", "european"]]), T2, KEY, ATTR)  # new key B
    write_reference(AssetMeta(asset_code="BTC"), grown, asset_dir)
    _, reloaded = read_reference(asset_dir)
    assert set(reloaded[OptionsTerm.EXCH_SYMBOL]) == {"A", "B"}
