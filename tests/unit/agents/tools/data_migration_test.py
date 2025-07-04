"""data_migration tool: the diagnosis finds metadata / structure / type problems (T25)."""

import pandas as pd

from _forge.tools.data_migration._diagnose import (
    diagnose_exchange,
    diagnose_metadata,
    diagnose_parquet,
)
from alphavar.io.provider import write_reference
from alphavar.options.dictionary import OptionsTerm
from alphavar.options.entities import AssetMeta

UTC = "UTC"


def _ms(ts):
    """A millisecond-resolution tz-aware timestamp (the stored convention, R4.2)."""
    return pd.Series([pd.Timestamp(ts, tz=UTC)]).astype("datetime64[ms, UTC]")


def _good_frame():
    return pd.DataFrame(
        {
            OptionsTerm.ASSET_CODE: ["BTC"],
            OptionsTerm.EXCH_SYMBOL: ["BTC-28MAR25-100000-C"],
            OptionsTerm.TIMESTAMP: _ms("2025-01-01"),
            OptionsTerm.EXPIRATION_DATE: _ms("2025-03-28"),
            OptionsTerm.STRIKE: [100000.0],
            OptionsTerm.OPTION_RIGHT: ["call"],
            OptionsTerm.PRICE: [0.1],
        }
    )


def test_clean_frame_has_no_issues(tmp_path):
    path = tmp_path / "f.parquet"
    _good_frame().to_parquet(path)
    assert diagnose_parquet(str(path)) == []


def test_legacy_and_bad_dtype_are_flagged(tmp_path):
    df = pd.DataFrame(
        {
            "symbol": ["BTC"],  # legacy name -> structure error
            OptionsTerm.PRICE: ["oops"],  # string price -> types error
            OptionsTerm.TIMESTAMP: [pd.Timestamp("2025-01-01", tz=UTC)],
        }
    )
    path = tmp_path / "f.parquet"
    df.to_parquet(path)
    issues = diagnose_parquet(str(path))
    cats = {(i.category, i.severity) for i in issues}
    assert ("structure", "error") in cats  # legacy column
    assert ("types", "error") in cats  # non-numeric price


def test_tz_naive_timestamp_is_an_error(tmp_path):
    df = pd.DataFrame({OptionsTerm.TIMESTAMP: [pd.Timestamp("2025-01-01")]})  # naive
    path = tmp_path / "f.parquet"
    df.to_parquet(path)
    issues = diagnose_parquet(str(path))
    assert any(i.category == "types" and i.severity == "error" for i in issues)


def test_metadata_slim_series_without_sidecar_is_error(tmp_path):
    # slim options series (no asset_code / exch_symbol) and no reference sidecar -> unrecoverable
    asset_dir = tmp_path / "BTC"
    series = asset_dir / "option" / "EOD"
    series.mkdir(parents=True)
    slim = _good_frame().drop(columns=[OptionsTerm.ASSET_CODE, OptionsTerm.EXCH_SYMBOL])
    slim.to_parquet(series / "2025.parquet")
    issues = diagnose_metadata(str(asset_dir))
    assert any(i.category == "metadata" and i.severity == "error" for i in issues)


def test_metadata_ok_when_sidecar_present(tmp_path):
    asset_dir = tmp_path / "BTC"
    series = asset_dir / "option" / "EOD"
    series.mkdir(parents=True)
    _good_frame().to_parquet(series / "2025.parquet")
    write_reference(AssetMeta(asset_code="BTC"), pd.DataFrame(), str(asset_dir))
    # wide series + a sidecar that matches the folder -> no metadata error
    assert not [i for i in diagnose_metadata(str(asset_dir)) if i.severity == "error"]


def test_diagnose_exchange_walks_assets(tmp_path):
    exch = tmp_path / "DERIBIT"
    series = exch / "BTC" / "option" / "EOD"
    series.mkdir(parents=True)
    _good_frame().to_parquet(series / "2025.parquet")
    issues = diagnose_exchange(str(exch))
    # only the recoverable "no sidecar" metadata warning, no errors
    assert not [i for i in issues if i.severity == "error"]
    assert any(i.category == "metadata" for i in issues)
