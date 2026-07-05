"""Reference extraction from wide history (T25 inc.5): extract_reference + the etl driver."""

import pandas as pd

from alphavar.io.provider import read_reference
from alphavar.options.dictionary import OptionsTerm
from alphavar.options.lib.reference import extract_reference

T0 = pd.Timestamp("2025-01-01", tz="UTC")
T1 = pd.Timestamp("2025-01-02", tz="UTC")
EXP = pd.Timestamp("2025-03-28", tz="UTC")


def _wide():
    """A wide single-asset options frame: two contracts (call+put), two timestamps each."""
    return pd.DataFrame(
        {
            OptionsTerm.ASSET_CODE: ["BTC"] * 4,
            OptionsTerm.INSTRUMENT_KIND: ["option"] * 4,
            OptionsTerm.TIMESTAMP: [T0, T1, T0, T1],
            OptionsTerm.EXPIRATION_DATE: [EXP] * 4,
            OptionsTerm.STRIKE: [100000.0, 100000.0, 90000.0, 90000.0],
            OptionsTerm.OPTION_RIGHT: ["call", "call", "put", "put"],
            OptionsTerm.EXCH_SYMBOL: [
                "BTC-28MAR25-100000-C",
                "BTC-28MAR25-100000-C",
                "BTC-28MAR25-90000-P",
                "BTC-28MAR25-90000-P",
            ],
            OptionsTerm.PRICE: [0.1, 0.12, 0.2, 0.18],
        }
    )


def test_extract_reference_builds_asset_meta_and_one_open_version_per_contract():
    asset, history = extract_reference(_wide(), T0)
    assert asset.asset_code == "BTC"
    assert asset.instrument_kind == "option"
    # one open version per contract key (2 contracts), seeded at `when`
    assert len(history) == 2
    assert history[OptionsTerm.VALID_FROM].eq(T0).all()
    assert history[OptionsTerm.VALID_TO].isna().all()
    assert set(history[OptionsTerm.EXCH_SYMBOL]) == {"BTC-28MAR25-100000-C", "BTC-28MAR25-90000-P"}


def test_extract_reference_without_contracts_yields_empty_history():
    df = pd.DataFrame({OptionsTerm.ASSET_CODE: ["BTC"] * 2, OptionsTerm.PRICE: [1.0, 2.0]})
    asset, history = extract_reference(df, T0)
    assert asset.asset_code == "BTC"
    assert history.empty


def test_migrate_asset_writes_sidecars_and_leaves_series_untouched(tmp_path):
    from alphavar.options.etl.reference_migration import migrate_asset

    series_dir = tmp_path / "BTC" / "option" / "EOD"
    series_dir.mkdir(parents=True)
    series_path = series_dir / "2025.parquet"
    _wide().to_parquet(series_path)
    before = pd.read_parquet(series_path)

    assert migrate_asset(str(tmp_path / "BTC"), apply=True) is True

    asset, history = read_reference(str(tmp_path / "BTC"))
    assert asset.asset_code == "BTC"
    assert len(history) == 2  # seeded from min(timestamp) = T0
    assert history[OptionsTerm.VALID_FROM].min() == T0
    # EXTRACT-ONLY: the wide series file is unchanged
    pd.testing.assert_frame_equal(pd.read_parquet(series_path), before)


def test_migrate_asset_no_options_is_skipped(tmp_path):
    from alphavar.options.etl.reference_migration import migrate_asset

    (tmp_path / "BTC" / "future" / "EOD").mkdir(parents=True)
    assert migrate_asset(str(tmp_path / "BTC"), apply=True) is False
