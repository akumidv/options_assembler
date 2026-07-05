"""EtlHistory._fold_reference: keep the SCD reference sidecar current on history write (T25 inc.4B)."""

import pandas as pd

from alphavar.io.exchange.exchange_entities import ExchangeCode
from alphavar.io.provider import read_reference
from alphavar.options.dictionary import OptionsTerm, Timeframe
from alphavar.options.etl.etl_updates_to_history import EtlHistory

T0 = pd.Timestamp("2025-01-01", tz="UTC")
T1 = pd.Timestamp("2025-02-01", tz="UTC")
EXP = pd.Timestamp("2025-03-28", tz="UTC")


def _wide(when, style="european"):
    """A two-contract wide options batch observed at `when`."""
    return pd.DataFrame(
        {
            OptionsTerm.ASSET_CODE: ["BTC"] * 2,
            OptionsTerm.INSTRUMENT_KIND: ["option"] * 2,
            OptionsTerm.TIMESTAMP: [when, when],
            OptionsTerm.EXPIRATION_DATE: [EXP, EXP],
            OptionsTerm.STRIKE: [100000.0, 90000.0],
            OptionsTerm.OPTION_RIGHT: ["call", "put"],
            OptionsTerm.OPTION_STYLE: [style, style],
            OptionsTerm.EXCH_SYMBOL: ["BTC-28MAR25-100000-C", "BTC-28MAR25-90000-P"],
            OptionsTerm.PRICE: [0.1, 0.2],
        }
    )


def _etl(tmp_path):
    return EtlHistory(
        exchange_code=ExchangeCode.DERIBIT,
        history_path=str(tmp_path),
        update_path=str(tmp_path),
        timeframe=Timeframe.EOD,
    )


def test_fold_reference_writes_sidecar(tmp_path):
    etl = _etl(tmp_path)
    etl._fold_reference(_wide(T0), "BTC")

    asset, history = read_reference(str(tmp_path / "DERIBIT" / "BTC"))
    assert asset.asset_code == "BTC"
    assert len(history) == 2  # one open version per contract
    assert history[OptionsTerm.VALID_TO].isna().all()


def test_fold_reference_appends_on_attribute_change(tmp_path):
    etl = _etl(tmp_path)
    etl._fold_reference(_wide(T0, style="european"), "BTC")
    etl._fold_reference(_wide(T1, style="american"), "BTC")  # option_style flipped

    _asset, history = read_reference(str(tmp_path / "DERIBIT" / "BTC"))
    # each of the 2 contracts now has a closed european version + an open american one
    assert len(history) == 4
    open_rows = history[history[OptionsTerm.VALID_TO].isna()]
    assert (open_rows[OptionsTerm.OPTION_STYLE] == "american").all()
    closed = history[history[OptionsTerm.VALID_TO].notna()]
    assert (closed[OptionsTerm.VALID_TO] == T1).all()


def test_fold_reference_unchanged_is_noop(tmp_path):
    etl = _etl(tmp_path)
    etl._fold_reference(_wide(T0), "BTC")
    etl._fold_reference(_wide(T1), "BTC")  # same attributes

    _asset, history = read_reference(str(tmp_path / "DERIBIT" / "BTC"))
    assert len(history) == 2  # no new versions


def test_fold_reference_skips_non_options(tmp_path):
    etl = _etl(tmp_path)
    futures = pd.DataFrame({OptionsTerm.ASSET_CODE: ["BTC"], OptionsTerm.PRICE: [1.0]})  # no strike
    etl._fold_reference(futures, "BTC")

    asset, history = read_reference(str(tmp_path / "DERIBIT" / "BTC"))
    assert asset is None and history.empty  # nothing written


def test_to_stored_series_slims_only_when_enabled(tmp_path):
    wide = _wide(T0)
    off = _etl(tmp_path)  # slim_series default False
    pd.testing.assert_frame_equal(off._to_stored_series(wide), wide)  # no-op

    on = EtlHistory(
        exchange_code=ExchangeCode.DERIBIT,
        history_path=str(tmp_path),
        update_path=str(tmp_path),
        timeframe=Timeframe.EOD,
        params={"slim_series": True},
    )
    slim = on._to_stored_series(wide)
    assert OptionsTerm.EXCH_SYMBOL not in slim.columns  # reference columns dropped
    assert OptionsTerm.ASSET_CODE not in slim.columns
    assert OptionsTerm.PRICE in slim.columns and OptionsTerm.STRIKE in slim.columns  # series + key kept


def test_slim_series_requires_update_reference(tmp_path):
    # slimming without writing the reference would be lossy -> the guard disables it
    etl = EtlHistory(
        exchange_code=ExchangeCode.DERIBIT,
        history_path=str(tmp_path),
        update_path=str(tmp_path),
        timeframe=Timeframe.EOD,
        params={"slim_series": True, "update_reference": False},
    )
    assert etl._slim_series is False
    pd.testing.assert_frame_equal(etl._to_stored_series(_wide(T0)), _wide(T0))
