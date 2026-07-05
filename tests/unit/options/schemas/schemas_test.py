import importlib

import pandas as pd
import pytest

from alphavar.options.dictionary import OptionsTerm as C
from alphavar.options.schemas import FuturesHistory, OptionsHistory, validate


def _valid_options_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            C.ASSET_CODE: ["BTC"],
            C.INSTRUMENT_KIND: ["option"],
            C.EXPIRATION_DATE: pd.to_datetime(["2025-04-30"]),
            C.STRIKE: [100000.0],
            C.OPTION_RIGHT: ["call"],
            C.TIMESTAMP: pd.to_datetime(["2025-01-01"]),
            C.PRICE: [0.05],
        }
    )


def test_schema_field_binds_to_registry():
    # R4.4: Model.field resolves to the registry alias string (reference, not repeat).
    assert OptionsHistory.strike == C.STRIKE
    assert OptionsHistory.asset_code == C.ASSET_CODE
    assert FuturesHistory.timestamp == C.TIMESTAMP


def test_valid_frame_passes_and_keeps_extra_columns():
    df = _valid_options_df()
    df["some_extra"] = 1  # strict=False
    out = validate(df, OptionsHistory)
    assert "some_extra" in out.columns


def test_negative_strike_rejected():
    df = _valid_options_df()
    df[C.STRIKE] = -1.0
    with pytest.raises(Exception):
        validate(df, OptionsHistory)


def test_missing_mandatory_column_rejected():
    df = _valid_options_df().drop(columns=[C.OPTION_RIGHT])
    with pytest.raises(Exception):
        validate(df, OptionsHistory)


def test_bad_option_right_value_rejected():
    # 'buy' is a side, not a right (R4.5) — must be rejected.
    df = _valid_options_df()
    df[C.OPTION_RIGHT] = ["buy"]
    with pytest.raises(Exception):
        validate(df, OptionsHistory)


def test_coerce_fixes_dtype():
    df = _valid_options_df()
    df[C.STRIKE] = df[C.STRIKE].astype(str)  # wrong dtype, coerce=True should fix
    out = validate(df, OptionsHistory)
    assert out[C.STRIKE].dtype == float


def test_validation_can_be_disabled(monkeypatch):
    # ALPHAVAR_VALIDATE=0 -> validate() is a no-op (production ETL).
    monkeypatch.setenv("ALPHAVAR_VALIDATE", "0")
    import alphavar.options.schemas as schemas

    importlib.reload(schemas)
    try:
        bad = pd.DataFrame({C.STRIKE: [-1.0]})  # would fail if validated
        assert schemas.validate(bad, schemas.OptionsHistory) is bad
        assert schemas.validation_enabled() is False
    finally:
        monkeypatch.setenv("ALPHAVAR_VALIDATE", "1")
        importlib.reload(schemas)
