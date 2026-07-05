"""V1-lc result-chain price slice — autonomous producers, Disc surface, the flow prototype (ADR 0003).

Covers: each producer in isolation, the interchange schemas, the self-describing Disc surface, and the
minimal flow prototype reproducing the hand-wired chain (and the prior one-call forecast) bit-for-bit.
"""
import numpy as np
import pandas as pd

import alphavar.flow as flow
import alphavar.options.producers  # noqa: F401 — registers the producers onto the Disc surface
from alphavar.core.dictionary import ResultTerm, Term
from alphavar.core.disc import catalog, describe
from alphavar.options.dictionary import OptionsTerm
from alphavar.options.lib.forecast import (
    ForecastResult,
    forecast_distribution,
    make_engine,
    make_forecast_model,
    price_series,
    to_horizon_years,
)
from alphavar.options.lib.forecast._series import median_dt_years
from alphavar.options.option_data_class import OptionsData
from alphavar.options.schemas import (
    ForecastDistributionSchema,
    PriceSeriesSchema,
    validate,
)


def _futures_frame():
    ts = pd.date_range("2026-01-01", periods=200, freq="D", tz="UTC")
    return pd.DataFrame(
        {
            OptionsTerm.TIMESTAMP: ts,
            OptionsTerm.EXPIRATION_DATE: pd.Timestamp("2026-09-01", tz="UTC"),
            OptionsTerm.PRICE: 100.0 + np.arange(200) * 0.1,
        }
    )


# --- P2: price_series producer ----------------------------------------------------------------------
def test_price_series_is_tidy_and_schema_valid():
    px = price_series(_futures_frame(), source="future")
    assert list(px.columns) == [Term.TIMESTAMP, Term.PRICE]
    assert px[Term.TIMESTAMP].is_monotonic_increasing
    # passes the pinned interchange schema (positive price, datetime timestamp)
    validate(px, PriceSeriesSchema)


# --- P3: forecast_distribution producer -------------------------------------------------------------
def test_forecast_distribution_matches_raw_factory_bit_for_bit():
    """The producer reproduces the underlying factory exactly (same seed) — V1 is structural only."""
    px = price_series(_futures_frame(), source="future")
    result = forecast_distribution(px, 30.0, model="gbm", engine="montecarlo", n=5000, seed=0)
    assert isinstance(result, ForecastResult)

    # rebuild the expected result straight from the factory, same inputs/seed
    prices = px[Term.PRICE].to_numpy(dtype=float)
    timestamps = pd.DatetimeIndex(px[Term.TIMESTAMP])
    fitted = make_forecast_model("price", "gbm").fit(
        prices, median_dt_years(timestamps), to_horizon_years(30.0, timestamps.max())
    )
    expected = make_engine("montecarlo", n=5000, seed=0).run(fitted)
    np.testing.assert_array_equal(result.scenarios(), expected.scenarios())
    assert result.as_of == timestamps.max()


def test_to_interchange_schema_and_change_identity():
    px = price_series(_futures_frame(), source="future")
    result = forecast_distribution(px, 30.0, model="gbm", engine="montecarlo", n=5000, seed=0)
    frame = result.to_interchange(quantiles=(0.05, 0.5, 0.95))
    assert list(frame.columns) == [ResultTerm.QUANTILE, ResultTerm.VALUE, ResultTerm.CHANGE]
    # change = value − spot (the shape invariant the owner verifies, D2)
    np.testing.assert_allclose(
        frame[ResultTerm.CHANGE].to_numpy(), frame[ResultTerm.VALUE].to_numpy() - result.spot
    )
    validate(frame, ForecastDistributionSchema)


# --- Disc: the self-describing surface --------------------------------------------------------------
def test_disc_surface_describes_the_producers():
    surface = catalog()
    for kind in ("futures_history", "options_history", "price_series", "forecast_distribution"):
        assert kind in surface

    px_disc = describe("price_series")
    assert px_disc.inputs == (("futures_history", "options_history"),)
    assert px_disc.output_schema is PriceSeriesSchema

    fc_disc = describe("forecast_distribution")
    assert fc_disc.inputs == ("price_series",)
    assert "horizon" in fc_disc.params
    assert ResultTerm.SPOT in fc_disc.scalars  # scalars ride alongside, not in the frame
    assert fc_disc.interchange is not None


# --- flow prototype: the contract-reading interpreter -----------------------------------------------
def test_flow_prototype_reproduces_hand_wired_chain():
    fut = _futures_frame()
    # hand-wired off the same surface (what a user / agent would do)
    expected = forecast_distribution(
        price_series(fut, source="future"), 30.0, model="gbm", engine="montecarlo", n=5000, seed=0
    )
    # the flow prototype, seeding the loaded frame and reading kind→I/O off Disc
    result = flow.run(
        ["price_series", "forecast_distribution"],
        params={
            "price_series": {"source": "future"},
            "forecast_distribution": {
                "horizon": 30.0,
                "model": "gbm",
                "engine": "montecarlo",
                "n": 5000,
                "seed": 0,
            },
        },
        inputs={"futures_history": fut},
    )
    np.testing.assert_array_equal(result.scenarios(), expected.scenarios())
    assert result.as_of == expected.as_of


def test_flow_prototype_runs_the_load_source():
    data = OptionsData(provider=None, asset_code="BTC")
    data.df_fut = _futures_frame()
    result = flow.run(
        ["futures_history", "price_series", "forecast_distribution"],
        params={
            "futures_history": {"data": data},
            "price_series": {"source": "future"},
            "forecast_distribution": {"horizon": 30.0, "model": "gbm", "engine": "montecarlo", "n": 2000, "seed": 0},
        },
    )
    assert isinstance(result, ForecastResult) and result.spot > 0.0


def test_flow_prototype_reports_missing_input():
    import pytest

    with pytest.raises(ValueError, match="not available"):
        flow.run(["forecast_distribution"], params={"forecast_distribution": {"horizon": 30.0}})


# --- vol / smile / surface reduced to producers (same P-autonomy) -----------------------------------
def test_vol_producer_via_forecast_distribution_target():
    """Vol is the scalar-distribution producer with target='vol' (shares the interchange kind)."""
    px = price_series(_futures_frame(), source="future")
    # flow reaches vol through the one forecast_distribution kind, target param = vol
    result = flow.run(
        ["forecast_distribution"],
        params={"forecast_distribution": {"horizon": 30.0, "target": "vol", "model": "ewma", "engine": "analytic"}},
        inputs={"price_series": px},
    )
    assert result.target.value == "vol"
    assert result.point() > 0.0


def _options_history(seed=2):
    """A small multi-strike convex IV smile history (EXCH_MARK_IV) for the smile producer."""
    rng = np.random.default_rng(seed)
    expiration = pd.Timestamp("2026-09-01", tz="UTC")
    strikes = np.linspace(80.0, 120.0, 11)
    k = np.log(strikes / 100.0)
    rows = []
    for day in range(40):
        ts = pd.Timestamp("2026-06-01", tz="UTC") + pd.Timedelta(days=day)
        iv = 0.5 + 0.8 * k**2 + rng.normal(0.0, 1e-4, size=k.size)
        rows.append(
            pd.DataFrame(
                {
                    OptionsTerm.TIMESTAMP: ts,
                    OptionsTerm.EXPIRATION_DATE: expiration,
                    OptionsTerm.STRIKE: strikes,
                    OptionsTerm.UNDERLYING_PRICE: 100.0,
                    OptionsTerm.EXCH_MARK_IV: iv,
                }
            )
        )
    return pd.concat(rows, ignore_index=True)


def test_smile_producer_via_flow_matches_hand_wired():
    from alphavar.options.lib.forecast import forecast_smile

    df = _options_history()
    expected = forecast_smile(df, horizon=14.0, model="param_rw", engine="analytic")
    result = flow.run(
        ["forecast_smile"],
        params={"forecast_smile": {"horizon": 14.0, "model": "param_rw", "engine": "analytic"}},
        inputs={"options_history": df},
    )
    assert result.t_target > 0.0
    np.testing.assert_allclose(
        result.expected_smile().iv(0.0), expected.expected_smile().iv(0.0)
    )


def test_disc_surface_has_smile_and_surface_kinds():
    surface = catalog()
    for kind in ("forecast_smile", "forecast_surface"):
        assert kind in surface
        assert surface[kind].inputs == ("options_history",)
        assert surface[kind].interchange is not None
