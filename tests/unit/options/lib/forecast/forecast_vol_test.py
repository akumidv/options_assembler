"""Volatility forecast models (T27 iteration 2): realized / ewma / garch / har."""

import numpy as np
import pandas as pd

from alphavar.options.dictionary import OptionsTerm
from alphavar.options.forecast_class import OptionsForecast
from alphavar.options.lib.forecast import ForecastTarget, price_series
from alphavar.options.lib.forecast.engine.analytic import AnalyticEngine
from alphavar.options.lib.forecast.engine.montecarlo import MonteCarloEngine
from alphavar.options.lib.forecast.vol.ewma import EwmaVol, ewma_variance
from alphavar.options.lib.forecast.vol.garch import GarchVol
from alphavar.options.lib.forecast.vol.har import HarVol
from alphavar.options.lib.forecast.vol.realized import RealizedVol
from alphavar.options.option_data_class import OptionsData

_DT = 1.0 / 365.0


def _series(n=750, mu=0.0, sigma=0.4, s0=100.0, seed=0):
    rng = np.random.default_rng(seed)
    r = (mu - 0.5 * sigma**2) * _DT + sigma * np.sqrt(_DT) * rng.standard_normal(n)
    return s0 * np.exp(np.cumsum(r))


def test_realized_vol_matches_annualized_sample_std():
    prices = _series(sigma=0.5)
    result = AnalyticEngine().run(RealizedVol().fit(prices, _DT, 30.0 / 365.0))
    r = np.diff(np.log(prices))
    expected = np.sqrt(np.var(r, ddof=1) / _DT)
    assert np.isclose(result.point(), expected)
    assert np.isclose(result.change(), 0.0)  # realized: forecast == reference


def test_ewma_variance_recursion():
    r = np.array([0.01, -0.02, 0.015, -0.005])
    var = float(np.var(r))
    for x in r:
        var = 0.94 * var + 0.06 * x * x
    assert np.isclose(ewma_variance(r, 0.94), var)


def test_ewma_forecast_is_positive_point():
    result = AnalyticEngine().run(EwmaVol().fit(_series(), _DT, 30.0 / 365.0))
    assert result.target is ForecastTarget.VOL
    assert result.point() > 0.0
    assert np.allclose(result.quantiles([0.1, 0.9]), result.point())  # degenerate (point) forecast


def test_garch_vol_analytic_and_montecarlo():
    fitted = GarchVol().fit(_series(seed=3), _DT, 30.0 / 365.0)
    analytic = AnalyticEngine().run(fitted)
    mc = MonteCarloEngine(n=20_000, seed=1).run(fitted)
    assert analytic.point() > 0.0
    assert np.all(mc.scenarios() > 0.0)
    # MC realized-vol mean ≈ the analytic expected vol (same √(mean variance) ballpark)
    assert np.isclose(analytic.point(), mc.point(), rtol=0.15)


def test_garch_vol_supports_both_engines():
    assert GarchVol().supports == frozenset({"analytic", "montecarlo"})
    assert EwmaVol().supports == frozenset({"analytic"})


def test_har_runs_and_falls_back_on_short_history():
    long_result = AnalyticEngine().run(HarVol().fit(_series(n=400), _DT, 20.0 / 365.0))
    assert long_result.point() > 0.0
    short = _series(n=5)  # below the monthly window → realized fallback, no crash
    short_result = AnalyticEngine().run(HarVol().fit(short, _DT, 20.0 / 365.0))
    assert short_result.point() > 0.0


def test_facade_vol_returns_distribution():
    ts = pd.date_range("2026-01-01", periods=300, freq="D", tz="UTC")
    rng = np.random.default_rng(0)
    prices = 100.0 * np.exp(np.cumsum(0.3 * np.sqrt(_DT) * rng.standard_normal(300)))
    df_fut = pd.DataFrame(
        {
            OptionsTerm.TIMESTAMP: ts,
            OptionsTerm.EXPIRATION_DATE: pd.Timestamp("2026-12-01", tz="UTC"),
            OptionsTerm.PRICE: prices,
        }
    )
    data = OptionsData(provider=None, asset_code="BTC")
    data.df_fut = df_fut
    px = price_series(data.df_fut, source="future")
    result = OptionsForecast(data).vol(px, 30.0, model="ewma")
    assert result.target is ForecastTarget.VOL
    assert result.point() > 0.0
    assert list(result.to_frame().columns) == ["quantile", "vol", "change"]
