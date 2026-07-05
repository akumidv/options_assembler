"""Smile-target forecast (T27 iteration 3): θ models / engines / decode / facade."""

import numpy as np
import pandas as pd
import pytest

from alphavar.options.dictionary import OptionsTerm
from alphavar.options.forecast_class import OptionsForecast
from alphavar.options.lib.forecast import make_forecast_model
from alphavar.options.lib.forecast._base import ForecastTarget
from alphavar.options.lib.forecast.smile import (
    SMILE_PARAM_NAMES,
    MaturityConvention,
    ParamPCA,
    ParamRandomWalk,
    ParamVAR1,
    build_theta_history,
    decode_smile,
    make_smile_engine,
    make_smile_forecast_model,
    resolve_maturity,
)
from alphavar.options.lib.forecast.smile._engine import SmileAnalyticEngine, SmileMonteCarloEngine
from alphavar.options.lib.pricer.smile.svi import _raw_w
from alphavar.options.option_data_class import OptionsData

_DT = 1.0 / 365.0
# A well-behaved SVI slice (a, b, rho, m, sigma); butterfly-free at t≈0.25.
_THETA = np.array([0.04, 0.1, -0.3, 0.0, 0.2])


def _svi_iv(k, theta, t):
    a, b, rho, m, sigma = theta
    return np.sqrt(np.maximum(_raw_w(np.asarray(k, float), a, b, rho, m, sigma), 0.0) / t)


def _theta_history(n=60, seed=0, drift=0.0):
    """A random-walk θ history around the reference (slow drift in `a`)."""
    rng = np.random.default_rng(seed)
    steps = rng.normal(0.0, [0.002, 0.004, 0.02, 0.01, 0.01], size=(n, 5))
    steps[:, 0] += drift
    return _THETA + np.cumsum(steps, axis=0)


# --- decode + sampler ----------------------------------------------------------------------

def test_decode_smile_round_trips_iv():
    smile = decode_smile(_THETA, SMILE_PARAM_NAMES, t_years=0.25)
    k = np.linspace(-0.4, 0.4, 9)
    assert np.allclose(smile.iv(k), _svi_iv(k, _THETA, 0.25), atol=1e-9)
    assert smile.is_butterfly_free()


def test_decode_clamps_out_of_domain_params():
    smile = decode_smile(np.array([0.04, -1.0, 5.0, 0.0, -0.2]), SMILE_PARAM_NAMES, 0.25)
    assert np.all(smile.iv(np.linspace(-0.5, 0.5, 11)) >= 0.0)  # b≥0, |ρ|<1, σ>0 clamp + w floor


# --- models --------------------------------------------------------------------------------

def test_param_rw_is_driftless_with_scaled_cov():
    theta = _theta_history()
    fitted = ParamRandomWalk().fit(theta, _DT, 10.0 / 365.0, t_target=0.25)
    assert np.allclose(fitted.mean_terminal_theta(), theta[-1])  # driftless: mean = last θ
    one_step = ParamRandomWalk().fit(theta, _DT, _DT, t_target=0.25)
    # terminal cov scales with the number of steps (10 vs 1)
    assert np.allclose(fitted.cov, 10.0 * one_step.cov, rtol=1e-6)


def test_param_var_reverts_and_falls_back_when_short():
    fitted = ParamVAR1().fit(_theta_history(n=80, seed=2), _DT, 5.0 / 365.0, t_target=0.25)
    assert fitted.mean.shape == (5,) and np.all(np.isfinite(fitted.mean))
    short = ParamVAR1().fit(_THETA[None, :] + np.zeros((4, 5)), _DT, 5.0 / 365.0, t_target=0.25)
    assert np.allclose(short.mean, _THETA)  # under-identified → driftless RW fallback


def test_param_pca_cov_is_rank_limited():
    fitted = ParamPCA(n_components=2).fit(_theta_history(seed=5), _DT, 8.0 / 365.0, t_target=0.25)
    assert np.linalg.matrix_rank(fitted.cov, tol=1e-12) <= 2
    assert np.allclose(fitted.mean, fitted.theta0)  # driftless centre


# --- engines + result ----------------------------------------------------------------------

def test_analytic_engine_has_no_scenario_bands():
    fitted = ParamRandomWalk().fit(_theta_history(), _DT, 10.0 / 365.0, t_target=0.25)
    forecast = SmileAnalyticEngine().run(fitted)
    k = np.linspace(-0.3, 0.3, 7)
    bands = forecast.iv_quantiles(k, (0.05, 0.95))
    assert bands.shape == (2, 7)
    assert np.allclose(bands[0], bands[1])  # degenerate (no scenarios)
    assert forecast.scenario_smiles() == []


def test_montecarlo_engine_widens_bands_and_to_frame():
    fitted = ParamRandomWalk().fit(_theta_history(), _DT, 30.0 / 365.0, t_target=0.25)
    forecast = SmileMonteCarloEngine(n=4000, seed=1).run(fitted)
    k = np.linspace(-0.3, 0.3, 7)
    lo, hi = forecast.iv_quantiles(k, (0.05, 0.95))
    assert np.all(hi >= lo) and np.any(hi > lo)  # MC produces a non-degenerate band
    assert len(forecast.scenario_smiles()) == 4000
    frame = forecast.to_frame(quantiles=(0.05, 0.5, 0.95))
    assert list(frame.columns) == ["k", "iv", "iv_q0.05", "iv_q0.5", "iv_q0.95"]


# --- factory -------------------------------------------------------------------------------

def test_smile_factory_models_and_engines():
    assert isinstance(make_smile_forecast_model(), ParamRandomWalk)  # default
    assert isinstance(make_smile_forecast_model("param_var"), ParamVAR1)
    assert make_smile_forecast_model("param_pca", n_components=2).n_components == 2
    assert isinstance(make_smile_engine(), SmileMonteCarloEngine)
    assert isinstance(make_smile_engine("analytic"), SmileAnalyticEngine)


def test_smile_factory_rejects_unknown_and_planned():
    with pytest.raises(ValueError):
        make_smile_forecast_model("nope")
    with pytest.raises(NotImplementedError):
        make_smile_engine("bootstrap")
    # both maturity conventions are now built (A fixed_expiration, B constant_maturity)
    assert resolve_maturity(MaturityConvention.CONSTANT_MATURITY) is MaturityConvention.CONSTANT_MATURITY
    # the scalar factory points smile at its dedicated factory
    with pytest.raises(NotImplementedError):
        make_forecast_model(ForecastTarget.SMILE, "param_rw")


# --- end-to-end via the facade -------------------------------------------------------------

def _options_history(n_days=40, seed=0):
    rng = np.random.default_rng(seed)
    expiration = pd.Timestamp("2026-09-01", tz="UTC")
    strikes = np.linspace(80.0, 120.0, 11)
    theta_path = _theta_history(n=n_days, seed=seed)
    rows = []
    for day in range(n_days):
        ts = pd.Timestamp("2026-06-01", tz="UTC") + pd.Timedelta(days=day)
        t = (expiration - ts).total_seconds() / (365.0 * 86400.0)
        forward = 100.0
        k = np.log(strikes / forward)
        iv = _svi_iv(k, theta_path[day], t) + rng.normal(0.0, 1e-4, size=k.size)
        rows.append(
            pd.DataFrame(
                {
                    OptionsTerm.TIMESTAMP: ts,
                    OptionsTerm.EXPIRATION_DATE: expiration,
                    OptionsTerm.STRIKE: strikes,
                    OptionsTerm.UNDERLYING_PRICE: forward,
                    OptionsTerm.EXCH_MARK_IV: iv,
                }
            )
        )
    return pd.concat(rows, ignore_index=True)


def test_build_theta_history_recovers_known_slice():
    df = _options_history()
    theta, timestamps, expiration = build_theta_history(df, expiration=None)
    assert theta.shape == (40, 5)
    assert len(timestamps) == 40
    assert expiration == pd.Timestamp("2026-09-01", tz="UTC")
    # the first slice's fitted θ recovers the generating θ closely
    assert np.allclose(theta[0], _theta_history()[0], atol=5e-3)


def test_facade_smile_end_to_end():
    data = OptionsData(provider=None, asset_code="BTC")
    data.df_hist = _options_history(seed=3)
    forecast = OptionsForecast(data).smile(
        data.df_hist, horizon=14.0, model="param_rw", engine="montecarlo", n=2000, seed=0
    )
    assert forecast.t_target > 0.0
    assert forecast.is_butterfly_free()
    assert forecast.expected_smile().iv(0.0) > 0.0
    frame = forecast.to_frame()
    assert (frame["iv_q0.95"] >= frame["iv_q0.05"]).all()


def test_facade_smile_rejects_expired_horizon():
    data = OptionsData(provider=None, asset_code="BTC")
    data.df_hist = _options_history()
    with pytest.raises(ValueError, match="expired"):
        OptionsForecast(data).smile(data.df_hist, horizon=400.0)  # past the 2026-09-01 expiration


def test_facade_smile_all_models_and_analytic_engine():
    data = OptionsData(provider=None, asset_code="BTC")
    data.df_hist = _options_history(seed=7)
    forecaster = OptionsForecast(data)
    for model in ("param_rw", "param_var", "param_pca"):
        result = forecaster.smile(data.df_hist, horizon=10.0, model=model, engine="analytic")
        assert result.engine == "analytic"
        assert result.expected_smile().iv(0.0) > 0.0
