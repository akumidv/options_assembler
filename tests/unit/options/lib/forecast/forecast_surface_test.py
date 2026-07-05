"""Surface-target forecast (T27 iteration 4) + the constant_maturity smile convention (B)."""

import numpy as np
import pandas as pd
import pytest

from alphavar.options.dictionary import OptionsTerm
from alphavar.options.forecast_class import OptionsForecast
from alphavar.options.lib.forecast.smile._decode import decode_smile
from alphavar.options.lib.forecast.smile.param_pca import ParamPCA
from alphavar.options.lib.forecast.smile.param_rw import ParamRandomWalk
from alphavar.options.lib.forecast.surface import (
    DEFAULT_TENOR_NODES,
    SurfaceForecast,
    constant_maturity_theta_history,
    decode_surface,
    interp_total_variance,
    make_surface_engine,
    make_surface_forecast_model,
)
from alphavar.options.lib.forecast.surface._engine import SurfaceAnalyticEngine, SurfaceMonteCarloEngine
from alphavar.options.lib.pricer.smile.svi import _raw_w
from alphavar.options.option_data_class import OptionsData

_DAYS = 365.0
# An ATM-ish SVI θ scaled so total variance grows with tenor (calendar-monotone).
_THETA = np.array([0.04, 0.1, -0.3, 0.0, 0.2])


def _svi_iv(k, theta, t):
    a, b, rho, m, sigma = theta
    return np.sqrt(np.maximum(_raw_w(np.asarray(k, float), a, b, rho, m, sigma), 0.0) / t)


# --- interpolation -------------------------------------------------------------------------

def test_interp_total_variance_linear_and_extrapolation():
    tenors = np.array([0.25, 0.5])
    w = np.array([[0.04], [0.08]])  # ATM total variance ∝ tenor
    assert np.isclose(interp_total_variance(tenors, w, 0.375)[0], 0.06)  # midpoint
    assert np.isclose(interp_total_variance(tenors, w, 0.125)[0], 0.02)  # below → flat w/τ rate
    assert np.isclose(interp_total_variance(tenors, w, 1.0)[0], 0.16)  # above → flat w/τ rate


def test_interp_single_tenor_flat_rate():
    assert np.isclose(interp_total_variance(np.array([0.5]), np.array([[0.08]]), 0.25)[0], 0.04)


# --- decode + result -----------------------------------------------------------------------

def test_decode_surface_round_trips_per_node():
    nodes = np.array([0.1, 0.25, 0.5])
    stacked = np.concatenate([_THETA, _THETA, _THETA])
    surface = decode_surface(stacked, nodes)
    k = np.linspace(-0.3, 0.3, 7)
    for tau in nodes:
        assert np.allclose(surface.iv(k, tau), decode_smile(_THETA, ("a", "b", "rho", "m", "sigma"), tau).iv(k))
    assert surface.is_butterfly_free()


def test_decode_surface_rejects_wrong_size():
    with pytest.raises(ValueError):
        decode_surface(np.zeros(11), np.array([0.1, 0.25]))  # 2 nodes need 10 params


# --- models + engines ----------------------------------------------------------------------

def test_surface_factory_maps_to_theta_models():
    assert isinstance(make_surface_forecast_model(), ParamRandomWalk)  # svi_surface default
    assert isinstance(make_surface_forecast_model("pca_factor", n_components=4), ParamPCA)
    with pytest.raises(ValueError):
        make_surface_forecast_model("nope")
    nodes = np.array([0.1, 0.25])
    assert isinstance(make_surface_engine("analytic", nodes), SurfaceAnalyticEngine)
    assert isinstance(make_surface_engine("montecarlo", nodes), SurfaceMonteCarloEngine)
    with pytest.raises(NotImplementedError):
        make_surface_engine("bootstrap", nodes)


def test_stacked_theta_process_to_surface_forecast():
    nodes = np.array([0.1, 0.25, 0.5])
    # a stacked-θ history: 3 nodes × 5 params = 15-dim, small random walk around the reference
    rng = np.random.default_rng(0)
    base = np.concatenate([_THETA, _THETA, _THETA])
    history = base + np.cumsum(rng.normal(0, 1e-3, size=(40, 15)), axis=0)
    fitted = ParamRandomWalk().fit(history, 1.0 / 365.0, 10.0 / 365.0, t_target=0.25)
    forecast = SurfaceMonteCarloEngine(nodes, n=1500, seed=1).run(fitted)
    assert isinstance(forecast, SurfaceForecast)
    k = np.linspace(-0.3, 0.3, 7)
    lo, hi = forecast.iv_quantiles(k, 0.2, (0.05, 0.95))
    assert np.all(hi >= lo)
    frame = forecast.to_frame(tenors=nodes, k_grid=k)
    assert set(frame.columns) >= {"tenor", "k", "iv"}
    assert frame["tenor"].nunique() == 3


# --- end-to-end via the facade -------------------------------------------------------------

def _multi_expiry_history(n_days=24, seed=0):
    rng = np.random.default_rng(seed)
    strikes = np.linspace(80.0, 120.0, 11)
    expirations = [pd.Timestamp("2026-07-15", tz="UTC"), pd.Timestamp("2026-09-01", tz="UTC"),
                   pd.Timestamp("2026-11-01", tz="UTC")]
    rows = []
    for day in range(n_days):
        ts = pd.Timestamp("2026-06-01", tz="UTC") + pd.Timedelta(days=day)
        theta = _THETA + rng.normal(0, [0.002, 0.003, 0.01, 0.005, 0.005])
        for exp in expirations:
            t = (exp - ts).total_seconds() / (_DAYS * 86400.0)
            if t <= 0:
                continue
            forward = 100.0
            k = np.log(strikes / forward)
            iv = _svi_iv(k, theta, t) + rng.normal(0, 1e-4, size=k.size)
            rows.append(
                pd.DataFrame(
                    {
                        OptionsTerm.TIMESTAMP: ts,
                        OptionsTerm.EXPIRATION_DATE: exp,
                        OptionsTerm.STRIKE: strikes,
                        OptionsTerm.UNDERLYING_PRICE: forward,
                        OptionsTerm.EXCH_MARK_IV: iv,
                    }
                )
            )
    return pd.concat(rows, ignore_index=True)


def test_constant_maturity_theta_history_shape():
    df = _multi_expiry_history()
    nodes = np.array([14.0, 30.0, 60.0]) / _DAYS
    theta, timestamps, resolved = np.array([]), None, None
    theta, timestamps, resolved = constant_maturity_theta_history(df, nodes)
    assert theta.shape == (24, 15)  # 3 nodes × 5 params
    assert len(timestamps) == 24
    assert np.allclose(resolved, nodes)


def test_facade_surface_end_to_end():
    data = OptionsData(provider=None, asset_code="BTC")
    data.df_hist = _multi_expiry_history(seed=2)
    forecast = OptionsForecast(data).surface(
        data.df_hist, horizon=10.0, tenor_nodes=np.array([14.0, 30.0, 60.0]) / _DAYS, n=1000, seed=0
    )
    assert isinstance(forecast, SurfaceForecast)
    assert forecast.is_butterfly_free()
    assert forecast.iv(0.0, 30.0 / _DAYS) > 0.0


def test_facade_surface_default_nodes_and_analytic():
    data = OptionsData(provider=None, asset_code="BTC")
    data.df_hist = _multi_expiry_history(seed=4)
    forecast = OptionsForecast(data).surface(data.df_hist, horizon=7.0, engine="analytic", model="pca_factor")
    assert forecast.engine == "analytic"
    assert np.allclose(forecast.tenor_nodes, DEFAULT_TENOR_NODES)
    assert forecast.scenario_surfaces() == []


def test_facade_smile_constant_maturity_convention():
    data = OptionsData(provider=None, asset_code="BTC")
    data.df_hist = _multi_expiry_history(seed=6)
    forecast = OptionsForecast(data).smile(
        data.df_hist, horizon=10.0, maturity="constant_maturity", engine="analytic"
    )
    assert forecast.t_target > 0.0
    assert forecast.expected_smile().iv(0.0) > 0.0
