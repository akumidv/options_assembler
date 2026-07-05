"""Smile- / surface-target forecast **producers** (V1-lc, ADR 0003): options history → forecast.

The autonomous parameter-vector forecast producers, mirroring ``forecast_distribution`` (the scalar
price/vol producer): each **consumes an ``options_history`` frame explicitly** (P-autonomy — no loading,
no upstream resolution) and returns the terminal forecast (a ``SmileForecast`` / ``SurfaceForecast``).
The θ-history build (per-timestamp SVI calibration, cross-expiration interpolation) is the producer's
internal kernel; the assembler (``flow`` / user / agent) only hands the frame in.

Lives at the ``lib/forecast`` level (not inside ``smile/`` / ``surface/``) so it can read both
subpackages without an import cycle: it imports the ``smile`` / ``surface`` package roots, which depend
only on their own leaf modules (never back on this package root), so sibling import order is irrelevant.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from alphavar.options.dictionary import OptionsTerm
from alphavar.options.lib.forecast._base import to_horizon_years
from alphavar.options.lib.forecast._series import median_dt_years
from alphavar.options.lib.forecast.smile import (
    MaturityConvention,
    SmileForecast,
    build_theta_history,
    default_expiration,
    make_smile_engine,
    make_smile_forecast_model,
    resolve_maturity,
)
from alphavar.options.lib.forecast.surface import (
    DEFAULT_TENOR_NODES,
    SurfaceForecast,
    constant_maturity_theta_history,
    make_surface_engine,
    make_surface_forecast_model,
)

_DAYS_PER_YEAR = 365.0


def forecast_smile(
    options_history: pd.DataFrame,
    horizon: pd.Timestamp | pd.Timedelta | float,
    *,
    expiration: pd.Timestamp | None = None,
    model: str = "param_rw",
    engine: str = "montecarlo",
    maturity: str = "fixed_expiration",
    smile_model: str = "svi",
    n: int = 10000,
    seed: int | None = None,
    n_components: int = 3,
) -> SmileForecast:
    """**Autonomous ``forecast_smile`` producer**: an ``options_history`` frame → a ``SmileForecast``.

    Calibrates the SVI θ=(a,b,ρ,m,σ) of ``expiration`` (default the most-populated) per timestamp over
    the history handed in, forecasts θ forward, and decodes the terminal θ to a smile at the target
    tenor τ = E − (as_of + horizon). ``maturity`` — ``fixed_expiration`` (model one expiration's θ) /
    ``constant_maturity`` (model a single fixed target tenor, cross-expiration interpolated). It neither
    loads nor resolves its upstream (P-autonomy); the caller passes the frame in. ``horizon`` is
    calendar ACT/365.
    """
    maturity_conv = resolve_maturity(maturity)
    df_hist = options_history
    chosen_exp = default_expiration(df_hist) if expiration is None else pd.Timestamp(expiration)
    as_of = df_hist[OptionsTerm.TIMESTAMP].max()
    horizon_years = to_horizon_years(horizon, as_of)
    target_at = as_of + pd.Timedelta(days=horizon_years * _DAYS_PER_YEAR)
    t_target = (chosen_exp - target_at).total_seconds() / (_DAYS_PER_YEAR * 86400.0)
    if t_target <= 0.0:
        raise ValueError(
            f"expiration {chosen_exp.date()} is at/before the horizon ({target_at.date()}); "
            "the smile has expired by then — choose a later expiration or a shorter horizon"
        )

    if maturity_conv is MaturityConvention.CONSTANT_MATURITY:
        theta, timestamps, _ = constant_maturity_theta_history(df_hist, np.array([t_target]), smile_model)
    else:
        theta, timestamps, _ = build_theta_history(df_hist, chosen_exp, smile_model)
    dt_years = median_dt_years(timestamps)

    forecaster = make_smile_forecast_model(model, n_components=n_components)
    if engine not in forecaster.supports:
        raise ValueError(f"model {model!r} does not support engine {engine!r}; supports {sorted(forecaster.supports)}")
    fitted = forecaster.fit(theta, dt_years, horizon_years, t_target)
    return make_smile_engine(engine, n=n, seed=seed).run(fitted)


def forecast_surface(
    options_history: pd.DataFrame,
    horizon: pd.Timestamp | pd.Timedelta | float,
    *,
    tenor_nodes: np.ndarray | None = None,
    model: str = "svi_surface",
    engine: str = "montecarlo",
    smile_model: str = "svi",
    n: int = 10000,
    seed: int | None = None,
    n_components: int = 5,
) -> SurfaceForecast:
    """**Autonomous ``forecast_surface`` producer**: an ``options_history`` frame → a ``SurfaceForecast``.

    Builds SVI θ stacked across constant-maturity ``tenor_nodes`` (default 1w/2w/1m/2m/3m) per timestamp
    by interpolating total variance across expirations, forecasts the stacked θ forward, and decodes it
    back to a surface σ(k,τ). It neither loads nor resolves its upstream (P-autonomy). ``horizon`` is
    calendar ACT/365.
    """
    nodes = DEFAULT_TENOR_NODES if tenor_nodes is None else tenor_nodes
    theta, timestamps, resolved_nodes = constant_maturity_theta_history(options_history, nodes, smile_model)
    as_of = timestamps.max()
    horizon_years = to_horizon_years(horizon, as_of)
    dt_years = median_dt_years(timestamps)

    forecaster = make_surface_forecast_model(model, n_components=n_components)
    if engine not in forecaster.supports:
        raise ValueError(f"model {model!r} does not support engine {engine!r}; supports {sorted(forecaster.supports)}")
    # each node is forecast at its own constant maturity (held fixed), so the decode reads each node at
    # its node tenor; t_target only seeds the shared process scale.
    fitted = forecaster.fit(theta, dt_years, horizon_years, t_target=float(resolved_nodes[0]))
    return make_surface_engine(engine, resolved_nodes, n=n, seed=seed).run(fitted)
