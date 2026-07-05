"""Forecast facade component (R3, T27): distributions of a target at a future horizon.

Aggregated by ``Option`` over the shared ``OptionsData`` (like pricer / validation). Pure models and
engines live in ``options.lib.forecast``; **V1-lc (ADR 0003) reduced this facade to autonomous
producers** — each method takes its input frame **explicitly** (a ``price_series`` for price/vol, an
``options_history`` for smile/surface) and delegates to the matching lib producer. It does **not** load
or build its upstream (P-autonomy): the assembler (``alphavar.flow`` / user / agent) wires
``load → price_series → forecast`` off the self-describing Disc surface (``options.producers``).

Targets: **price** / **vol** (scalar distribution → ``ForecastResult``), **smile** (SVI-θ →
``SmileForecast``) and **surface** (constant-maturity θ stack → ``SurfaceForecast``) × engines
``analytic`` / ``montecarlo`` / ``bootstrap``. Horizon is calendar ACT/365 (a ``pd.Timestamp`` = an
expiration).
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from alphavar.options.lib.forecast import (
    ForecastResult,
    ForecastTarget,
    SmileForecast,
    SurfaceForecast,
)
from alphavar.options.lib.forecast import forecast_distribution as _forecast_distribution
from alphavar.options.lib.forecast import forecast_smile as _forecast_smile
from alphavar.options.lib.forecast import forecast_surface as _forecast_surface
from alphavar.options.option_data_class import OptionsData


class OptionsForecast:
    """Distributional forecasts as autonomous producers over explicit input frames (T27, V1-lc)."""

    def __init__(self, data: OptionsData):
        self.data = data

    def forecast_distribution(
        self,
        price_series: pd.DataFrame,
        horizon: pd.Timestamp | pd.Timedelta | float,
        model: str = "gbm",
        engine: str = "montecarlo",
        n: int = 10000,
        seed: int | None = None,
    ) -> ForecastResult:
        """**``forecast_distribution`` producer** — a ``price_series`` frame → a price ``ForecastResult``.

        Consumes the ``price_series`` interchange frame (tidy ``timestamp | price``) **explicitly**: it
        does not load or build the series (P-autonomy). Build it with the ``price_series`` producer
        (``lib.forecast.price_series`` over ``self.data.df_fut`` / ``df_hist``) and pass it in; the
        end-to-end load→series→forecast wiring is the assembler's job. ``horizon`` — calendar ACT/365.
        ``model`` — ``random_walk`` / ``gbm`` / ``garch`` / ``ar1`` / ``empirical``; ``engine`` —
        ``montecarlo`` / ``analytic`` / ``bootstrap``.
        """
        return _forecast_distribution(
            price_series, horizon, target=ForecastTarget.PRICE, model=model, engine=engine, n=n, seed=seed
        )

    def vol(
        self,
        price_series: pd.DataFrame,
        horizon: pd.Timestamp | pd.Timedelta | float,
        model: str = "ewma",
        engine: str = "analytic",
        n: int = 10000,
        seed: int | None = None,
    ) -> ForecastResult:
        """**``vol`` producer** — a ``price_series`` frame → an annualized-vol ``ForecastResult``.

        Same scalar-distribution producer as ``forecast_distribution`` with ``target=vol`` (the vol
        target shares the ``quantile | value | change`` interchange). Consumes the ``price_series``
        frame **explicitly** (P-autonomy); choosing the series (``source`` / ``expiration``) is the
        ``price_series`` producer's job, done upstream by the assembler. ``model`` — ``ewma`` (default) /
        ``garch`` / ``har`` / ``realized``; ``engine`` — ``analytic`` (point) / ``montecarlo`` (garch).
        The result's ``change`` is vs the trailing realized vol.
        """
        return _forecast_distribution(
            price_series, horizon, target=ForecastTarget.VOL, model=model, engine=engine, n=n, seed=seed
        )

    def smile(
        self,
        options_history: pd.DataFrame,
        horizon: pd.Timestamp | pd.Timedelta | float,
        expiration: pd.Timestamp | None = None,
        model: str = "param_rw",
        engine: str = "montecarlo",
        maturity: str = "fixed_expiration",
        smile_model: str = "svi",
        n: int = 10000,
        seed: int | None = None,
        n_components: int = 3,
    ) -> SmileForecast:
        """**``smile`` producer** — an ``options_history`` frame → a ``SmileForecast`` at ``horizon``.

        Consumes the ``options_history`` frame **explicitly** (P-autonomy); the per-timestamp SVI-θ
        calibration + forecast is the producer's internal kernel. ``model`` — ``param_rw`` (default) /
        ``param_var`` / ``param_pca``; ``engine`` — ``montecarlo`` (σ(k) bands) / ``analytic``.
        ``maturity`` — ``fixed_expiration`` / ``constant_maturity``. ``horizon`` is calendar ACT/365.
        """
        return _forecast_smile(
            options_history,
            horizon,
            expiration=expiration,
            model=model,
            engine=engine,
            maturity=maturity,
            smile_model=smile_model,
            n=n,
            seed=seed,
            n_components=n_components,
        )

    def surface(
        self,
        options_history: pd.DataFrame,
        horizon: pd.Timestamp | pd.Timedelta | float,
        tenor_nodes: np.ndarray | None = None,
        model: str = "svi_surface",
        engine: str = "montecarlo",
        smile_model: str = "svi",
        n: int = 10000,
        seed: int | None = None,
        n_components: int = 5,
    ) -> SurfaceForecast:
        """**``surface`` producer** — an ``options_history`` frame → a ``SurfaceForecast`` at ``horizon``.

        Consumes the ``options_history`` frame **explicitly** (P-autonomy); the constant-maturity θ-stack
        build + forecast is the producer's kernel. ``model`` — ``svi_surface`` (default) /
        ``svi_surface_var`` / ``pca_factor``; ``engine`` — ``montecarlo`` (σ(k,τ) bands) / ``analytic``.
        ``horizon`` is calendar ACT/365.
        """
        return _forecast_surface(
            options_history,
            horizon,
            tenor_nodes=tenor_nodes,
            model=model,
            engine=engine,
            smile_model=smile_model,
            n=n,
            seed=seed,
            n_components=n_components,
        )
