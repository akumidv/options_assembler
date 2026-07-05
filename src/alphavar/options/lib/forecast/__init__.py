"""Forecast model factory (T27, R5): distributions of a target at a future horizon.

Three orthogonal axes — **target** (``ForecastTarget``), **process/model** (``ForecastModel`` via
``make_forecast_model``) and **engine** (``make_engine``: ``analytic`` / ``montecarlo``) — produce a
distributional ``ForecastResult`` (point / quantiles / scenarios / change). Iteration 1 covers the
``price`` target (``random_walk`` / ``gbm`` / ``garch``). The DataFrame-level facade is
``options.forecast_class.OptionsForecast``.
"""
from alphavar.options.lib.forecast._base import (
    FittedProcess,
    ForecastEngine,
    ForecastModel,
    ForecastResult,
    ForecastTarget,
    to_horizon_years,
)
from alphavar.options.lib.forecast._distribution import forecast_distribution
from alphavar.options.lib.forecast._factory import (
    DEFAULT_ENGINE,
    DEFAULT_PRICE_MODEL,
    make_engine,
    make_forecast_model,
)
from alphavar.options.lib.forecast._producers import forecast_smile, forecast_surface
from alphavar.options.lib.forecast._series import (
    median_dt_years,
    price_series,
    series_arrays,
)
from alphavar.options.lib.forecast.smile import (
    SmileForecast,
    make_smile_engine,
    make_smile_forecast_model,
)
from alphavar.options.lib.forecast.surface import (
    SurfaceForecast,
    make_surface_engine,
    make_surface_forecast_model,
)

__all__ = [
    "ForecastTarget",
    "ForecastModel",
    "FittedProcess",
    "ForecastEngine",
    "ForecastResult",
    "to_horizon_years",
    "make_forecast_model",
    "make_engine",
    "forecast_distribution",
    "forecast_smile",
    "forecast_surface",
    "price_series",
    "series_arrays",
    "median_dt_years",
    "DEFAULT_PRICE_MODEL",
    "DEFAULT_ENGINE",
    "SmileForecast",
    "make_smile_forecast_model",
    "make_smile_engine",
    "SurfaceForecast",
    "make_surface_forecast_model",
    "make_surface_engine",
]
