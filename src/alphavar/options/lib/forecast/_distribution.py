"""``forecast_distribution`` producer (V1-lc, R5): a ``price_series`` frame → ``ForecastResult``.

The autonomous distributional-forecast producer. It **consumes a ``price_series`` interchange frame
explicitly** (P-autonomy: no loading, no series-building, no upstream resolution) and produces the
terminal distribution. Everything it needs — the data step ``dt_years``, the anchor ``as_of``, the
horizon, and ``spot`` — is derived from the frame it is handed. Pure: no I/O, no ``OptionsData``.
"""
from __future__ import annotations

import pandas as pd

from alphavar.options.lib.forecast._base import ForecastResult, ForecastTarget, to_horizon_years
from alphavar.options.lib.forecast._factory import make_engine, make_forecast_model
from alphavar.options.lib.forecast._series import median_dt_years, series_arrays


def forecast_distribution(
    price_series: pd.DataFrame,
    horizon: pd.Timestamp | pd.Timedelta | float,
    *,
    target: ForecastTarget | str = ForecastTarget.PRICE,
    model: str = "gbm",
    engine: str = "montecarlo",
    n: int = 10000,
    seed: int | None = None,
) -> ForecastResult:
    """Forecast the terminal distribution at ``horizon`` from a ``price_series`` frame.

    ``price_series`` — a tidy ``timestamp | price`` frame (the ``price_series`` producer's output).
    ``horizon`` — calendar ACT/365 (``float`` days / ``pd.Timedelta`` / ``pd.Timestamp`` expiration).
    ``model`` / ``engine`` as in the forecast factory. Sets ``ForecastResult.as_of`` to the series'
    last timestamp.
    """
    prices, timestamps = series_arrays(price_series)
    dt_years = median_dt_years(timestamps)
    as_of = timestamps.max()
    horizon_years = to_horizon_years(horizon, as_of)

    forecaster = make_forecast_model(target, model)
    if engine not in forecaster.supports:
        raise ValueError(f"model {model!r} does not support engine {engine!r}; supports {sorted(forecaster.supports)}")
    fitted = forecaster.fit(prices, dt_years, horizon_years)
    result = make_engine(engine, n=n, seed=seed).run(fitted)
    result.as_of = as_of
    return result
