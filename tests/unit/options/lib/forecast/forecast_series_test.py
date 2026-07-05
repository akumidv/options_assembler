"""Price-series extraction + the OptionsForecast facade (T27)."""

import numpy as np
import pandas as pd

from alphavar.options.dictionary import OptionsTerm
from alphavar.options.forecast_class import OptionsForecast
from alphavar.options.lib.forecast import ForecastResult, price_series
from alphavar.options.lib.forecast._series import (
    front_price_series,
    futures_price_series,
    median_dt_years,
    underlying_price_series,
)
from alphavar.options.option_data_class import OptionsData


def _futures_frame():
    """Two expirations; the near one is the most-populated (default series)."""
    ts = pd.date_range("2026-01-01", periods=200, freq="D", tz="UTC")
    near = pd.DataFrame(
        {
            OptionsTerm.TIMESTAMP: ts,
            OptionsTerm.EXPIRATION_DATE: pd.Timestamp("2026-09-01", tz="UTC"),
            OptionsTerm.PRICE: 100.0 + np.arange(200) * 0.1,
        }
    )
    far = pd.DataFrame(
        {
            OptionsTerm.TIMESTAMP: ts[:20],
            OptionsTerm.EXPIRATION_DATE: pd.Timestamp("2026-12-01", tz="UTC"),
            OptionsTerm.PRICE: 105.0 + np.arange(20) * 0.1,
        }
    )
    return pd.concat([near, far], ignore_index=True)


def test_futures_series_default_picks_most_populated():
    prices, ts = futures_price_series(_futures_frame())
    assert len(prices) == 200  # the near (most-populated) contract
    assert ts.is_monotonic_increasing


def test_futures_series_by_expiration():
    prices, _ = futures_price_series(_futures_frame(), expiration=pd.Timestamp("2026-12-01", tz="UTC"))
    assert len(prices) == 20


def _roll_futures_frame():
    """Two contracts that roll: near A (exp 02-01) hands off to far B (exp 03-01), with overlap.

    B sits ~10 above A so an unadjusted splice would jump at the roll — the back-adjustment must
    remove that jump while leaving B's latest leg (the true current price) unscaled.
    """
    a_ts = pd.date_range("2026-01-01", "2026-01-31", freq="D", tz="UTC")
    a = pd.DataFrame(
        {
            OptionsTerm.TIMESTAMP: a_ts,
            OptionsTerm.EXPIRATION_DATE: pd.Timestamp("2026-02-01", tz="UTC"),
            OptionsTerm.PRICE: 100.0 + 0.1 * np.arange(len(a_ts)),
        }
    )
    b_ts = pd.date_range("2026-01-15", "2026-02-15", freq="D", tz="UTC")
    b = pd.DataFrame(
        {
            OptionsTerm.TIMESTAMP: b_ts,
            OptionsTerm.EXPIRATION_DATE: pd.Timestamp("2026-03-01", tz="UTC"),
            OptionsTerm.PRICE: 110.0 + 0.1 * np.arange(len(b_ts)),
        }
    )
    return pd.concat([a, b], ignore_index=True)


def test_front_series_rolls_and_back_adjusts():
    prices, ts = front_price_series(_roll_futures_frame())
    assert ts.is_monotonic_increasing
    assert len(prices) == 46  # 2026-01-01 .. 2026-02-15 inclusive
    # latest leg is B unscaled → last value = B's last raw price (110 + 0.1*31)
    assert np.isclose(prices[-1], 110.0 + 0.1 * 31)
    # the ~10-point roll jump is removed: spliced log-returns stay tiny (no discontinuity)
    assert np.max(np.abs(np.diff(np.log(prices)))) < 0.01


def test_underlying_series_dedups_per_timestamp():
    ts = pd.Timestamp("2026-01-01", tz="UTC")
    df = pd.DataFrame(
        {
            OptionsTerm.TIMESTAMP: [ts, ts, ts + pd.Timedelta(days=1)],
            OptionsTerm.UNDERLYING_PRICE: [100.0, 100.0, 101.0],
            OptionsTerm.STRIKE: [90.0, 110.0, 100.0],
        }
    )
    prices, idx = underlying_price_series(df)
    assert list(prices) == [100.0, 101.0]
    assert len(idx) == 2


def test_median_dt_years_for_daily_data():
    ts = pd.date_range("2026-01-01", periods=10, freq="D", tz="UTC")
    assert np.isclose(median_dt_years(ts), 1.0 / 365.0)


def test_facade_forecast_distribution_returns_distribution():
    # V1-lc: the facade is the autonomous forecast_distribution producer — it takes a price_series
    # frame in (built by the assembler), it no longer loads or builds the series.
    data = OptionsData(provider=None, asset_code="BTC")
    data.df_fut = _futures_frame()
    forecast = OptionsForecast(data)
    px = price_series(data.df_fut, source="future")
    result = forecast.forecast_distribution(px, 30.0, model="gbm", engine="montecarlo", n=5000, seed=0)
    assert isinstance(result, ForecastResult)
    assert result.spot > 0.0
    assert result.as_of == px[OptionsTerm.TIMESTAMP].max()
    frame = result.to_frame()
    assert list(frame.columns) == ["quantile", "price", "change"]


def test_facade_forecast_distribution_front_source_and_it5_models():
    data = OptionsData(provider=None, asset_code="BTC")
    data.df_fut = _roll_futures_frame()
    forecast = OptionsForecast(data)
    px = price_series(data.df_fut, source="front")
    ar1 = forecast.forecast_distribution(px, 15.0, model="ar1", engine="analytic")
    assert isinstance(ar1, ForecastResult) and ar1.spot > 0.0
    emp = forecast.forecast_distribution(px, 15.0, model="empirical", engine="bootstrap", n=2000, seed=0)
    assert np.all(emp.scenarios() > 0.0)
