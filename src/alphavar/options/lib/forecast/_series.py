"""Price-series extraction for forecasting — DataFrame in → (prices, timestamps) out (R5, T27).

Selects the single underlying series a price model fits, from either the futures frame (a chosen
expiration, default the most-populated = de-facto front) or the per-timestamp ``underlying_price``
on the options history. No I/O; the ``OptionsForecast`` facade wires ``df_fut`` / ``df_hist`` in.

# 4VERIFY (owner, D2): the default-expiration choice (most-populated series), the per-timestamp
# underlying dedup (one value per ts), and the ACT/365 median-spacing step inference.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from pandera.typing import DataFrame

from alphavar.core.dictionary import Term
from alphavar.options.dictionary import OptionsTerm
from alphavar.options.schemas import PriceSeriesSchema

_DAYS_PER_YEAR = 365.0


def futures_price_series(
    df_fut: pd.DataFrame, expiration: pd.Timestamp | None = None
) -> tuple[np.ndarray, pd.DatetimeIndex]:
    """A single futures price series from ``df_fut``: a chosen ``expiration`` or the most-populated."""
    if df_fut is None or len(df_fut) == 0:
        raise ValueError("no futures data to build a price series; try source='underlying'")
    df = df_fut
    if expiration is not None:
        df = df[df[OptionsTerm.EXPIRATION_DATE] == pd.Timestamp(expiration)]
        if len(df) == 0:
            raise ValueError(f"no futures rows for expiration {expiration!r}")
    elif OptionsTerm.EXPIRATION_DATE in df.columns and df[OptionsTerm.EXPIRATION_DATE].nunique() > 1:
        # default = the continuously-quoted (most-populated) contract; a true rolled front
        # series is the planned source='front'.
        chosen = df[OptionsTerm.EXPIRATION_DATE].value_counts().idxmax()
        df = df[df[OptionsTerm.EXPIRATION_DATE] == chosen]
    df = df.sort_values(OptionsTerm.TIMESTAMP)
    return df[OptionsTerm.PRICE].to_numpy(dtype=float), pd.DatetimeIndex(df[OptionsTerm.TIMESTAMP])


def front_price_series(
    df_fut: pd.DataFrame, roll_buffer_days: float = 2.0
) -> tuple[np.ndarray, pd.DatetimeIndex]:
    """Rolled continuous **front-contract** series from ``df_fut``, proportionally back-adjusted.

    At each timestamp the front = the nearest expiration still at least ``roll_buffer_days`` ahead
    (so we roll off a contract just before expiry, avoiding settlement microstructure). Splicing the
    chosen prices leaves jumps at each roll; we remove them by **proportional (ratio) back-adjustment**
    — at a roll ``t_r`` from contract A to B the factor ``ρ_r = price_B(t_r)/price_A(t_r)`` (from the
    overlap in ``df_fut``; ``1.0`` if B is not yet quoted at ``t_r``) scales every *earlier* segment by
    the product of subsequent ratios. The latest segment is left unadjusted, so the last value is the
    true current front price and the spliced **log-returns** are continuous (what the models consume).

    # 4VERIFY (owner, D2): the front selection (nearest expiry ≥ now + roll_buffer), the roll-point
    # detection, and the proportional back-adjustment ρ_r = P_B(t_r)/P_A(t_r) anchored at the latest leg.
    """
    if df_fut is None or len(df_fut) == 0:
        raise ValueError("no futures data to build a front series; try source='underlying'")
    exp_col, ts_col, px_col = OptionsTerm.EXPIRATION_DATE, OptionsTerm.TIMESTAMP, OptionsTerm.PRICE
    df = df_fut[[ts_col, exp_col, px_col]].dropna().sort_values([ts_col, exp_col])
    buffer = pd.Timedelta(days=roll_buffer_days)
    # per timestamp: the front contract (nearest expiry still beyond the roll buffer)
    rows = []
    for ts, grp in df.groupby(ts_col, sort=True):
        live = grp[grp[exp_col] >= ts + buffer]
        pick = (live if len(live) else grp).iloc[0]  # nearest non-stale expiry; fall back to nearest
        rows.append((ts, pick[exp_col], float(pick[px_col])))
    if not rows:
        raise ValueError("no futures rows survive the front-series roll buffer")
    ts_idx = pd.DatetimeIndex([r[0] for r in rows])
    chosen_exp = np.array([r[1] for r in rows])
    raw_px = np.array([r[2] for r in rows], dtype=float)

    # proportional back-adjustment: anchor the latest leg, scale earlier legs by the roll ratios
    factor = np.ones(raw_px.size, dtype=float)
    roll_at = np.flatnonzero(chosen_exp[1:] != chosen_exp[:-1]) + 1  # first index of each new contract
    lookup = df.set_index([ts_col, exp_col])[px_col]
    for r in roll_at:
        t_r, exp_old, exp_new = ts_idx[r], chosen_exp[r - 1], chosen_exp[r]
        try:  # both contracts' price at the roll timestamp ⇒ the continuity ratio
            ratio = float(lookup.loc[(t_r, exp_new)]) / float(lookup.loc[(t_r, exp_old)])
        except KeyError:
            ratio = 1.0  # no overlap quote → accept the (small) gap rather than guess
        if np.isfinite(ratio) and ratio > 0.0:
            factor[:r] *= ratio  # all earlier timestamps live before this roll
    return raw_px * factor, ts_idx


def underlying_price_series(df_hist: pd.DataFrame) -> tuple[np.ndarray, pd.DatetimeIndex]:
    """Per-timestamp underlying price from the options history (one value per timestamp)."""
    if df_hist is None or len(df_hist) == 0:
        raise ValueError("no options history to build an underlying price series")
    grouped = df_hist.groupby(OptionsTerm.TIMESTAMP, sort=True)[OptionsTerm.UNDERLYING_PRICE].first()
    return grouped.to_numpy(dtype=float), pd.DatetimeIndex(grouped.index)


def median_dt_years(timestamps: pd.DatetimeIndex) -> float:
    """ACT/365 step between observations = median timestamp spacing, in years."""
    ts = pd.DatetimeIndex(timestamps).sort_values()
    if len(ts) < 2:
        raise ValueError("need at least 2 timestamps to infer the data step")
    median_ns = float(np.median(np.diff(ts.asi8)))
    return median_ns / (1e9 * 86400.0 * _DAYS_PER_YEAR)


def price_series(
    df: pd.DataFrame, source: str = "future", expiration: pd.Timestamp | None = None
) -> DataFrame[PriceSeriesSchema]:
    """**Autonomous ``price_series`` producer** (V1-lc): a market frame → tidy ``timestamp | price``.

    Takes the frame **handed in** (no loading, no source fallback — the assembler chooses which frame
    to pass): ``future`` / ``front`` read a **futures** frame (``df_fut``), ``underlying`` reads an
    **options-history** frame (``df_hist``). ``source`` selects the producer's own construction over
    that frame; it does not reach back to whoever loaded it. Wraps the numpy series kernels above
    into the chain's interchange frame (``Term.TIMESTAMP`` / ``Term.PRICE``).
    """
    if source == "future":
        prices, timestamps = futures_price_series(df, expiration)
    elif source == "front":
        prices, timestamps = front_price_series(df)
    elif source == "underlying":
        prices, timestamps = underlying_price_series(df)
    else:
        raise ValueError(f"unknown source {source!r}; use 'future', 'front' or 'underlying'")
    return pd.DataFrame({Term.TIMESTAMP: timestamps, Term.PRICE: prices})


def series_arrays(price_series_df: pd.DataFrame) -> tuple[np.ndarray, pd.DatetimeIndex]:
    """Extract the ``(prices, timestamps)`` kernel arrays from a ``price_series`` frame (chronological)."""
    df = price_series_df.sort_values(Term.TIMESTAMP)
    return df[Term.PRICE].to_numpy(dtype=float), pd.DatetimeIndex(df[Term.TIMESTAMP])
