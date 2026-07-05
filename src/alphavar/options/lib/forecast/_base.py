"""Forecast model factory — interfaces + result (T27, R5).

A *forecast* is a **distribution** of a target value at a future horizon, produced along three
orthogonal axes:

- **Target** (``ForecastTarget``) — what we forecast (iteration 1: ``PRICE``).
- **Process / model** (``ForecastModel``) — the dynamics + how parameters are estimated from
  history; ``fit`` returns a stateless ``FittedProcess``.
- **Engine** (``ForecastEngine``) — how the fitted process becomes a distribution: closed-form
  (analytic) or simulated (Monte-Carlo).

Horizon is calendar **ACT/365** (``to_horizon_years``), the same convention as
``SmileResult.t_years`` / ``pricer._enrich.years_to_expiry``.

# 4VERIFY (owner, D2): the horizon ACT/365 normalization and that ``ForecastResult`` exposes both
# the level ``S_{t+h}`` and the change ``ΔS = S_{t+h} − S₀``. Per-model math is in price/*.py.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import StrEnum
from typing import ClassVar

import numpy as np
import pandas as pd

from alphavar.core.dictionary import ResultTerm
from alphavar.options.lib.forecast._stats import TerminalDistribution
from alphavar.options.schemas import ForecastDistributionSchema

_DAYS_PER_YEAR = 365.0


class ForecastTarget(StrEnum):
    """What a forecast model predicts. Only ``PRICE`` is implemented (T27 iteration 1)."""

    PRICE = "price"
    VOL = "vol"
    SMILE = "smile"
    SURFACE = "surface"


def to_horizon_years(horizon: pd.Timestamp | pd.Timedelta | float, as_of: pd.Timestamp) -> float:
    """Normalize a horizon spec to ACT/365 calendar years (> 0).

    - ``pd.Timestamp`` — an expiration date; ``(horizon − as_of) / 365d``.
    - ``pd.Timedelta`` — ``horizon / 365d``.
    - ``float`` / ``int`` — a number of **calendar days**.
    """
    if isinstance(horizon, pd.Timestamp):
        years = (horizon - as_of).total_seconds() / (_DAYS_PER_YEAR * 86400.0)
    elif isinstance(horizon, pd.Timedelta):
        years = horizon.total_seconds() / (_DAYS_PER_YEAR * 86400.0)
    else:
        years = float(horizon) / _DAYS_PER_YEAR
    if not np.isfinite(years) or years <= 0.0:
        raise ValueError(f"horizon must be positive calendar time; got {horizon!r} → {years} years")
    return float(years)


@dataclass
class ForecastResult:
    """A distributional forecast of a target at the horizon: point / quantiles / scenarios.

    Carries either a closed-form ``distribution`` (analytic engine) or an empirical ``samples``
    array (Monte-Carlo). ``spot`` (``S₀``) is the last observed value, so the **change** view
    ``ΔS = S_{t+h} − S₀`` is available alongside the level.

    The interchange contract lives **on the type** (read by ``core.disc``): ``to_interchange`` renders
    the ``ForecastDistributionSchema`` frame, and ``interchange_scalars`` names the scalars that ride
    alongside it (never columns in the frame).
    """

    interchange_schema: ClassVar = ForecastDistributionSchema
    interchange_scalars: ClassVar[tuple[str, ...]] = (
        ResultTerm.AS_OF,
        ResultTerm.SPOT,
        ResultTerm.HORIZON_YEARS,
        ResultTerm.TARGET,
        ResultTerm.MODEL,
        ResultTerm.ENGINE,
    )

    target: ForecastTarget
    model: str
    engine: str
    horizon_years: float
    spot: float
    distribution: TerminalDistribution | None = field(default=None, repr=False)
    samples: np.ndarray | None = field(default=None, repr=False)
    as_of: pd.Timestamp | None = None  # the anchor instant (last observed timestamp of the input series)

    def point(self) -> float:
        """Expected terminal level (distribution mean or sample mean)."""
        if self.distribution is not None:
            return float(self.distribution.mean())
        return float(np.mean(self.samples))

    def quantiles(self, q: np.ndarray | float) -> np.ndarray | float:
        """Terminal-level quantile(s) at probability ``q``."""
        if self.distribution is not None:
            return self.distribution.ppf(np.asarray(q, dtype=float))
        return np.quantile(self.samples, q)

    def scenarios(self, n: int = 10000, rng: np.random.Generator | None = None) -> np.ndarray:
        """Terminal-level scenarios: the MC sample as-is, or ``n`` analytic draws."""
        if self.samples is not None:
            return self.samples
        rng = np.random.default_rng() if rng is None else rng
        return self.distribution.sample(n, rng)

    def change(self) -> float:
        """Expected change ``E[S_{t+h}] − S₀``."""
        return self.point() - self.spot

    def change_quantiles(self, q: np.ndarray | float) -> np.ndarray | float:
        """Quantile(s) of the change ``S_{t+h} − S₀``."""
        return self.quantiles(q) - self.spot

    def to_frame(self, quantiles: tuple[float, ...] = (0.05, 0.25, 0.5, 0.75, 0.95)) -> pd.DataFrame:
        """Ergonomic tabular view: one row per quantile, the level **named by target** + change.

        For the target-neutral result-chain interchange (``quantile | value | change``) use
        ``to_interchange`` instead — that is the form passed between capability areas.
        """
        qs = np.asarray(quantiles, dtype=float)
        level = np.atleast_1d(self.quantiles(qs))
        return pd.DataFrame({"quantile": qs, self.target.value: level, "change": level - self.spot})

    def to_interchange(self, quantiles: tuple[float, ...] = (0.05, 0.25, 0.5, 0.75, 0.95)) -> pd.DataFrame:
        """Result-chain interchange frame: ``quantile | value | change`` (neutral ``ResultTerm``).

        Same numbers as ``to_frame`` but the level column is the **target-neutral** ``value`` (so any
        forecast target shares one schema, ``ForecastDistributionSchema``). The scalars
        (``as_of`` / ``spot`` / ``horizon_years`` / ``target`` / ``model`` / ``engine``) ride on this
        object, not in the frame.
        """
        qs = np.asarray(quantiles, dtype=float)
        level = np.atleast_1d(self.quantiles(qs))
        return pd.DataFrame(
            {ResultTerm.QUANTILE: qs, ResultTerm.VALUE: level, ResultTerm.CHANGE: level - self.spot}
        )


class FittedProcess(ABC):
    """A calibrated process for one (target, horizon); turned into a distribution by an engine."""

    target: ForecastTarget
    model_name: str
    spot: float
    horizon_years: float

    @abstractmethod
    def sample_terminal(self, n: int, rng: np.random.Generator) -> np.ndarray:
        """Draw ``n`` terminal target values at the horizon (Monte-Carlo)."""

    def analytic_terminal(self) -> TerminalDistribution | None:
        """Closed-form terminal distribution, or ``None`` if the process has no analytic form."""
        return None

    def bootstrap_terminal(self, n: int, rng: np.random.Generator, block: int | None = None) -> np.ndarray | None:
        """Model-free terminal draws by resampling the process's own history (block bootstrap).

        ``None`` if the process exposes no empirical residual/return series to resample — the
        ``bootstrap`` engine then raises and points the caller at ``montecarlo`` / ``analytic``.
        Distinct from ``sample_terminal`` (parametric noise) for processes that support both.
        """
        return None


class ForecastModel(ABC):
    """A process specification + its estimator. Stateless: ``fit`` returns a ``FittedProcess``."""

    name: str
    target: ForecastTarget
    supports: frozenset[str]  # engine names this model can be run with

    @abstractmethod
    def fit(self, prices: np.ndarray, dt_years: float, horizon_years: float) -> FittedProcess:
        """Calibrate to a chronological ``prices`` series sampled every ``dt_years`` (ACT/365)."""

    @staticmethod
    def _log_returns(prices: np.ndarray) -> tuple[np.ndarray, float]:
        """Log returns of the positive-finite prices + the last price (``S₀``)."""
        p = np.asarray(prices, dtype=float)
        p = p[np.isfinite(p) & (p > 0.0)]
        if p.size < 2:
            raise ValueError("need at least 2 positive finite prices to fit a forecast model")
        return np.diff(np.log(p)), float(p[-1])

    @staticmethod
    def _step_var(returns: np.ndarray) -> float:
        """Per-step return variance (sample variance; ``ddof=0`` for a single return)."""
        return float(np.var(returns, ddof=1)) if returns.size > 1 else float(np.var(returns))


class ForecastEngine(ABC):
    """Inference: turn a ``FittedProcess`` into a ``ForecastResult`` distribution."""

    name: str

    @abstractmethod
    def run(self, fitted: FittedProcess) -> ForecastResult:
        """Produce the forecast distribution for ``fitted``."""
