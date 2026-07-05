"""Smile-forecast interfaces + result (T27 iteration 3, R5).

A *smile forecast* predicts the SVI parameter vector ``θ = (a, b, ρ, m, σ)`` of one option
expiration at a future horizon and decodes it back to a smile at the target tenor. Its state is a
**parameter vector**, not a scalar, so it has its own fitted process (``ThetaProcess``), models
(``SmileForecastModel``) and result (``SmileForecast``) — a sibling of the scalar
``forecast._base`` framework, mirroring how ``pricer.smile`` is a sibling factory of the scalar
pricer (ADR 0002).

Every smile model reduces to a **Gaussian terminal** on θ (a mean vector + a covariance), so the
fitted process is a single concrete dataclass; the three models (``param_rw`` / ``param_var`` /
``param_pca``) differ only in how they estimate that mean and covariance from the θ history.

# 4VERIFY (owner, D2): θ as the forecast state (the ``fixed_expiration`` maturity mixes tenors
# across history — see ``_theta``), the Gaussian-terminal reduction (mean θ + cov θ), and the
# decode-at-target-tenor convention (``_decode.decode_smile``).
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import ClassVar

import numpy as np
import pandas as pd

from alphavar.options.lib.forecast.smile._decode import decode_smile, sample_mvn
from alphavar.options.lib.pricer.smile import SmileResult
from alphavar.options.schemas import SmileForecastSchema

# Raw-SVI parameters, in the fixed order used by every θ vector / covariance in this package.
SMILE_PARAM_NAMES: tuple[str, ...] = ("a", "b", "rho", "m", "sigma")

_DEFAULT_K_GRID = np.linspace(-1.0, 1.0, 21)


@dataclass
class ThetaProcess:
    """A calibrated Gaussian terminal on the SVI parameter vector θ at the horizon.

    ``mean`` / ``cov`` are the terminal θ distribution ``N(mean, cov)``; ``theta0`` is the last
    observed θ (the change-view reference). ``t_target`` is the tenor (ACT/365) at which the
    forecast smile is presented (``fixed_expiration``: ``E − (as_of + horizon)``).
    """

    model_name: str
    param_names: tuple[str, ...]
    t_target: float
    horizon_years: float
    theta0: np.ndarray
    mean: np.ndarray
    cov: np.ndarray = field(repr=False)

    def mean_terminal_theta(self) -> np.ndarray:
        """Expected terminal θ vector."""
        return np.asarray(self.mean, dtype=float)

    def sample_terminal_theta(self, n: int, rng: np.random.Generator) -> np.ndarray:
        """Draw ``n`` terminal θ vectors from ``N(mean, cov)`` (shape ``(n, p)``)."""
        return sample_mvn(self.mean, self.cov, int(n), rng)


class SmileForecastModel(ABC):
    """A θ-dynamics specification + estimator. Stateless: ``fit`` returns a ``ThetaProcess``."""

    name: str
    supports: frozenset[str]  # engine names this model can run with

    @abstractmethod
    def fit(self, theta_history: np.ndarray, dt_years: float, horizon_years: float, t_target: float) -> ThetaProcess:
        """Calibrate to a chronological θ history (rows = times, cols = ``SMILE_PARAM_NAMES``)."""

    @staticmethod
    def _prepare(theta_history: np.ndarray) -> tuple[np.ndarray, int]:
        """Validate the θ history and return ``(theta, p)`` (need ≥ 2 rows for any dynamics)."""
        theta = np.atleast_2d(np.asarray(theta_history, dtype=float))
        if theta.ndim != 2 or theta.shape[0] < 2:
            raise ValueError("need at least 2 θ observations (timestamps) to fit a smile forecast")
        return theta, theta.shape[1]

    @staticmethod
    def _n_steps(horizon_years: float, dt_years: float) -> int:
        """Whole forecast steps at the data cadence (``round(H / dt)``, ≥ 1)."""
        return max(1, int(round(float(horizon_years) / float(dt_years))))


@dataclass
class SmileForecast:
    """A distributional forecast of a smile: an expected curve + scenario σ(k) quantile bands.

    Carries the expected terminal θ (``mean_theta``) and, for the Monte-Carlo engine, ``samples``
    of θ (shape ``(n, p)``). Each θ decodes to a ``SmileResult`` at ``t_target`` via the raw-SVI
    form; the no-arbitrage butterfly check runs on the expected smile. ``to_frame`` renders the
    ``SmileForecastSchema`` interchange (read off this type by ``core.disc``).
    """

    interchange_schema: ClassVar = SmileForecastSchema

    model: str
    engine: str
    param_names: tuple[str, ...]
    t_target: float
    horizon_years: float
    theta0: np.ndarray
    mean_theta: np.ndarray
    samples: np.ndarray | None = field(default=None, repr=False)

    def expected_smile(self) -> SmileResult:
        """The forecast smile from the expected terminal θ (analytic centre)."""
        return decode_smile(self.mean_theta, self.param_names, self.t_target)

    def current_smile(self) -> SmileResult:
        """The last observed θ decoded at ``t_target`` — the change-view reference smile."""
        return decode_smile(self.theta0, self.param_names, self.t_target)

    def iv(self, k: np.ndarray | float) -> np.ndarray:
        """Expected σ(k) over log-moneyness ``k`` (the expected-smile curve)."""
        return self.expected_smile().iv(k)

    def is_butterfly_free(self, k_grid: np.ndarray | None = None) -> bool:
        """Whether the expected smile is free of static butterfly arbitrage (Gatheral g(k))."""
        return self.expected_smile().is_butterfly_free(k_grid)

    def scenario_smiles(self, max_n: int | None = None) -> list[SmileResult]:
        """Decode the θ scenarios into smiles (empty for the analytic engine)."""
        if self.samples is None:
            return []
        samples = self.samples if max_n is None else self.samples[: int(max_n)]
        return [decode_smile(theta, self.param_names, self.t_target) for theta in samples]

    def iv_quantiles(self, k: np.ndarray | float, quantiles: tuple[float, ...]) -> np.ndarray:
        """σ(k) quantile bands across scenarios, shape ``(len(quantiles), len(k))``.

        For the analytic engine (no scenarios) every band is the expected curve (degenerate).
        """
        k = np.atleast_1d(np.asarray(k, dtype=float))
        qs = np.asarray(quantiles, dtype=float)
        if self.samples is None:
            return np.tile(self.iv(k), (qs.size, 1))
        curves = np.array([s.iv(k) for s in self.scenario_smiles()])  # (n, K)
        return np.quantile(curves, qs, axis=0)

    def to_frame(
        self, k_grid: np.ndarray | None = None, quantiles: tuple[float, ...] = (0.05, 0.5, 0.95)
    ) -> pd.DataFrame:
        """Tabular smile: one row per log-moneyness ``k`` — expected σ(k) + a column per quantile."""
        k = _DEFAULT_K_GRID if k_grid is None else np.asarray(k_grid, dtype=float)
        out = {"k": k, "iv": self.iv(k)}
        bands = self.iv_quantiles(k, quantiles)
        for q, row in zip(quantiles, bands, strict=True):
            out[f"iv_q{q:g}"] = row
        return pd.DataFrame(out)
