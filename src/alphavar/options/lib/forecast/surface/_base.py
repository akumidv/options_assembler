"""Surface decode + result types (T27 iteration 4, R5).

A forecast on a stacked constant-maturity θ vector decodes back to a **surface**: one SVI smile per
tenor node, read at any ``(k, τ)`` by total-variance interpolation across the nodes (``_interpolate``,
with flat ``w/τ`` T-extrapolation). ``SurfaceResult`` is the deterministic decoded surface;
``SurfaceForecast`` is the distributional result (expected surface + scenario σ(k,τ) bands), the
surface sibling of ``SmileForecast``.

No-arbitrage is checked two ways: **butterfly** per node smile (Gatheral g(k)) and **calendar**
(ATM total variance non-decreasing across the tenor nodes).

# 4VERIFY (owner, D2): the stacked-θ → per-node decode (node-major reshape), the σ(k,τ) tenor
# interpolation, and the calendar (∂w/∂τ ≥ 0 at k=0) + butterfly no-arb checks.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import ClassVar

import numpy as np
import pandas as pd

from alphavar.options.lib.forecast.smile._base import SMILE_PARAM_NAMES
from alphavar.options.lib.forecast.smile._decode import decode_smile
from alphavar.options.lib.forecast.surface._interpolate import interp_total_variance
from alphavar.options.lib.pricer.smile import SmileResult
from alphavar.options.schemas import SurfaceForecastSchema

_N_PARAMS = len(SMILE_PARAM_NAMES)
_DEFAULT_K_GRID = np.linspace(-1.0, 1.0, 21)


def decode_surface(stacked_theta: np.ndarray, tenor_nodes: np.ndarray) -> SurfaceResult:
    """Split a stacked θ vector (node-major) into one decoded smile per tenor node."""
    theta = np.asarray(stacked_theta, dtype=float)
    tenors = np.asarray(tenor_nodes, dtype=float)
    if theta.size != tenors.size * _N_PARAMS:
        raise ValueError(f"stacked θ size {theta.size} != n_nodes({tenors.size})·{_N_PARAMS}")
    blocks = theta.reshape(tenors.size, _N_PARAMS)
    smiles = {
        float(t): decode_smile(block, SMILE_PARAM_NAMES, float(t)) for t, block in zip(tenors, blocks, strict=True)
    }
    return SurfaceResult(tenors, smiles)


@dataclass
class SurfaceResult:
    """A decoded vol surface: one smile per tenor node, evaluable at any ``(k, τ)``."""

    tenor_nodes: np.ndarray
    smiles_by_tenor: dict[float, SmileResult] = field(repr=False)

    def iv(self, k: np.ndarray | float, tau: float) -> np.ndarray:
        """σ(k, τ) by total-variance interpolation across the tenor nodes (+ T-extrapolation)."""
        k = np.atleast_1d(np.asarray(k, dtype=float))
        tenors = np.array(sorted(self.smiles_by_tenor), dtype=float)
        w = np.vstack([self.smiles_by_tenor[t].total_variance(k) for t in tenors])
        return np.sqrt(np.maximum(interp_total_variance(tenors, w, float(tau)), 0.0) / max(float(tau), 1e-12))

    def is_butterfly_free(self, k_grid: np.ndarray | None = None) -> bool:
        """Every node smile free of static butterfly arbitrage."""
        return all(s.is_butterfly_free(k_grid) for s in self.smiles_by_tenor.values())

    def is_calendar_free(self, tol: float = -1e-6) -> bool:
        """ATM total variance non-decreasing across tenor nodes (no calendar arbitrage)."""
        tenors = np.array(sorted(self.smiles_by_tenor), dtype=float)
        w_atm = np.array([float(self.smiles_by_tenor[t].total_variance(0.0)) for t in tenors])
        return bool(np.all(np.diff(w_atm) >= tol))


@dataclass
class SurfaceForecast:
    """A distributional forecast of a vol surface: expected surface + scenario σ(k,τ) bands.

    ``to_frame`` renders the ``SurfaceForecastSchema`` interchange (read off this type by ``core.disc``).
    """

    interchange_schema: ClassVar = SurfaceForecastSchema

    model: str
    engine: str
    tenor_nodes: np.ndarray
    horizon_years: float
    mean_theta: np.ndarray
    samples: np.ndarray | None = field(default=None, repr=False)

    def expected_surface(self) -> SurfaceResult:
        """The forecast surface from the expected terminal stacked θ."""
        return decode_surface(self.mean_theta, self.tenor_nodes)

    def iv(self, k: np.ndarray | float, tau: float) -> np.ndarray:
        """Expected σ(k, τ)."""
        return self.expected_surface().iv(k, tau)

    def is_butterfly_free(self, k_grid: np.ndarray | None = None) -> bool:
        """Expected surface free of static butterfly arbitrage (per node)."""
        return self.expected_surface().is_butterfly_free(k_grid)

    def is_calendar_free(self) -> bool:
        """Expected surface free of calendar arbitrage (ATM w non-decreasing in τ)."""
        return self.expected_surface().is_calendar_free()

    def scenario_surfaces(self, max_n: int | None = None) -> list[SurfaceResult]:
        """Decode the θ scenarios into surfaces (empty for the analytic engine)."""
        if self.samples is None:
            return []
        samples = self.samples if max_n is None else self.samples[: int(max_n)]
        return [decode_surface(theta, self.tenor_nodes) for theta in samples]

    def iv_quantiles(self, k: np.ndarray | float, tau: float, quantiles: tuple[float, ...]) -> np.ndarray:
        """σ(k, τ) quantile bands across scenarios, shape ``(len(quantiles), len(k))``."""
        k = np.atleast_1d(np.asarray(k, dtype=float))
        qs = np.asarray(quantiles, dtype=float)
        if self.samples is None:
            return np.tile(self.iv(k, tau), (qs.size, 1))
        curves = np.array([s.iv(k, tau) for s in self.scenario_surfaces()])
        return np.quantile(curves, qs, axis=0)

    def to_frame(
        self,
        tenors: np.ndarray | None = None,
        k_grid: np.ndarray | None = None,
        quantiles: tuple[float, ...] = (0.05, 0.5, 0.95),
    ) -> pd.DataFrame:
        """Long surface table: one row per ``(tenor, k)`` — expected σ(k,τ) + a column per quantile."""
        tenors = self.tenor_nodes if tenors is None else np.asarray(tenors, dtype=float)
        k = _DEFAULT_K_GRID if k_grid is None else np.asarray(k_grid, dtype=float)
        frames = []
        for tau in tenors:
            out = {"tenor": float(tau), "k": k, "iv": self.iv(k, float(tau))}
            for q, row in zip(quantiles, self.iv_quantiles(k, float(tau), quantiles), strict=True):
                out[f"iv_q{q:g}"] = row
            frames.append(pd.DataFrame(out))
        return pd.concat(frames, ignore_index=True)
