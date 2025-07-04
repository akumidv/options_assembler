"""Neutral chain/result term registry — the vocabulary of *calculation outputs* (A5).

Sibling of ``Term`` (market data: identity / time / price / OHLC). ``ResultTerm`` holds only the
**result-chain** vocabulary — the columns and scalars that describe a calculation's output frame,
shared across every domain (a price forecast, a vol forecast, a risk measure all speak it). Kept
apart from ``Term`` so market-data terms stay clean (R4.3: one canonical name per concept).

Same rules as ``Term``: values are plain strings, used **verbatim** as a column label, a
variable/parameter name, and in the contract self-description. Engine-neutral (no dtypes — those
live in the schema layer). See the result-chain design concept (``_forge/design/result-chain``).
"""

from typing import Final


class ResultTerm:
    """Registry of neutral result/chain terms (one canonical name per concept, A5).

    Reference as ``ResultTerm.QUANTILE`` (resolves to ``"quantile"``). Reuse ``Term.TIMESTAMP`` /
    ``Term.PRICE`` for series frames — those are market-data terms, not result terms.
    """

    # --- Distributional output frame (forecast / risk) ---
    QUANTILE: Final = "quantile"
    """Cumulative probability of a distributional row (in (0, 1))."""

    VALUE: Final = "value"
    """The forecast/result level at the row — target-neutral (a price, a vol, a loss …),
    so any target shares one schema. The ergonomic ``to_frame`` may name it by target."""

    CHANGE: Final = "change"
    """Change of ``value`` vs the reference level ``spot`` (``value − spot``)."""

    # --- Risk-profile output frame (payoff / risk graph) ---
    RISK_PNL: Final = "risk_pnl"
    """Per-strike P&L at expiration (intrinsic value net of premium paid)."""

    RISK_PNL_PREMIUM: Final = "risk_pnl_premium"
    """Per-strike mark-to-market ("today") P&L — the current-price line of a risk graph."""

    LEG_ID: Final = "leg_id"
    """Identifier of a single strategy leg in the per-leg payoff breakdown."""

    # --- Result scalars (ride on the result object / contract scalar-spec, not in the frame) ---
    HORIZON_YEARS: Final = "horizon_years"
    """Forecast horizon in ACT/365 calendar years."""

    AS_OF: Final = "as_of"
    """The instant the result is anchored at (last observation of the input series)."""

    SPOT: Final = "spot"
    """The reference level ``S₀`` (last observed value) the change view is measured from."""

    CONFIDENCE: Final = "confidence"
    """Confidence level of a risk measure (VaR/CVaR) — reserved for the risk layer (T35)."""

    TARGET: Final = "target"
    """What was forecast (price / vol / smile / surface)."""

    MODEL: Final = "model"
    """The process/model name used to produce the result."""

    ENGINE: Final = "engine"
    """The inference engine used (analytic / montecarlo / bootstrap)."""
