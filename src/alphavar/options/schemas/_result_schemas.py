"""Result-chain interchange schemas (A4a / D-b) — the pinned tidy form of a producer's output.

Distinct from the market-data entity schemas (``_schemas.py``: ``OptionsHistory`` …): these pin the
**interchange** frames passed between capability areas (the result-chain). V1 covers the price
slice — ``price_series`` and ``forecast_distribution``; the full Shape-2 catalog is design point D-b.

Columns bind to the registry by reference (``alias=...``): market-data terms via ``Term`` (series
``timestamp``/``price``), result terms via ``ResultTerm`` (``quantile``/``value``/``change``). Same
validation policy as the entity schemas (``strict=False``, ``coerce=True``, ``lazy=True`` at call).
"""

import pandas as pd
import pandera.pandas as pa

from alphavar.core.dictionary import ResultTerm, Term
from alphavar.options.dictionary import OptionsTerm


class _Base(pa.DataFrameModel):
    class Config:
        strict = False  # extra columns allowed
        coerce = True  # coerce dtypes (interchange hygiene)


class PriceSeriesSchema(_Base):
    """A ``price_series`` interchange frame: one positive price per timestamp, chronological."""

    timestamp: pd.Timestamp = pa.Field(alias=Term.TIMESTAMP, nullable=False)
    price: float = pa.Field(alias=Term.PRICE, gt=0, nullable=False)


class ForecastDistributionSchema(_Base):
    """A ``forecast_distribution`` interchange frame: one row per quantile of the terminal value.

    ``change = value − spot`` (the spot scalar rides on the result object / contract scalar-spec).
    """

    quantile: float = pa.Field(alias=ResultTerm.QUANTILE, gt=0, lt=1, nullable=False)
    value: float = pa.Field(alias=ResultTerm.VALUE, nullable=False)
    change: float = pa.Field(alias=ResultTerm.CHANGE, nullable=False)


class SmileForecastSchema(_Base):
    """A ``forecast_smile`` interchange frame: expected σ(k) per log-moneyness (+ quantile bands).

    Pins the fixed columns; the per-quantile ``iv_q*`` bands are extra (``strict=False``).
    """

    k: float = pa.Field(nullable=False)
    iv: float = pa.Field(ge=0, nullable=False)


class SurfaceForecastSchema(_Base):
    """A ``forecast_surface`` interchange frame: expected σ(k,τ) per ``(tenor, k)`` (+ quantile bands)."""

    tenor: float = pa.Field(gt=0, nullable=False)
    k: float = pa.Field(nullable=False)
    iv: float = pa.Field(ge=0, nullable=False)


class ChainSchema(_Base):
    """A ``chain`` interchange frame: one options-history slice — a single settlement ``timestamp`` and
    a single ``expiration_date`` (call and put rows). Same columns as ``OptionsHistory`` (extra columns
    allowed, ``strict=False``); this pins the key columns a chain consumer relies on. The one-timestamp /
    one-expiration invariant is enforced at the seam by ``chain.validate_chain``."""

    timestamp: pd.Timestamp = pa.Field(alias=OptionsTerm.TIMESTAMP, nullable=False)
    expiration_date: pd.Timestamp = pa.Field(alias=OptionsTerm.EXPIRATION_DATE, nullable=False)
    strike: float = pa.Field(alias=OptionsTerm.STRIKE, gt=0, nullable=False)
    option_right: str = pa.Field(alias=OptionsTerm.OPTION_RIGHT, nullable=False)


class DeskSchema(_Base):
    """A ``desk`` interchange frame: a call/put pivot of a chain by strike (value columns carry
    ``_call`` / ``_put`` suffixes). Pins the ``(timestamp, expiration_date, strike)`` key; the
    parametric value columns are extra (``strict=False``)."""

    timestamp: pd.Timestamp = pa.Field(alias=OptionsTerm.TIMESTAMP, nullable=False)
    expiration_date: pd.Timestamp = pa.Field(alias=OptionsTerm.EXPIRATION_DATE, nullable=False)
    strike: float = pa.Field(alias=OptionsTerm.STRIKE, gt=0, nullable=False)


class TimeValueSeriesSchema(_Base):
    """A ``time_value_series`` interchange frame: an option's time value over time for one selected
    strike (chronological). One row per settlement ``timestamp``."""

    timestamp: pd.Timestamp = pa.Field(alias=OptionsTerm.TIMESTAMP, nullable=False)
    strike: float = pa.Field(alias=OptionsTerm.STRIKE, gt=0, nullable=False)
    timed_value: float = pa.Field(alias=OptionsTerm.TIMED_VALUE, nullable=True)


class PayoffCurveSchema(_Base):
    """A ``payoff_curve`` interchange frame: the combined strategy P&L per strike — one row per
    ``strike`` with the expiration line (``risk_pnl``) and the mark-to-market line (``risk_pnl_premium``)."""

    strike: float = pa.Field(alias=OptionsTerm.STRIKE, gt=0, nullable=False)
    risk_pnl: float = pa.Field(alias=ResultTerm.RISK_PNL, nullable=False)
    risk_pnl_premium: float = pa.Field(alias=ResultTerm.RISK_PNL_PREMIUM, nullable=True)


class PayoffLegsSchema(_Base):
    """A ``payoff_legs`` interchange frame: the per-leg payoff breakdown — one row per
    ``(leg_id, strike)``. Summing ``risk_pnl`` / ``risk_pnl_premium`` over ``leg_id`` yields the
    ``payoff_curve``."""

    strike: float = pa.Field(alias=OptionsTerm.STRIKE, gt=0, nullable=False)
    risk_pnl: float = pa.Field(alias=ResultTerm.RISK_PNL, nullable=False)
    risk_pnl_premium: float = pa.Field(alias=ResultTerm.RISK_PNL_PREMIUM, nullable=True)
    leg_id: str = pa.Field(alias=ResultTerm.LEG_ID, nullable=False)
