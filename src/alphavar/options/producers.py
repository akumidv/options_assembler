"""Register the options/futures result-chain producers on the neutral Disc surface (V1-lc, ADR 0003).

Importing this module publishes the V1 price-slice producers — ``futures_history`` / ``options_history``
(``load``), ``price_series``, ``forecast_distribution``, ``forecast_smile`` / ``forecast_surface`` —
onto ``core.disc`` so an assembler (a user, an AI agent, or the ``flow`` prototype) can wire them
``output-kind → input-kind`` off one self-describing surface.

Registration is **minimal**: each producer's contract (params, output schema, scalars, interchange) is
read back off the function and its return type by ``core.disc`` — see that module. By the ``f(subject,
*rest)`` convention the input edge is just the **first parameter** (``forecast_distribution(price_series,
…)`` / ``forecast_smile(options_history, …)``), so here we supply only the exceptions: a ``kind`` override
for the private ``load`` wrappers, ``consumes=[]`` to mark them as **sources** (no upstream edge), and the
one ``consumes`` *alternatives* edge (``price_series`` from ``futures_history`` **or** ``options_history``).

The ``load`` kinds wrap ``OptionsData`` (which hides the provider / reference / dropna internals,
R1/R2) and expose only the **output frame-kind** to the chain. New producers join by adding one
``register(...)`` line — no change to ``core.disc`` or ``flow``.
"""
from __future__ import annotations

from pandera.typing import DataFrame

from alphavar.core.disc import register
from alphavar.options.lib.analytic.price import time_value_series_by_atm_distance
from alphavar.options.lib.analytic.risk import payoff_curve, payoff_legs
from alphavar.options.lib.chain import convert_chain_to_desk, select_chain
from alphavar.options.lib.forecast import (
    forecast_distribution,
    forecast_smile,
    forecast_surface,
    price_series,
)
from alphavar.options.option_data_class import OptionsData
from alphavar.options.schemas import FuturesHistory, OptionsHistory

# --- P1: load (data acquisition as graph nodes, P-data) ---------------------------------------------
# Sources (no input kinds): they expose the already-schema-pinned entity frames; the provider/reference
# fetch internals stay hidden inside OptionsData (R1/R2). The typed return carries the output schema.


def _load_futures_history(data: OptionsData) -> DataFrame[FuturesHistory]:
    """Load the futures history frame (source node; hides the provider fetch)."""
    return data.df_fut


def _load_options_history(data: OptionsData) -> DataFrame[OptionsHistory]:
    """Load the options history frame (source node; hides the provider fetch)."""
    return data.df_hist


register(_load_futures_history, kind="futures_history", consumes=[])
register(_load_options_history, kind="options_history", consumes=[])

# --- P2: price_series — the one explicit edge (a series from either market frame) -------------------
register(price_series, consumes=[("futures_history", "options_history")])

# --- P3: scalar forecast (price / vol) and parameter-vector forecasts (smile / surface) -------------
# Input edges inferred from the first parameter name (``price_series`` / ``options_history``); params,
# output schema, scalars and interchange are read off each function and its return type by core.disc.
# Price **and** vol are the one ``forecast_distribution`` kind — both scalar distributions over the
# same interchange schema; ``target`` selects which (the vol facade method is this with target='vol').
register(forecast_distribution)
register(forecast_smile)
register(forecast_surface)

# --- P4: analytic tidy producers (T42a) — chain selection, desk pivot, time-value series ------------
# Single-frame Shape-2 producers whose output schema is read off the ``DataFrame[...]`` return type.
# Input edges are supplied explicitly (the first parameter is named for the frame, not the kind): a
# ``chain`` is one options-history slice; a ``desk`` is a call/put pivot of a ``chain``; a
# ``time_value_series`` is derived from the options history for one selected strike.
register(select_chain, kind="chain", consumes=["options_history"])
register(convert_chain_to_desk, kind="desk", consumes=["chain"])
register(time_value_series_by_atm_distance, kind="time_value_series", consumes=["options_history"])

# --- P5: payoff (T42a) — two producers with a real edge: the combined curve is the sum of the legs.
# ``payoff_legs`` is the base (per-leg P&L over a ``chain``); ``payoff_curve`` consumes it and sums
# each strike across legs (``payoff_curve ← payoff_legs``). ``chain_payoff`` stays as the convenience
# tuple over both.
register(payoff_legs, kind="payoff_legs", consumes=["chain"])
register(payoff_curve, kind="payoff_curve", consumes=["payoff_legs"])
