"""alphavar.flow — minimal contract-reading chain interpreter (V1-lc prototype, ADR 0003).

This is the **seed** of the result-chain assembler: it takes an ordered chain of producer ``kind``s,
reads each producer's self-description from the neutral ``core.disc`` surface, and wires
``output-kind → input-kind`` forward — exactly what a user or an AI agent would do by hand off the same
surface. It proves ``flow`` is **non-privileged** (A7): it imports no producer, hardcodes no step, and
holds no domain knowledge — it only reads ``Disc`` data and threads frames.

**Scope is deliberately minimal.** The formal layer (a ``Contract`` dataclass, acyclicity checks, a
``RunRecord`` for provenance, the demand-driven Layer-B planner that *computes a missing prerequisite*)
stays Phase 2. Here: resolve kinds → ``Disc``, match each input slot to an already-produced output,
call ``produce(*inputs, **params)``, move on.

To register the built-in options producers, import their registration module first
(``import alphavar.options.producers``); ``flow`` itself stays domain-agnostic.
"""
from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from alphavar.core.disc import Disc, catalog, describe, kinds

__all__ = ["run", "describe", "catalog", "kinds"]


def _match_input(slot: str | tuple[str, ...], produced: dict[str, Any]) -> Any:
    """Resolve one input slot to an already-produced output (a slot may list alternatives)."""
    options = (slot,) if isinstance(slot, str) else slot
    for kind in options:
        if kind in produced:
            return produced[kind]
    raise ValueError(
        f"input {slot!r} is not available; produce one of {list(options)} earlier in the chain "
        f"or seed it via inputs= (have: {sorted(produced)})"
    )


def run(
    chain: Sequence[str],
    params: dict[str, dict[str, Any]] | None = None,
    inputs: dict[str, Any] | None = None,
) -> Any:
    """Run ``chain`` forward off the Disc surface and return the **last** producer's output.

    - ``chain`` — producer ``kind``s in execution order (e.g. ``["price_series", "forecast_distribution"]``).
    - ``params`` — per-kind free params: ``{kind: {param: value}}`` (e.g. ``horizon`` / ``model`` for
      ``forecast_distribution``, ``data`` for a ``load`` source).
    - ``inputs`` — pre-built outputs seeded into the run, keyed by kind, so a chain can start partway
      (e.g. seed a ``futures_history`` frame instead of running a ``load`` source).

    Each step's input slots are matched to outputs already produced (or seeded); the assembler does no
    fallback / prerequisite computation (P-autonomy — that is the caller's or Phase-2 planner's job).
    Render a non-frame result to its interchange frame via the producer's ``Disc.interchange``.
    """
    params = params or {}
    produced: dict[str, Any] = dict(inputs or {})
    last: Any = None
    for kind in chain:
        disc: Disc = describe(kind)
        args = [_match_input(slot, produced) for slot in disc.inputs]
        out = disc.produce(*args, **params.get(kind, {}))
        produced[kind] = out
        last = out
    return last
