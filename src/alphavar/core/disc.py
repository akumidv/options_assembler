"""Disc — a producer's self-description, **derived from the function itself** (V1-lc, ADR 0003).

A **producer** is any callable that turns input frame(s) + params into an output *kind* (a tidy frame,
an enriched frame, or a result object rendered to an interchange frame). Its contract is already in the
Python: the signature names the parameters, the return annotation names the output type, and that type
carries its own interchange schema / renderer. So a producer does **not** re-declare any of that — it
registers its callable, and a **``Disc``** is a thin *view* that reads the contract back off the
function on demand:

**Signature convention** (project-wide — see ``DEVELOPMENT_REQUIREMENTS.md``): a callable reads as
``f(subject, *rest)`` — the **first parameter is the data being acted on** (the *subject* — an upstream
interchange frame in the common case, or a source handle / other object for a load node); the **rest**
either say *what to do* (modifier params) or supply *additional data*. So a producer's input is
**positional** — ``inputs[0]`` is the subject — and only the exceptions are declared.

- ``kind`` — the function name (or an explicit override for a private wrapper).
- ``inputs`` — the upstream kinds it consumes. By convention the **first parameter is the subject** →
  the single input edge (kind = its name). ``consumes=`` overrides for the exceptions: an *alternatives*
  slot (a price series from ``futures_history`` **or** ``options_history``), *several* inputs, or ``[]``
  for a *source* (a load node consuming no upstream kind).
- ``params`` — every parameter after the leading input(s): the *what-to-do* modifiers (and any extras).
- ``output_schema`` — the pandera schema of the output, read from the return type: a typed
  ``DataFrame[Schema]`` carries it directly; a result object exposes it as ``interchange_schema``.
- ``scalars`` — scalar provenance names that ride alongside the frame, read from the result type's
  ``interchange_scalars`` (never columns in the frame).
- ``interchange`` — how a non-frame result renders to a tidy frame: the result type's own
  ``to_interchange`` / ``to_frame`` method (a frame output is already its own interchange).

The neutral registry here lets a producer publish **without importing ``flow``** (non-privileged
``flow``, A7): producers ``register``, any assembler (a user, an AI agent, or the ``flow`` prototype)
``describe``s the same surface. Domain-neutral (``core``): no options/futures knowledge — only the
shape of a producer contract — so vol / smile / surface / ``load`` / chain / payoff producers plug in
by registering, with no change here or in ``flow``.
"""
from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any, get_args, get_type_hints

# An input slot: a single producer kind, or a tuple of acceptable alternatives.
InputKind = str | tuple[str, ...]

_REGISTRY: dict[str, Disc] = {}


def _schema_of(annotation: Any) -> Any | None:
    """A pandera schema reachable from a return annotation, else ``None``.

    Two carriers: a typed ``DataFrame[Schema]`` (the schema is the type arg) or a result class that
    exposes ``interchange_schema``. A pandera ``DataFrameModel`` is recognised by its ``to_schema``.
    """
    for cand in get_args(annotation) or (annotation,):
        if isinstance(cand, type) and hasattr(cand, "to_schema"):
            return cand
    return getattr(annotation, "interchange_schema", None)


def _is_frame_output(annotation: Any) -> bool:
    """Whether the return annotation is a (typed) DataFrame — already its own interchange."""
    return any(isinstance(c, type) and hasattr(c, "to_schema") for c in get_args(annotation))


class Disc:
    """A producer's self-description, computed from its function (see the module docstring).

    Holds only what the function can't state itself: the callable, an optional ``kind`` override, and
    the optional ``consumes`` edge declaration. Everything else is a derived, read-only view.
    """

    def __init__(
        self,
        produce: Callable[..., Any],
        kind: str | None = None,
        consumes: tuple[InputKind, ...] | None = None,
    ) -> None:
        self.produce = produce
        self.kind = kind or produce.__name__
        self._consumes = consumes
        self._hints: dict[str, Any] | None = None

    def __repr__(self) -> str:
        return f"Disc(kind={self.kind!r}, inputs={self.inputs!r}, params={self.params!r})"

    # --- signature-derived contract ---------------------------------------------------------------
    @property
    def _param_names(self) -> tuple[str, ...]:
        return tuple(inspect.signature(self.produce).parameters)

    @property
    def inputs(self) -> tuple[InputKind, ...]:
        """The upstream kinds consumed, in positional order.

        Convention: the **first parameter is the subject** — the single input edge, its kind = the
        parameter name. ``consumes`` overrides it: an explicit list (alternatives, or several inputs)
        or ``[]`` for a *source* (a load node with no upstream edge).
        """
        if self._consumes is not None:
            return self._consumes
        names = self._param_names
        return (names[0],) if names else ()

    @property
    def params(self) -> tuple[str, ...]:
        """Free parameter names — the signature after the leading input(s) (the *what-to-do* + extras)."""
        return self._param_names[len(self.inputs):]

    # --- return-type-derived contract -------------------------------------------------------------
    @property
    def _typehints(self) -> dict[str, Any]:
        if self._hints is None:
            try:
                self._hints = get_type_hints(self.produce, include_extras=True)
            except Exception:
                self._hints = {}
        return self._hints

    @property
    def _return(self) -> Any:
        return self._typehints.get("return")

    @property
    def output_schema(self) -> Any | None:
        """The pandera schema of the output, read off the return type (or ``None``)."""
        return _schema_of(self._return)

    @property
    def scalars(self) -> tuple[str, ...]:
        """Scalar provenance names riding alongside the frame (from the result type)."""
        return tuple(getattr(self._return, "interchange_scalars", ()))

    @property
    def interchange(self) -> Callable[[Any], Any] | None:
        """A ``result → tidy frame`` renderer, or ``None`` if the output is already a frame."""
        ret = self._return
        if ret is None or _is_frame_output(ret):
            return None
        for method in ("to_interchange", "to_frame"):
            if hasattr(ret, method):
                return lambda result, _m=method: getattr(result, _m)()
        return None

    @property
    def doc(self) -> str:
        """One-line human description (the producer's docstring summary)."""
        return (self.produce.__doc__ or "").strip().split("\n", 1)[0]


def register(
    fn: Callable[..., Any] | None = None,
    *,
    kind: str | None = None,
    consumes: list[InputKind] | tuple[InputKind, ...] | None = None,
) -> Any:
    """Publish ``fn`` as a producer (usable plain — ``register(fn, ...)`` — or as a decorator).

    ``kind`` overrides the inferred name (``fn.__name__``); ``consumes`` overrides the default
    "first parameter is the input" — an explicit edge list (alternatives / several inputs), or ``[]``
    for a *source* (no upstream edge). Returns ``fn`` so it doubles as a decorator.
    """

    def _apply(func: Callable[..., Any]) -> Callable[..., Any]:
        disc = Disc(func, kind=kind, consumes=tuple(consumes) if consumes is not None else None)
        _REGISTRY[disc.kind] = disc
        return func

    return _apply(fn) if fn is not None else _apply


def describe(kind: str) -> Disc:
    """The ``Disc`` for ``kind`` (raises ``KeyError`` listing known kinds if unregistered)."""
    try:
        return _REGISTRY[kind]
    except KeyError:
        raise KeyError(f"unknown producer kind {kind!r}; registered: {sorted(_REGISTRY)}") from None


def catalog() -> dict[str, Disc]:
    """A copy of the whole producer surface (``kind → Disc``) for listing / introspection."""
    return dict(_REGISTRY)


def kinds() -> tuple[str, ...]:
    """All registered producer kinds, sorted."""
    return tuple(sorted(_REGISTRY))
