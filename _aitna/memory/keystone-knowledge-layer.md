# knowledge → implementation → usage (knowledge is optional)

A domain concept is modeled as a **three-artifact chain** (akmon README §3b):
**knowledge → implementation → usage skill**.

- **knowledge** — [`../../knowledge/`](../../knowledge/) at the repo root (lifted from the
  old `agents/shared/knowledge/`): "what it is / how it's realized", **sourced**. Documents
  the implementation and **points DOWN** to `src/` + the `skills/` skill. Destined for an
  MCP server later.
- **implementation** — `src/alphavar/`.
- **usage skill** — root [`../../skills/`](../../skills/) (USAGE): "how to apply it" = the
  concept→function map.

**knowledge is OPTIONAL (decided 2026-06-20):** add a `knowledge/` leaf only for a concept
with substantial theory/sources/rationale or an external resource (an exchange). Otherwise
**skip it** — short description in the SKILL.md + details in the **function docstring** (which
is shorter). Don't make a thin leaf that only echoes a docstring.

**Storage rule:** document a concept (knowledge, or just skill+docstring) **only if
implemented or planned**. Neither → store nothing (not even a task). Planned-but-uncoded,
knowledge-worthy → a `knowledge/` leaf marked "planned" + an impl task in
[`../TASKS.md`](../TASKS.md), but **no skill until the code lands** (currently Greeks/Sortino
= T36; VaR/CVaR = T31–T33).

`knowledge ≠ skill`: knowledge is what/how-built; a skill is how-to-use. One source of truth
per fact. Part of [[akmon-ai-assist-model]].
