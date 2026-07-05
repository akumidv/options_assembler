# Domain knowledge base (for AI agents + humans)

Concentrated, **sourced** domain knowledge for `alphavar`: exchanges and their APIs,
options theory, risk, portfolio management. A **source for every non-trivial fact** so it
can be re-queried/verified when a note is insufficient or stale.

This is reference knowledge (the problem domain). It is distinct from
[`_aitna/memory/`](../_aitna/memory/) (how we work on *this* project) and from the formal
rules in [`docs/dev/`](../docs/dev/) (R#/D#).

## The three layers: knowledge → implementation → usage

`knowledge` is one of **three** connected artifacts (the project's AI-assist model — see
[`_aitna/akmon/README.md`](../_aitna/akmon/README.md)):

| Layer | What | Where |
|---|---|---|
| **knowledge** (here) | *what* a concept is and *how it is realized* — theory, decisions, a domain area, or an external resource (an exchange), with sources | `knowledge/` |
| **implementation** | the code that realizes it | `src/alphavar/` |
| **usage skill** | how to *apply* the implementation when a user asks | root [`skills/`](../skills/) |

**knowledge documents the implementation** and **points down** to it: a leaf links the
`src/` that realizes the concept and the [`skills/`](../skills/) skill that applies it
(when one exists). knowledge ≠ skill: knowledge is "what it is / how it's built"; a skill
is "how to use it".

**knowledge is optional.** Add a leaf here only when a concept has **substantial theory,
sources, or design rationale**, or is an external resource (an exchange API). When the
concept is light, **skip it**: describe it briefly in the [`skills/`](../skills/) SKILL.md
and put the implementation detail in the **function docstring** (shorter). Don't create a
thin leaf that only echoes a docstring.

**What we keep:** a concept is documented (here, or just skill + docstring) **only if it is
implemented or planned**. A concept that is neither is **not stored** — not here, not as a
task. (A *planned*, knowledge-worthy concept lives here with an impl task in
[`_aitna/TASKS.md`](../_aitna/TASKS.md), but **no** skill until the code lands.)

> Destined to move out into an MCP knowledge server later (it is cross-project domain
> reference); for now it lives in the project root, doubling as implementation documentation.

## Structure — a multi-level model, not flat files

Knowledge is organized as a **hierarchy of folders → sub-areas → entities**, so it can be
grown in place without reshuffling:

```
knowledge/
  <domain>/                 # exchanges, options, risk, portfolio, …
    README.md               # index of this domain (links up + to children)
    <sub-area>/             # e.g. options/payoffs, options/strategies, risk/var
      README.md             # index of the sub-area
      <entity>.md           # one concrete entity (a payoff, a strategy, a VaR method…)
```

**Hierarchical indexes (required):** every folder has a `README.md` that
- links **up** to its parent index, and
- lists its **immediate children** (subfolders + files), each with a one-line hook.

So the indexes form a navigable tree from this file down to every leaf. When you add an
entity file or subfolder, **update its parent README** in the same change.

## Conventions

- **Concentrated:** short, factual bullets — not prose. Optimize for an assistant skimming
  it into context (token-efficient, see DEVELOPMENT_REQUIREMENTS D4).
- **Every non-obvious fact carries a source** inline: `[short label](url)` to the
  authoritative origin (official API docs, exchange spec, a textbook/paper).
- **Point down to the implementation (and the skill):** a leaf that is realized in code
  links the `src/alphavar/...` that encodes it and, if a usage skill exists, the
  [`skills/`](../skills/) entry that applies it. This is the knowledge→impl→usage chain.
- **Mark confidence/recency:** specifics drift — note "as of <date>" and link the live
  source. If unsure, say so and link the source rather than guess.
- **Re-query path:** when a note is missing detail, follow its source link (or fetch the
  official doc / search) and then *expand the note* with the new sourced fact.
- **One entity per leaf file**; granularity grows by adding files/subfolders, not by
  bloating a file.

## Domains (top-level index)

- [`exchanges/`](exchanges/) — venues & their APIs (Deribit, MOEX, Binance).
- [`options/`](options/) — options theory: concepts, pricing/Greeks/IV, payoffs, strategies.
- [`risk/`](risk/) — payoff profiles, VaR family, risk-adjusted ratios (Sharpe, Sortino).
- [`portfolio/`](portfolio/) — portfolio construction, sizing, risk budgeting.

_Seeded as a skeleton — expand with sourced entity files as the work touches each area._
