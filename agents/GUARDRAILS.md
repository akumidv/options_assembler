# Desk guardrails — `G#`

Hard, binding constraints for **desk (operate) agents** — the ones that act on the market,
data and money. They are the runtime counterpart to the codebase rules **R#**
(architecture) and **D#** (development); together **R# / D# / G#** are the project's three
requirement axes, indexed by the root [`AGENTS.md`](../AGENTS.md).

G# apply **only to desk agents**; the build agent's guardrails are **D#** (it never acts on
the market). A desk agent's charter must reference the G# it relies on; agent-specific
limits are listed in that agent's own `README.md`.

## Global — every desk agent

- **G1 Provenance.** Every non-trivial number reported carries its source (data timestamp,
  endpoint, or computation). No unsourced figures. (Runtime sibling of D2.)
- **G2 No silent failure.** A data gap, stale quote, or schema mismatch **halts** the agent
  with a clear error — never guess or interpolate around it.
- **G3 Auditability.** Every decision is logged with its inputs and sources, and is
  reproducible from them.
- **G4 Secrets & least privilege.** API keys never live in the repo; trading keys are scoped
  to the minimum needed (e.g. no withdrawal permission).

## Class — analysis vs execution

- **G5 Read-only by default.** Analysis agents may fetch / compute / report only. They have
  no path to place an order.
- **G6 Act only through the gateway.** The only agent that may place orders is the
  **trader**, and only through `catcher-bot`'s gated API — never raw exchange calls.

## Agent — the trader (strongest)

- **G7 Paper by default.** Live trading is an explicit, flagged opt-in; the default is
  sim / paper.
- **G8 Limits are config the agent cannot override.** Max position size, max daily loss,
  allowed instruments and leverage caps come from configuration; the agent may not raise
  them.
- **G9 Separation of duties.** Whoever *proposes* a trade (analyst / strategy-tester) is not
  who *executes* it (trader); the orchestrator or a human is the gate.
- **G10 Human-in-the-loop + kill switch.** A live order requires explicit confirmation, and
  the trader is haltable by a single command.

## Learn-loop note

Desk agents record key actions/insights to their own `memory/`; they do **not** self-edit
code, tools, or these guardrails. Stabilising a new constraint means graduating a memory
note into a numbered G# here — done by the **build agent** under R#/D# via the TODO cycle
([`../_aitna/TASKS.md`](../_aitna/TASKS.md)).
