# `agents/` — the operating ("desk") agents

**The trading desk.** Agents that **operate** the alphavar ecosystem on the market — fetch
and analyse data, test strategies, and (only the trader) place orders through
[`catcher-bot`](https://github.com/akumidv/catcher-bot).

> This is the **OPERATE** mode — runtime market actors, a different concern from developing
> the project (`_aitna/`) or using the library (root `skills/`). OPERATE is **not yet part
> of the akmon model** — see [`../_aitna/akmon/ROADMAP.md`](../_aitna/akmon/ROADMAP.md)
> (O1). These agents and their **G#** guardrails predate that formulation and will be
> reworked into it.

**Bound by** the runtime guardrails [`GUARDRAILS.md`](GUARDRAILS.md) (**G#**), plus the
architecture rules
[`../docs/dev/ARCHITECTURE_REQUIREMENTS.md`](../docs/dev/ARCHITECTURE_REQUIREMENTS.md)
(**R#**) wherever an agent touches the project's data model. The build rules **D#** do *not*
apply here — desk agents never edit the codebase.

## Roster (one folder per agent)

Each agent is a folder: a `README.md` charter (role, scope, guardrails), a `pipeline.md`
(the ordered playbook), and its own `skills/ tools/ memory/`. Domain how-to comes from the
USAGE layer ([`../skills/`](../skills/) — the concept→function map).

| Agent | Acts | Status |
|---|---|---|
| [`options-analyst/`](options-analyst/) | read-only — mispricing / IV-surface scan | reference (seeded) |
| `investment-analyst/` | read-only — cross-asset allocation views | planned |
| `strategy-tester/` | read-only — backtest strategies, report | planned |
| `fundamental-analyst/` | read-only — company fundamentals → equity/bond forecast | planned |
| `trader/` | **acts** — places orders via catcher-bot (strongest G#) | planned (Phase 4) |
| `orchestrator/` | routes work, separation of duties, consolidated analysis | planned |

## Orchestration

The **orchestrator** is the head-of-desk: it routes a request to the right agent(s),
combines their outputs into one consolidated view, and enforces **separation of duties** —
the agent that *proposes* a trade (analyst / strategy-tester) is never the one that
*executes* it (trader), with the orchestrator (or a human) as the gate (**G9**). Entering
desk mode without naming a role lands here; or address a role directly ("as
options-analyst …").

## Learn loop (desk side)

A desk agent **persists its key actions and insights to its own `memory/`** (what it looked
at, what it concluded, what was missing or wrong). It does **not** change code or add tools
itself. The [engineer agent](../_aitna/agents/engineer/README.md) drains those notes via the
TODO cycle ([`../_aitna/TASKS.md`](../_aitna/TASKS.md)) and, under R#/D#, reworks them into
skills, tools, or library changes. See **G10** in [`GUARDRAILS.md`](GUARDRAILS.md).
