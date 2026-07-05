# agents/ = OPERATE (desk); OPERATE deferred from akmon

[`../../agents/`](../../agents/) is now **only the OPERATE (desk) layer** — runtime market
actors (analysis, backtest, and trading agents bound by **G#** guardrails). Its contents
were **lifted up one level** (2026-06-20): `agents/desk/*` → `agents/*` (so `agents/README.md`,
`agents/GUARDRAILS.md`, `agents/options-analyst/`). The old two-class `agents/` operating
system and the `agents/shared/` substrate are gone.

**OPERATE is a third mode, deliberately NOT in the akmon model yet** — its risk is
real-money/irreversible, needing runtime guardrails, an orchestrator, and propose≠execute
separation, not D#/USAGE rules. Tracked as an open question in
[`../akmon/ROADMAP.md`](../akmon/ROADMAP.md) (**O1**); when formulated it becomes a
third branch of the layer decision tree (DEVELOP / USE / **OPERATE**), not a layer under
DEVELOP.

Do **not** conflate USAGE ("using the library") with OPERATE ("operating in a market").
Part of [[akmon-ai-assist-model]].
