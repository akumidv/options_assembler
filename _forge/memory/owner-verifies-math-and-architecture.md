# Owner verifies math, DataFrame logic, and architecture (MANDATORY)

Any new/changed **DataFrame operation** (filters, groupby/agg, joins, masks, column
derivations, clip/fillna…), any **quantitative/financial math** (payoff, pricing, Greeks,
IV, risk, normalization), and any **architectural change** (package layout, layer
boundaries, public interfaces, data schema/column semantics, storage layout) MUST be
explained (logic of each step + rationale) and submitted to the owner for explicit
approval. Such code is **not "done" until the owner has fully verified it** — passing
tests are necessary but not sufficient.

**Why:** the owner personally verifies all math/architecture in this
trading library; an unverified-but-green implementation is worse than asking, because
wrong financial math looks correct.

**How to apply:** mark unverified code `# 4VERIFY (owner)`, keep the prior
version commented (marked the same) so the owner can compare, and mark the task "pending
owner verification" (not "Done"). When in doubt, present options and ask rather than
silently implement.

Codified as **D2** in `_forge/DEVELOPMENT_REQUIREMENTS.md` (architectural invariants
themselves are in `ARCHITECTURE_REQUIREMENTS.md` R0…R10). Example applied: the
`_calc_premium_profile` payoff math (T14b). See [env-and-test-running.md](env-and-test-running.md).
