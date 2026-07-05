# Layer B — demand-driven planner, structures & requirements

> Hub: [`README.md`](README.md). This file = the **later** layer (but Layer A's contracts must not
> block it). The heart of **alpha search**: think in terms of the *subject of analysis*, not the data
> you happen to have. Dead-ends → [`rejected-branches.md`](rejected-branches.md).

## The demand-driven idea (why a separate layer)

Layer A is forward/data-driven ("given inputs, produce outputs per contract"). Layer B is the reverse:
start from the **desired output** (the goal / subject of analysis) and walk the chain **backward** to
derive what is needed.

```
   goal (subject of analysis)            e.g. "the risk of THIS structure over horizon H"
     → which calculation produces it
       → what inputs its contract declares
         → … recursively (off the registry contracts) …
           → the DATA ENVELOPE it ultimately needs (which instruments, which day/period,
             the whole board vs just the legs) + the PARAMETERS at each step
```

The planner **derives** the data requirement and parameter set from the goal — it does **not** start
from "here is a dataframe, what can we compute". This is the alpha mindset and the reason A and B are
split: A is stable plumbing; B is the evolving *research* question (which subjects / envelopes /
parameter sweeps to search). B re-points at a new subject without touching A; A gains a producer
without B knowing how it'll be assembled. The **`Plan`** is the interface between them.

## Open points

### B1. Subject-of-analysis model — pending
How a "goal / subject of analysis" is expressed so the planner can start from it (the alpha-search
entry point). It names the leaf calculation + the target structure, not the data.

### B2. Backward derivation — pending
How the planner walks outputs → inputs → **data envelope + parameters** off the registry contracts
(flow-module A2/A6). Cycle detection (the graph must stay acyclic; a factor that is itself a forecast
composes one level at a time).

### B3. The `Plan` as the A/B interface — *leaning declarative (YAML / dict)*
What a `Plan` carries so the Layer A resolver executes it forward: the **DAG** (nodes = producer
instances, edges = output→input wiring), the **data envelope** to load once, and the **parameter set**
per node. The `Plan` is the *forward blueprint*; `flow.RunRecord` is the *backward trace* after a run
(the two are views of the same DAG).

**Storage (owner, 2026-06-20):** the `Plan` is **pure data** (kinds + edges + scalar params, no
function refs) → **declarative YAML or dict**, authorable by a human *or an AI assistant*, validated at
load against the Registry (typo in a `kind` / unsatisfied schema is caught there). Contrast the
**Registry**, which must be **Python code** (binds to real functions + pandera schemas — A2). Example:

```yaml
nodes:
  ps:  { kind: price_series,          inputs: { board: $board }, params: { source: front } }
  fc:  { kind: forecast_distribution, inputs: { series: ps },    params: { horizon: 30d, model: gbm } }
  var: { kind: var,                   inputs: { dist: fc },      params: { confidence: 0.99 } }
```

### B4. Assembler — explicit vs derived; execution — pending
Caller declares the legs + target leaf and the steps are **derived** from the registry, vs. a fully
explicit plan. Likely both (declare legs + leaf, derive between). Eager vs lazy execution; **caching
of shared DAG nodes** so a shared upstream (the cleaned/filled board) runs **once** and is reused
across legs and branches.

### B5. Structure / legs + aggregation node — pending
The **horizontal axis**. A **structure** = `[leg, …]` + an aggregation node. The per-leg vertical
sub-chain **fans out** (each leg: load→clean→fit→forecast); the **collective fan-in** combines the leg
results — its inputs are the leg results **+ a cross-leg coupling param** (correlation/copula). The
aggregation is *collective*, not a naive sum of independent legs. This is the T33 (portfolio) / T35
(risk) shape; reuses the existing options leg/payoff machinery. Note the split from
[`flow-module.md`](flow-module.md): **neutral distributional aggregation** (chain-level) vs.
**options payoff aggregation** (options-domain).

### B6. Data envelope / context entity — pending
The "all info for a day / period" the planner pulls — possibly the **whole board + history**, not just
the legs (a single option-contract entity is **too narrow** for what fit/forecast need — owner,
2026-06-20). Domain-specific in *content* (option board vs spot bars vs bond schedule), uniform in
*role*. What this context object is, and how `flow` loads it **once** and shares it across the DAG
(ties to B4 caching).

## Ideas & problems to work on (parking lot)

Promote to an open point when picked up.

- **Parameter sweeps for alpha.** Layer B is a research surface — the same DAG over a grid of
  params/subjects/envelopes. The planner should make a sweep cheap (reuse shared nodes across runs).
- **Reproducibility.** `seed` + `flow.RunRecord` ⇒ a run is exactly reproducible / auditable from the
  record (not from a meta on each result).
- **Branches not yet drawn.** `fill missing K/expiry via surface` is itself a producer (consumes a
  `fit`/`surface` result → emits a completed board) — add as a registry kind. Same for
  `clean`/`validate` as nodes (today facade methods, not chain nodes).
- **Single-entity inconvenience (owner, 2026-06-20).** Driving the chain off one option-contract
  entity is awkward with several legs and when forecast needs a full day's/period's context → pushes
  toward B5 (structure) + B6 (context entity) as first-class, and the planner deriving the envelope
  rather than the caller hand-assembling it.
