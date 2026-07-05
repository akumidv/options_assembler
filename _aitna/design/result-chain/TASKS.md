# Design tasks — result chain & `alphavar.flow`

> **This is the DESIGN backlog** for the [result-chain concept](README.md) — *separate from the
> implementation backlog* ([`agents/_dev/TASKS.md`](../../TASKS.md)). A task here is
> "produce a design decision recorded in this concept folder", **not** code. Implementation tasks are
> only created once the relevant design tasks are *decided* and folded into [ADR 0003](../../../docs/dev/decisions/0003-composable-result-chain.md).
>
> IDs reuse the open-point numbering (A* = Layer A / flow mechanics, B* = Layer B / planner, X* =
> cross-cutting). Status: `[x]` decided · `[~]` leaning · `[ ]` open. Detail lives in the layer files.

## Priority reframe (2026-06-21, owner) — drives the ordering below
The **product is the lib + class contracts**, not `flow`. `flow` is **one of three** consumers of the
same descriptions: (1) `flow` auto-assembly, (2) a developer calling steps by hand, (3) an **AI agent**
assembling a chain. Therefore:
- [x] **P-reframe** Build order = **contracts at lib/class for options first → other domains → `flow`
      last**. `flow` implementation is **not a priority**; its design matters now only as it **shapes
      the lib/class code** so chains compose without it. Demotes A2a/A2b/Res/Run + all of Layer B to
      **Phase 2**; promotes Ref/A9-concrete/Sh1/A4a/A5 to **Phase 0**.
- [x] **P-selfdesc** New contract requirement: contracts are **self-describing & discoverable**
      (machine-readable `kind → I/O`) — because the AI-agent / manual caller are first-class consumers.
      Holds now, before `flow` exists. Test of done: `price_series → forecast` is assemblable by hand /
      by an agent **without importing `flow`**.
- [x] **P-autonomy** (2026-06-21, owner — *core*) **Strict component autonomy.** A component (lib
      function or domain class — incl. etl/exchange) knows **only its own contract**: the inputs it
      consumes (by kind/schema) and what it produces. It **never knows or invokes its upstream
      producer**. ⇒ **no `compute-if-absent` / resolver inside lib or class** — components take inputs
      **explicitly**. Composition (wire output→input, and "compute the prerequisite if missing") is an
      **assembler** concern, done one level up by the **user / AI agent / `flow`**. *This supersedes the
      earlier "provide-or-compute as a class-layer param" idea* and moves ADR 0003 §2 `resolve` out of
      the domain classes into the assembler layer.
- [x] **P-unblock** (revised under P-autonomy) **T27 it.5 factor-conditional, T33 portfolio, T35 risk
      still build directly on the class contracts, NOT blocked on `flow`** — but they take their
      upstream frames (e.g. a factor-scenario forecast) **explicitly passed in by the caller**, not
      derived inside the component. The caller (user/agent/flow) computes the upstream and passes it.
      (Supersedes ADR 0003 "remain `NotImplementedError` until Phase 2".)
- [x] **P-data** (2026-06-21, owner) **Data acquisition is in the unified graph.** etl/exchange/provider
      register **producer(s)** in the same contract vocabulary — a *load* is a producer with contract
      `{symbol, period → canonical frame-kind}`, a node like any other. So an assembler (esp. Layer B)
      can plan the whole path **including what to load**. The **fetch internals stay R1/R2** (wire
      format, exchange specifics hidden); only the **output frame-kind** is exposed to the chain.
- [x] **A5-now** Neutral chain terms (`quantile`, `value`, `horizon_years`, `confidence`, `target`,
      `model`, `engine`) go to a **neutral registry in `core.dictionary` now (Phase 0)**, before the
      second domain — not deferred behind spot/bonds.

## Decided (this round) — keep, don't re-litigate
- [x] **A0** Module = `alphavar.flow` (+ `composer` inside). → [flow-module](flow-module.md)
- [x] **A1** No `ResultMeta` entity — contract describes scalar I/O; provenance → `flow.RunRecord`.
- [x] **A3** Lineage collapsed into `flow.RunRecord` (not on results).
- [x] **A7** `flow` non-privileged — compatibility lives in descriptions; user can assemble by hand.
- [x] **A8** A `kind` is a contract, not forced decomposition — consolidated producers stay; intermediate
      interchange is opt-in.
- [x] **A9** Layering (lib `df+params→df` / class / flow) + **two shapes** (Shape 1 enrichment via
      column-deps · Shape 2 reduction = new-kind frame).
- [~] **A2** Registry = Python code (binds functions + pandera schemas). *(open sub-tasks below)*
- [~] **A4** Interchange = tidy frame; keep wide `to_frame()` as human render.
- [~] **B3** `Plan` = declarative data (YAML/dict), validated at load.

## Phase 0 — options lib/class contracts (PRIORITY; shapes the code, no `flow` needed)
> The deliverable is contracts that **stand alone** (P-selfdesc): usable by hand / by an agent without
> `flow`. Done here = the lib/class layer for options is composable and self-describing.
- [ ] **Ref** Class-vs-lib refactor (the spine): lib returns `df` (`df+params→df`); classes
      (`SmileResult`/`ForecastResult`/…) wrap + carry recomputed scalars. Incremental migration order.
- [x] **A9c** A9 made concrete: every options lib function classified Shape 1 / Shape 2 / kernel →
      **[`a9c-lib-inventory.md`](a9c-lib-inventory.md)** (2026-06-21). Surfaced design points D-a/D-b/D-c/
      D-d/D-h below.
- [ ] **D-a** Ruling on **kind-preserving reductions** (`timeframe_resample`, `validation/clean`,
      `reference/_scd.as_of` — fewer rows, same schema): a third micro-pattern, or fold into Shape 1
      (compat = columns) / Shape 2 (compat = kind)?
- [ ] **D-b** (= A4a generalized) **Catalog of Shape-2 kinds + their pinned interchange schema +
      scalar-spec**: `smile_fit`, `forecast_dist`, `smile_forecast`, `surface_forecast`, `payoff_curve`/
      `payoff_summary`, `time_value_series`, `chain`, `desk`, `reference_split`, `validation_report`.
      Many emit objects/lists/tuples today → need a tidy `to_interchange()` each.
- [ ] **D-d** (= A8 concrete) **Producer-vs-kernel rule**: which lib functions are contract nodes
      (producers) vs internal math/selection/IO kernels (no contract). The kernel set is large.
- [ ] **D-g** (= A5 scope) Which neutral `ResultTerm`s each Shape-2 kind needs beyond the V1 set
      (payoff terms, time-value, validation severity).
- [ ] **D-h** **I/O inside `lib/reference/_store`**: model `read/write_reference` as `load`/`store`
      producers (P-data) vs leaving them as an I/O concern; the load/store boundary in the unified graph.
- [ ] **D-i** (= class scalar-spec) The recomputed scalars each result class carries (forecast:
      `as_of`/`spot`; smile: `t_years`/`forward`; chain: `settlement`/`expiration`).

> **Phase-0 remaining-design snapshot (post-A9c, 2026-06-21):** core = **D-b** (catalog of Shape-2
> kinds + schemas) and **D-d** (producer-vs-kernel boundary); the rest (Sh1/Ref/A5-scope/D-i) are
> consequences. **V1-lc is the reference** these generalize from → building it next. Layer B (B1–B6)
> stays deferred with the flow planner.
- [ ] **Sh1** Formalize Shape-1 contracts: generalize the existing `OPTION_COLUMN_DEPENDENCIES`
      (column → required columns) to carry enrichment params; this is the Shape-1 compatibility surface.
- [ ] **A4a** Pin the pandera schema per options **Shape-2 kind** (`SmileResult`, `ForecastResult`,
      `SmileForecast`, `SurfaceForecast`) + `to_interchange()` (start: `ForecastDistributionSchema` =
      `quantile|value|change`; the quantile-grid convention).
- [ ] **A5** Neutral chain-term registry in `core.dictionary` (`quantile`/`value`/`horizon_years`/
      `confidence`/`target`/`model`/`engine`) — **now** (P decision A5-now), so options schemas already
      reference neutral terms.
- [ ] **Inp** Explicit-input contract (P-autonomy): each producer **consumes its inputs explicitly**,
      no `compute-if-absent`. Pin down how a producer declares the input kinds it needs **without**
      reaching back to who makes them. Revisit `source=`: choosing among a producer's own inputs is OK;
      silently building an upstream series is **not** — that becomes a separate `price_series` producer.
- [ ] **Load** Data acquisition as a producer (P-data): express etl/exchange/provider as producer(s)
      whose output is a **canonical frame-kind** (`{symbol, period → board/series}`); fetch internals
      stay R1/R2. This is the first node of the unified graph.
- [ ] **Disc** Self-describing surface (P-selfdesc): expose `kind → (input kinds, param-spec, output
      schema + scalar-spec)` as readable data **off the contracts themselves** (no `flow` import), for
      manual + agent assembly.
- [~] **V1-lc** Price slice: `load → price_series (timestamp|price) → forecast_distribution
      (quantile|value|change) + scalars{as_of,spot,horizon_years}` — three **autonomous producers**
      (explicit inputs) + schemas + `to_interchange()`; wired by the assembler, not inside any producer.
      **Spec specced + decided → [`v1-price-slice.md`](v1-price-slice.md)**. Decisions (owner 2026-06-21):
      **(a)** reduce `Option.forecast.price()` to the `forecast_distribution` producer; the end-to-end
      convenience = a **minimal `flow` prototype** that **reads the Disc contracts** (not a facade method /
      recipe); neutral terms = sibling **`ResultTerm`** in `core.dictionary`. ⇒ a contract-reading `flow`
      **seed lands in V1** (the formal Contract/registry/planner stays Phase 2). Ready for implementation
      setup.

## Phase 1 — replicate the contract pattern to other domains
> Only after the options pattern (Phase 0) is proven.
- [ ] **X1** Multi-domain concretely: how `spot` / `bonds` register producers; neutral vs domain kinds
      (lift the neutral terms from A5 as the shared vocabulary).
- [ ] **X2** Portfolio split detail: neutral distributional aggregation vs options payoff aggregation.

## Phase 2 — `alphavar.flow` itself (DEFERRED; convenience layer over standing contracts)
> `flow` only mechanizes what hand/agent assembly already does off the Phase-0 contracts.
- [ ] **A2a** `Contract` shape in code: input kinds, `ParamSpec`, output frame-schema + scalar-spec —
      concrete dataclass/decorator form (formalizes the Phase-0 self-describing surface).
- [ ] **A2b** Acyclicity / validation rules for the registry (+ how a missing kind is reported).
- [ ] **Res** Resolver execution semantics: shared-node caching/memoization, eager vs lazy, error /
      partial-failure handling.
- [ ] **Run** `flow.RunRecord` format (provenance/lineage of a run; reproducibility from `seed`).
- [ ] **A6** How an input kind expresses a **data envelope** ("the board over period P"), so Layer B
      can derive the requirement backward.
- [ ] **V1-flow** Bind the V1 price slice into the `flow` registry as a declarative `Plan`.
- **Layer B — demand-driven planner** (later still; A must not block it):
  - [ ] **B1** Subject-of-analysis model (the alpha-search entry point).
  - [ ] **B2** Backward-derivation algorithm (goal → data envelope + params) + cycle detection.
  - [ ] **B4** Assembler: explicit vs derived; eager/lazy; shared-node caching across runs (sweeps).
  - [ ] **B5** Structure / legs + aggregation node; cross-leg coupling (correlation/copula).
  - [ ] **B6** Data envelope / context entity: what it is; load-once-and-share.

## Cross-cutting (apply across phases)
- [ ] **X3** Serialization / persistence of results across runs (the R4 *revisit-if* for a light
      self-describing export envelope).
- [ ] **X4** D2 verification plan for the new schemas (Type C where math) — what gets a ledger row.
- [ ] **X5** Consolidation pass: cross-check coherence README ↔ flow-module ↔ structure-requirements ↔
      rejected-branches, then fold the locked decisions (incl. this reframe) into ADR 0003.
