# Architecture Requirements — alphavar

> **Status: binding.** This document captures the current architecture as a set of
> structural requirements (`R#`). Any change (human or AI-agent) MUST preserve these
> invariants unless the document itself is explicitly revised first. **Verify these
> especially when introducing new entities/domain concepts or making serious changes to
> the existing domain model.** For compact day-to-day development rules (quality gates,
> owner verification, workflow) see the companion
> [DEVELOPMENT_REQUIREMENTS.md](../../_forge/DEVELOPMENT_REQUIREMENTS.md) (`D#`). Descriptive overview:
> [PROJECT_OVERVIEW.md](PROJECT_OVERVIEW.md). The remediation backlog is maintained
> outside this repository, alongside `ALPHAVAR_NAMING.md`.

## R0. Package layout: domain-first, then by layer, then by function

`alphavar` will grow beyond options/futures (equities, bonds, …). The top-level split is
**by domain**; *inside* a domain the split is **by layer, then by function within the
pure-logic layer**. A thin domain-neutral base sits under the domains.

```
alphavar/
  core/        # domain-NEUTRAL base: shared dictionary registry, normalization,
               #   base entities, schema mixins, path-safety. No domain math.
  io/          # data infrastructure, domain-neutral: provider/, exchange/, messanger/
  options/     # DOMAIN: options + futures. Inside, BY LAYER then function:
               #   <name>_class.py   — facade (Option + its components), flat at the domain
               #                       root; stateful, holds OptionsData / the provider.
               #   dictionary/ entities/ schemas/
               #                     — domain foundation: term vocabulary, entities,
               #                       validation contracts (used by every layer).
               #   lib/              — pure computational logic (DataFrame in/out, no I/O;
               #                       the Polars-port target, R8), by function:
               #                       analytic/ chain/ chart/ enrichment/ normalization/
               #   etl/              — I/O orchestration (uses io/ providers).
  spot/        # future domain (own math, same internal shape)
  bond/        # future domain
```

- **Why domain-first:** the math of options ≠ equities ≠ bonds; keeping each domain's
  logic, ETL and analytics together makes "add a new asset class = add a package"
  true, and a domain is visible from the tree (screaming architecture).
- **Why layer-then-function inside a domain:** the three-layer separation (R1) is made
  physical, so the boundary is visible in the tree and enforceable by import rules. The
  facade (stateful classes) sits flat at the domain root; the pure-logic layer is the
  `lib/` package and is organized by function inside it; `etl/` is the I/O-orchestration
  layer. `dictionary/`/`entities/`/`schemas/` are the shared domain foundation that every
  layer depends on, so they sit at the domain root, above `lib/`.
- **`lib/` is pure:** functions are `DataFrame` in → `DataFrame` out, with no I/O, no
  network, no provider/exchange imports, no facade imports, no global mutable state. This
  is what makes it the strategic Polars-port target (R8) and independently extractable.
  Import direction inside a domain is one-way: facade → `lib/` → (data via injected
  provider from `io/`); `etl/` orchestrates I/O on top.
- **What stays neutral (not in a domain):** `core/` (shared identity/dictionary,
  normalization, base entities — see R4.x; "core + domain extensions") and `io/`
  (an exchange returns options, futures, and tomorrow equities — it is infrastructure,
  not a domain).
- **Naming:** the first domain keeps the recognizable name `options` (not
  `derivatives`), package `alphavar.options`. This aligns with the family pattern
  recorded in `ALPHAVAR_NAMING.md` (`alphavar.<domain>`).
- **Every domain repeats this internal shape** (`<facade>_class.py` + `dictionary/`
  `entities/` `schemas/` + `lib/` + `etl/`), so the layer of any module is readable from
  its path regardless of domain.

## R1. Three-layer separation (core invariant)

```
domain facade (stateful)  →  <domain>/lib (pure functions)  →  io.provider, io.exchange (I/O)
```

1. **Facade layer — `src/alphavar/<domain>/*_class.py`** (e.g.
   `options/option_class.py`, `options/chain_class.py`): stateful classes only —
   `Option`, `OptionsData`, `OptionsEnrichment`, `OptionsChain`, `OptionsAnalytic`,
   `ChartClass`. They hold DataFrames, the provider reference, and request parameters.
   They contain **no computational business logic** — they orchestrate and delegate.
2. **Logic layer — `src/alphavar/<domain>/lib/`** (e.g. `options/lib/`): pure, stateless
   functions and Pydantic entities: `DataFrame` in → `DataFrame` out. **No I/O, no
   network, no `io` (provider/exchange) imports, no facade imports, no global mutable
   state.**
3. **Data layer — `src/alphavar/io/provider/` + `src/alphavar/io/exchange/`** — the only
   place where I/O happens (files, HTTP); domain-neutral infrastructure (R0). Exchanges
   implement `AbstractProvider` (via `AbstractExchange`).

Dependency direction is one-way: facade → logic → (data via injected provider).
`<domain>/lib` must never import from the domain facade modules or from `io`.

### R1.1 `lib` contract shapes: explicit inputs, no hidden upstream work

Every public function in `<domain>/lib` exposes one of these shapes; the shape is part of
the function's contract and should be visible from the signature and return annotation:

- **Transform:** `df + params → df` (or a row-aligned `Series`). The DataFrame being
  transformed is the first parameter; required extra data is passed explicitly as a
  later parameter. The function does not load, resolve, or compute its upstream inputs.
- **Reduction / producer:** `df + params → typed result` or `df + params → tidy frame`.
  The output has a pinned schema or result class that exposes an interchange schema.
- **Factory:** `make_*` selectors build interchangeable algorithms from a selector/spec
  and are exempt from the data-first rule.
- **Numerical kernel:** arrays/scalars in → arrays/scalars out; no DataFrame, I/O, or
  facade state.

The resolver / "provide-or-compute" behavior is not a domain-class or `lib` concern. It
lives one level up: a user script, an AI agent, or `alphavar.flow` may assemble a chain
from the self-described producer contracts. Domain classes and `lib` functions consume
only the inputs they are explicitly given.

### R1.2 Storage adapters are outside `lib`

`<domain>/lib` may contain pure reference logic (split/reapply reference data, SCD-2
folds, as-of joins), but it must not contain file, parquet, JSON, network, environment,
or path-creation operations. Persistence adapters live in `io/provider`, `etl`, or a
dedicated storage-adapter module outside `lib`, and call the pure `lib` functions.

If a transitional adapter exists in `lib`, it must be tracked by a remediation task and
must not become the pattern for new work.

## R2. Provider pattern

- Every data source implements `AbstractProvider`
  (`io/provider/_abstract_provider_class.py`): `get_assets_list`,
  `get_asset_history_years`, `load_options_history`, `load_options_book`,
  `load_futures_history`, `load_futures_book`, `load_options_chain`.
- File-based sources extend `AbstractFileProvider`; HTTP exchanges extend
  `AbstractExchange` and are registered in `ExchangeFabric` /
  `ExchangeProviderFactory`.
- The `Option` facade receives the provider by constructor injection and never
  instantiates providers itself.
- Request scoping goes through the `RequestParameters` Pydantic model — no ad-hoc
  parameter dicts.

### R2.1 Uniform exchange interface — internal identity in, venue symbol stays inside

The public provider/exchange API speaks the library's **internal identity**, never the
venue's. Every method takes `asset_code` (+ typed scope: expiration, strike, type,
`RequestParameters`) — **not** an exchange `exch_symbol`. All exchanges are therefore
called **identically**; swapping Deribit for MOEX changes no call site.

- Building the venue request symbol from `asset_code` (and scope) is the **exchange
  class's** responsibility, done internally. The transform `asset_code → exch_symbol`
  (e.g. `BTC` + 30APR25 + 100000 + C → `BTC-30APR25-100000-C`) lives in the concrete
  exchange, nowhere else.
- On the way back, the exchange parses the venue response into the canonical columns
  (R4.1.1) — `asset_code` plus typed fields — and may keep `exch_symbol` as the optional
  raw audit column. Callers never see or pass a venue symbol.
- No public provider/exchange method accepts a `symbol`/`exch_symbol` parameter. (After
  T1b they take `asset_code`; this requirement keeps it that way.)

### R2.2 Project enums are separate from exchange API parameters

A project enum value (its `.value`/`.code`) is the **internal** name of a concept; it is
**not** automatically the string an exchange API expects. Never send a project enum
value straight onto the wire — map it explicitly per exchange.

- Each exchange owns an explicit **project-enum → API-string** mapping (e.g.
  `_DERIBIT_API_KIND[DeribitAssetKind] -> 'option' | 'future' | …`), used when building a
  request. The wire spelling lives only in that mapping, in the exchange module.
- Rationale (real bug, 2026-06-14): `DeribitAssetKind.OPTION.value` was `'options'`
  (inherited from `AssetKind.OPTIONS.value`) and was sent as the Deribit `kind=` param,
  but Deribit wants singular `'option'` → HTTP 400, so **Deribit option snapshots
  silently failed**. The fix was an explicit API-kind map, decoupling the internal enum
  from the venue's wire format.
- Symmetric to R2.1 (identity) and R4.5 (internal classification values): internal names
  stay internal; the exchange layer translates at the boundary, both directions.

Rationale: a venue symbol is an exchange-specific encoding; leaking it into the shared
API would couple callers to one venue's format and break the "new data source =
new provider, no caller changes" rule.

## R3. Facade composition

- `Option` aggregates components that all share a single `OptionsData` instance
  (dependency injection of shared state). New capability areas (pricer, forecast,
  validation) follow the same pattern: a component class taking `OptionsData` in its
  constructor, exposed as an attribute of `Option`.
- Facade methods are bindings over shared state: they may select the relevant stored
  frame, call pure `lib` functions, validate boundary contracts, and assign the result
  back to `OptionsData`. They must not hide material upstream computation that is not
  named in the method contract. If a calculation needs another frame, result, or model
  output, the caller/assembler passes it explicitly or invokes a clearly named
  enrichment/producer step first.
- **Model-factory pattern.** A capability area that offers *interchangeable algorithms*
  exposes them through a pure-`lib` factory: an abstract base + a name→class registry + a
  `make_*` selector (instance pass-through; unknown name → `ValueError`, catalogued-but-unbuilt
  → `NotImplementedError`). Adding an algorithm = a subclass + a registry entry, no caller
  change. Established by smile (`make_smile_model`); forecast generalizes it to three orthogonal
  axes — target × process × engine — see [ADR 0002](decisions/0002-forecast-model-factory-axes.md).

## R4. Data dictionary discipline

- DataFrame column names, option types, price statuses, asset kinds, timeframes, and
  currencies are referenced **only** through the enums in
  `options/dictionary/` (`OptionsColumns`, `FuturesColumns`, `SpotColumns`,
  `OptionsType`, `Timeframe`, …). String literals for columns are forbidden.
- A new computed column requires: an `OptionsColumns` entry, the pure function in
  `options/lib/enrichment/`, an entry in `OPTION_COLUMNS_DEPENDENCIES` when it depends
  on other columns, and wiring in `OptionsEnrichment`.

### R4.1 Naming: singular vs plural for `option`/`future`

`option`/`future` are domain entity names. The rule is grammatical, not stylistic:

- **Plural** (`options`/`futures`) for anything denoting a *category or collection*:
  asset-kind values stored in data (`AssetKind.OPTIONS = "options"` — already in
  parquet, do not change), dictionary classes (`OptionsColumns`, `FuturesColumns`),
  API method families (`load_options_history`, `load_futures_history`), package/path
  segments, and any variable holding a set/list/DataFrame of instruments
  (`options_df`, not `option_df`; `BookData.options`, not `.option`).
- **Singular** (`option`/`future`) **only** as a qualifier of one attribute of a
  single instrument: `option_type`, `option_style`, `option_symbol`,
  `future_expiration_date`. A bare singular `option`/`future` identifier is a smell —
  it almost always denotes a collection and should be plural.

There is currently no legitimate use of a bare singular `option`/`future` for a single
entity in the code; the few that exist (`BookData.option/.future`, the `option = []`
accumulator in `moex.py`) are collections and should be pluralized.

**Domain convention takes precedence over grammar.** In derivatives trading the asset
classes are *the options market* / *the futures market* — established industry usage
(CME, Databento, Xignite and other data APIs use the plural for the instrument class).
So the plural is the correct domain name, and it applies to **all identifier kinds**,
not just variables:

- **Packages / directories**: the domain package is plural (`options`; a future
  `equity`/`bond`), as are collection subpackages (`entities/`). A new asset-class area
  follows suit.
- **Files / modules**: name by the entity in its domain-correct form
  (`option_class.py` describes a single `Option` *instrument* → singular is fine;
  a module about the options *dataset/market* uses plural).
- **Classes**: `Option` (one instrument) is singular by design; `OptionsColumns`,
  `OptionsChain`, `OptionsAnalytic` (the options domain/collection) are plural.
- **Enums / values stored in data**: `AssetKind.OPTIONS = "options"` (plural, already
  in parquet).

Rule of thumb: *one contract* → `Option`/`Future` (singular); *the market, dataset,
column group, or any collection* → `options`/`futures` (plural). When in doubt, match
how exchanges and market-data APIs name it.

### R4.1.1 Instrument identification: two-level model

Two distinct identity concepts, deliberately different columns (the legacy `symbol`
naming conflated them):

| Concept | Column | Examples | Role |
|---|---|---|---|
| Underlying asset | `ASSET_CODE` (`asset_code`) | `BRN`, `BTC`, `AAPL` | The base asset a contract is written on. The **unifying key**: a future and an option on BRN share it. Short, stable; suitable for file/dir names. Present for every row. The library's own identity — exchange-neutral. |
| Exchange instrument symbol | `EXCH_SYMBOL` (`exch_symbol`) | `BTC-30APR25-100000-C`, `BR-3.25`, `AAPL` | The venue's raw contract identifier (exchange-facing ticker). Encodes expiry/strike/right in a string. Prefixed `exch_` like all other raw venue data (R4.2). |

This matches industry usage: *symbol/ticker* = exchange-facing instrument code;
*asset* = the underlying. So `asset_code` ≠ `exch_symbol` — separate columns, not
synonyms. (For a spot asset like `AAPL` the two coincide.) The legacy `symbol` /
`exchange_symbol` naming is replaced by `exch_symbol` (and `asset_code` for the
underlying).

**`exch_symbol` is not part of the canonical parsed dataset.** Once a book is
normalized, the contract is fully and compactly identified by typed columns
`(asset_code, expiration_date, strike, option_type, timestamp)`. The raw `exch_symbol`
string is then redundant and storage-expensive (a string per row). Keep it only:
- in **raw book snapshots** (it is what the exchange sent; the source for parsing/debug);
- as an **optional** audit column in parsed data, behind an ETL flag (like
  `source_fields`) — never a required column.

The mandatory row key for parsed options is
`(asset_code, expiration_date, strike, option_type, timestamp)`; for futures
`(asset_code, expiration_date, timestamp)`. `base_code`/`underlying_code` remain their
own concepts (sub-asset / underlying contract), distinct from `asset_code`.

### R4.2 Price / IV column model

There are several independent price (and IV) concepts per instrument. They are
**distinct columns**, never overwritten into one another. Three groups:

**1. The project's own values (no prefix) — the headline columns.**
`PRICE` (`price`) / `IV` (`iv`) are **ours**, not the exchange's: the output of our
normalization pipeline (Black-Scholes pricing, volatility-smile fitting, arbitrage
removal). These are the canonical columns users consume; everything in the library
defaults to them. There is **no** separate `fair_*` pair — `price`/`iv` *are* our fair
value.

| Concept | Column | IV | Meaning |
|---|---|---|---|
| Our normalized value | `PRICE` (`price`) | `IV` (`iv`) | Output of our model (BS + smile fit + no-arbitrage). The library's default price/IV. |

**2. Real exchange values (`exch_` prefix) — what the venue actually published.**
Kept so our normalization is auditable and recomputable. An exchange publishes one fair
estimate, so Deribit `mark_price` and MOEX `theorprice` both map into the single
`exch_mark_*` pair (we do not split mark vs theoretical).

| Concept | Column | IV | Meaning |
|---|---|---|---|
| Exchange traded price | `EXCH_PRICE` (`exch_price`) | `EXCH_IV` (`exch_iv`) | The venue's traded/quoted price as received. |
| Exchange mark/estimate | `EXCH_MARK_PRICE` (`exch_mark_price`) | `EXCH_MARK_IV` (`exch_mark_iv`) | The venue's fair-value/mark estimate (Deribit `mark_price`, MOEX `theorprice`). An estimate, not a trade. |
| Settlement (EOD) | `SETTLE_PRICE` (`settle_price`) | `SETTLE_IV` (`settle_iv`) | Official daily clearing/settlement price. EOD-only; null intraday. Term is unambiguous, so no `exch_` prefix. |

`ask`/`bid`/`mid`/`last`/`high`/`low` keep their own columns; `price`/`iv` are never
mere copies of them.

**The `exch_` rule generalizes beyond price** to any field that holds a raw venue value
alongside our own derived one. Timestamps follow the same split:

| Concept | Column | Meaning |
|---|---|---|
| Our request moment | `REQUEST_TIMESTAMP` (`request_timestamp`) | When *we* fetched the snapshot (`pd.Timestamp.now`). Ours. |
| Exchange timestamp | `EXCH_TIMESTAMP` (`exch_timestamp`) | The venue's own timestamp on the record (Deribit `creation_timestamp`, MOEX `updatetime`). One column per venue value, like `exch_mark_*`. Renamed from the old `original_timestamp`. |
| Our normalized moment | `TIMESTAMP` (`timestamp`) | The working, normalized instant the library uses (rounded to 1s). Ours — no prefix. |

(Deribit `creation` vs MOEX `update` are different venue times conflated into
`exch_timestamp` today, mirroring the mark/theor case; splitting them is deferred.)

**Timestamp resolution — milliseconds at most, never nanoseconds.** Every datetime the
library stores (in parquet and in memory) is at **millisecond resolution or coarser**;
rounding down to **one second is acceptable** — 1 s is the minimum analysis timeframe
(the normalized `timestamp` is already rounded to 1 s). Sub-second precision carries no
analytical value here, so **do not add code to preserve nanoseconds** — let parquet's
default `ns → ms` coercion stand (don't pass `version="2.6"` or similar). When
round-tripping timestamps through storage, compare *instants*, not the exact dtype unit
(`s` and `ms` differ as raw integers but denote the same moment).

**3. Raw pre-transform values (`_raw` suffix) — narrow, only where we mutate.**
A `<col>_raw` column preserves an exchange value **before an irreversible transform we
apply**, so the transform can be reverted/recomputed. It is **not** a mirror of every
column. Today the only such transform is currency conversion in `deribit.py`
(multiplying by `estimated_delivery_price` to convert base→quote): the pre-conversion
value of each affected column (`ask`, `bid`, `last`, `high_24`, `low_24`,
`exch_mark_price`) is stored as `<col>_raw` (`ask_raw`, `exch_mark_price_raw`, …). Add
`<col>_raw` **only** when introducing a new irreversible per-exchange transform — not by
default. (Replaces the old `source_`/`SOURCE_PREFIX` prefix.)

The `_raw` **suffix** (not a prefix) is deliberate: it keeps a value and its raw form
adjacent under column sorting / prefix filtering (`exch_mark_price`,
`exch_mark_price_raw`), and matches the dictionary's existing suffix-as-modifier
pattern (`high_24`, `volume_notional`). A `source_` prefix would be the only prefixed
modifier and would scatter raw values into a separate namespace.

Short prefixes (`exch_`, `exch_mark_`, `settle_`) + the `_raw` suffix keep names compact
while explicit. Rationale: `price`/`iv` belong to the project (the value of the library
is the normalization), so they get the unprefixed names; raw venue data is explicitly
`exch_*`/`settle_*`; `mark` is the right domain term for an exchange estimate (vs
settlement = official EOD price). The old single `exchange_mark_price` had a typo
(`exhchange_…`) and conflated layers. Migration is covered by the backlog (T23.6).

### R4.3 Single term registry — one name per concept, everywhere

There is **one** registry of data terms (the **term dictionary** — `Term` in
`core/dictionary/`, `OptionsTerm` in the domain). A *term* is the canonical name of a data
concept; the registry is **not** a list of columns — a column is only one of a term's uses.
A concept has exactly one term there, and that term is **the only** spelling used for it
across the whole codebase, in every position:

1. **DataFrame columns** — referenced only via the registry (`Term.STRIKE`), never as a
   string literal. (Existing R4 rule, now part of the registry contract.)
2. **Variables, parameters, attributes** — a variable, parameter, or attribute holding one
   concept is named after its term, **especially in the functions that compute or modify
   that data**: the local that holds a strike, the parameter that receives it, and the
   column it writes are all `strike`. So `asset_code` (not `symbol`/`code`/`ac`),
   `exch_symbol`, `expiration_date`; a collection is the plural (`asset_codes`). This binds
   the in-code vocabulary to the in-frame/on-disk vocabulary — one term, one understanding,
   everywhere.
3. **Function names that handle a term — `<verb>_<term>`, never `<term>_<noun>`.** A function
   whose job is to compute, read, or transform the value of one term is named *verb + that term*
   — the term is the noun, a verb says what is done to it:
   - producing term `X` → `add_<x>` / `get_<x>` / `calc_<x>` (e.g. `add_intrinsic_value`
     produces `Term.INTRINSIC_VALUE`; `get_price_status` returns `Term.PRICE_STATUS`;
     the implied-vol computation is `calc_iv` / `add_iv`, **not** `iv_calculation`).
   - the verb says the effect (`add_` mutates/returns the frame with the term's column, `get_`
     returns the series/value, `calc_` is the pure computation), the noun is the exact term.
   - a function keyed to a concept must not drift from the term (no `add_timevalue` for
     `Term.TIMED_VALUE`, no `iv_calc`/`compute_implied_volatility` for `Term.IV`). One term →
     one spelling in the column, the variable, *and* the function that produces it.

**Why:** the same concept must be greppable and unambiguous from column to variable to
function to file. If `Term.TIMED_VALUE = "timed_value"`, then the column, the local var,
the param, and `add_timed_value` all read `timed_value`. This is what makes the rename
discipline (asset_code, exch_symbol, exch_mark_price, …) enforceable and what lets the
pandera schema layer (R4.4) bind to the registry by reference, not by repetition.

The registry is **engine-neutral** (plain strings; no pandas/polars types in it — R8)
and **layered** (core base names + per-domain extensions, R0/R4 "core + domain
extensions"): generic concepts (`timestamp`, `price`, `iv`, `asset_code`, greeks) live
in `core`; domain concepts (`strike`, `option_type`, `price_status`) live in the
domain's dictionary and extend core.

### R4.4 Schemas bind to the registry, never restate names

DataFrame schemas (pandera `DataFrameModel`s) are the validation + dataset-composition
layer. Every schema field binds to a registry name **by reference**
(`pa.Field(alias=Term.STRIKE)`), never by retyping the string. Shared column groups
(timestamp / quote / OHLC / greeks) are **mixin** models; entity models
(`OptionsHistory`, `FuturesHistory`, `SpotHistory`, `OptionsBook`) compose mixins +
domain fields. Validation runs at layer boundaries (provider/exchange normalize output;
enrichment in dev) with `strict=False`, `coerce=True`, `lazy=True`; disabled in
production ETL via config. See the backlog (T23) for the build-out.

Result/interchange frames follow the same rule. Any output intended to feed another
calculation gets a named schema (or a result class with an `interchange_schema`) before it
is treated as a chain contract. The schema is the compatibility surface between
capability areas; `flow` and other assemblers read it, they do not redefine it.

### R4.5 Classification axes — one word per axis

An instrument is classified along several **independent axes**. Each axis is a distinct
column and a distinct enum, and each classifier word (`kind`, `class`, `right`, `style`,
`tenor`) is reserved for exactly one axis — never reused. The legacy `type` was
overloaded (held a *kind* in `asset_type`, a *class* in `underlying_asset_type`, a
*right* in `option_type`); it is retired in favor of these:

| Axis | Column | Enum | Stored values | Where it lives | Notes |
|---|---|---|---|---|---|
| Instrument kind | `instrument_kind` | `InstrumentKind` | `option` / `future` / `spot` (**singular**) | per-row column (`core`) | the *form* of instrument. Singular — one row is one contract, matching exchange APIs (Deribit `kind="option"`). Was `asset_type` (mislabeled) with plural values. |
| Asset class | `asset_class` | `AssetClass` | `equity` / `commodity` / `crypto` / `index` / `currency` | property of `asset_code` (`core`) | nature of the *underlying*. Was the `AssetType` enum. |
| Contract kind | `contract_kind` | `ContractKind` | `vanilla` / `cso` / `stir` / `combo` … | per-row column (`core`) | same asset class, different product. Deribit `*_combo` → here. |
| Option right | `option_right` | `OptionRight` | `call` / `put` | per-row column (`options` domain) | the *right* (call=buy, put=sell). **Not** `side` (side = buy/sell of a position, a different concept reserved for legs). Was `option_type`. |
| Option style | `option_style` | `OptionStyle` | `american` / `european` | reference property of the instrument (`options` domain) | changes the pricing model. |
| Series tenor | `series_tenor` | `SeriesTenor` | `weekly` / `monthly` / `quarterly` | series property (`options` domain) | separate trading series on one asset class. Lower priority. |

Rules:
- **Stored values describing one row are singular**, following the domain's name for a
  *single contract* (`instrument_kind="option"`, not `"options"`) — this overrides any
  notation preference and matches exchange APIs. This is the data-value counterpart of
  R4.1: package/class names for the *domain/collection* stay plural (`alphavar.options`,
  `OptionsColumns`), but a value in a per-row column names *one* instrument, so singular.
  **Migration:** today `AssetKind` stores plural (`"options"`/`"futures"`) in parquet and
  in the dir layout (`DERIBIT/BTC/options/…`); moving to singular requires a parquet +
  path migration (backlog T23.6 / data migration). The full adoption — canon everywhere,
  retiring `AssetKind`, phased rollout, and tooling — is decided in
  [`decisions/0001-instrument-kind-canon.md`](decisions/0001-instrument-kind-canon.md).
- **Columns are singular** (one row = one instrument's attribute): `option_right`,
  `instrument_kind`, `option_style`. **Enums are singular** too (`OptionRight` — one
  value out of a set, like `enum Color`), an intentional exception to R4.1's "plural for
  classes": a value-enum names a single value, whereas `OptionsColumns` names a
  *collection* of columns.
- Identity vs classification: *what* the contract is on = `asset_code` (R4.1.1); *how*
  it is classified = these axes. Don't encode an axis into `asset_code`.
- `instrument_kind`/`asset_class`/`contract_kind` are domain-neutral → `core`;
  `option_right`/`option_style`/`series_tenor` are options-domain → `alphavar.options`.

**Axis enums are plain `StrEnum`; storage compactness is the schema's `category` dtype,
not a hand-rolled code.** The legacy `EnumCode` (value `"option"` + short code `"o"`,
storing `"o"` in parquet) is retired: a `StrEnum` member *is* its readable value
(`df[Term.OPTION_RIGHT] == OptionRight.CALL` needs no `.code`/`.value`), and the
pandas/polars `category` dtype — declared in the pandera schema (R4.4) — gives the same
memory/filter win automatically while keeping raw data readable (`"call"`, not `"c"`).
So: human values in data, no manual codes, `category` dtype for the categorical columns.
Migration: existing parquet stores the old short codes (`"o"`, `"c"`) and must be
expanded to values (backlog data migration).

### R4.6 Reference data vs time series — normalize, don't denormalize per row

Quote/history DataFrames hold **only time-varying, per-row data**. Attributes that are
constant for a whole instrument (or asset, or currency) are **not** stored as a repeated
column on every row — they live in separate **reference entities**, loaded and passed as
objects, never broadcast into the frame. Rationale (measured on a real Deribit options
file): constant string columns `kind`/`symbol`/`option_type` alone were ~8.8 MB of ~25 MB
(~35%) — pure repetition.

**Identity model:** `asset_code` is the library's internal, human-readable asset identity
inside a dataset/exchange namespace. It is not a venue symbol and is not globally unique by
itself. Do not append the exchange to `asset_code` to force global uniqueness; the storage
path and resolved context carry `exchange_code`. Use `listing_id` for a globally unique
venue listing (`NYSE:T`, `MOEX:T`) and optional `economic_asset_id` for the cross-listing
economic object (`BTC`, `APPLE_INC`). Raw venue/provider encodings live in reference
metadata: `exchange_code`, `provider_code`, `exchange_asset_code`, `provider_asset_code`,
`listing_id`, `economic_asset_id`, `contract_id`, and `exch_symbol`.

A user-maintained canonical asset catalog may live at `{DATA_PATH}/_catalog/assets.yaml`.
It defines `economic_asset_id` values and display metadata only; it is not a dataset
registry and does not decide which dataset to load. Dataset-local `asset.yaml` files link
to it via `economic_asset_id`.

Composite listing notation (`{exchange_code}:{asset_code}`, e.g. `NYSE:T`) is a
user-facing shorthand accepted by resolver/API boundaries only. Providers, facades, and
`lib` functions operate on resolved normalized fields and paths. The resolver owns
economic-asset lookup: after resolving a concrete listing it may use `economic_asset_id`
to return all known datasets/listings for the same economic object.

**What goes where:**
- **Per-row (stays in the quotes frame):** anything that varies row to row —
  `price`, `iv`, `ask`, `bid`, `volume`, `open_interest`, `timestamp`, greeks,
  and other observations. These are never stored in reference.
- **Option contract reference** (one record/version per concrete option contract):
  `contract_id`, `listing_id`, `asset_code`, `exchange_code`, `provider_code`,
  `exch_symbol`, `expiration_date`, `strike`, `option_right`, `option_style`,
  `underlying_listing_id`, `valid_from`, `valid_to`.
- **Future contract reference** (one record/version per concrete future contract):
  `contract_id`, `listing_id`, `asset_code`, `exchange_code`, `provider_code`,
  `exch_symbol`, `expiration_date`, `underlying_listing_id`, `valid_from`, `valid_to`.
- **Contract/listing specs:** `contract_size`, `multiplier`, `tick_size`,
  `settlement_type`, `currency`, and their validity range. These may be keyed by
  `contract_id` or `listing_id`, depending on the venue.
- **Asset metadata:** canonical-ish asset attributes such as title, asset class, base
  currency, and quote currency. Venue/provider spellings are listing metadata, not
  asset metadata.
- **Class/currency-level reference** (shared across many instruments): interest `rates`
  per currency; `splits`/`dividends` per equity listing/asset. These are their own
  reference entities, not attached to one quote frame.

**Two design rules:**
1. **Temporal validity (slowly-changing dimension).** Reference data changes over time
   (`contract_size` revisions, listing/delisting, dividend schedule). Reference records
   are **snapshots with a validity range** (`valid_from`/`valid_to`), not a single
   current row. A load for a date selects the snapshot valid then.
2. **Stored as domain-specific reference, not repeated columns.** Reference data is
   persisted **separately** from quotes under the asset's `reference/` folder:
   `option_contracts.parquet`, `future_contracts.parquet`, `contract_specs.parquet`,
   plus human-authored YAML metadata (`dataset.yaml`, `asset.yaml`, `listings/*.yaml`).
   There is no catch-all `_meta.parquet` storage contract. On load, reference is read
   into typed entities/tables carried by `OptionsData`/the facade and joined only when a
   computation or compatibility surface explicitly needs a wide frame.

This keeps quote files small and makes "what is true about this instrument over time" a
first-class, queryable thing rather than redundant column noise.

## R5. Packaging

- Single distributable package: `alphavar`, rooted at `src/alphavar/` (uv + hatchling,
  `[tool.hatch.build.targets.wheel] packages = ["src/alphavar"]`). No second top-level
  package may live under `src/`.
- Every `import` used by shipped code must be a declared dependency (direct, not
  transitive). Subpackages shipped in the wheel that need optional dependencies
  (e.g. `options/etl` → `apscheduler`) must map to a pip **extra**, and their imports
  must fail with an actionable error message.

## R6. ETL isolation

- ETL (`alphavar/options/etl/`) consumes exchanges through the same
  `AbstractExchange` interface; it never talks to HTTP endpoints directly.
- ETL writes update snapshots and history under the dataset storage layout:
  `{DATA_PATH}/{dataset_code}/{exchange_code}/{asset_code}/{instrument_kind}/{timeframe}/{year}.parquet`.
  Each dataset root has a human-authored `dataset.yaml`; each asset folder may have
  `asset.yaml`, `listings/*.yaml`, and domain reference tables under `reference/`.
  Providers and ETL must agree on this layout — change it only in both places at once.
- Dataset selection is not a provider responsibility. A resolver scans dataset folders
  and YAML metadata, may maintain a generated cache under `{DATA_PATH}/.alphavar/`, and
  returns a concrete dataset path/context. File providers read only that resolved
  dataset path.
- Notifications go through `AbstractMessanger`; ETL code must not depend on a concrete
  messenger.

## R7. Security requirements

- **No secrets in the repository** — tokens/keys only via environment variables
  (`TG_BOT_TOKEN`, `TG_CHAT`, …). `.env` stays gitignored; a committed
  `.env.example` documents the variables without values.
- Secrets must never appear in logs, exception messages, or report texts (incl. URLs
  containing tokens).
- Any value interpolated into a filesystem path (`asset_code`, `asset_name`,
  `exchange_code`, timeframe) must be validated against an allowlist pattern before
  use — no path traversal through data-derived names.
- All outbound HTTP uses explicit timeouts; public-only API usage is the default. If
  authenticated endpoints are ever added, request signing lives in `RequestClass`
  behind an explicit, tested code path — never per-exchange ad-hoc signing.
- Only HTTPS endpoints for exchange APIs.

## R8. DataFrame engine strategy: Polars as the target

- **Strategic direction: Polars is the target dataframe engine** (Rust core; enables
  interop with related Rust projects). pandas is the current engine; the codebase must
  not deepen its pandas lock-in.
- The term dictionary (`options/dictionary/`) is **engine-neutral**: it defines
  plain string names and engine-agnostic metadata. It must not import pandas/polars
  types as part of its public contract (engine-specific dtype mapping lives next to the
  schemas, not in the name registry).
- DataFrame schemas (pandera) are defined per engine behind a common naming registry:
  `pandera.pandas` today, `pandera.polars` on migration — column names and checks carry
  over unchanged.
- New code in `options/lib` should prefer constructs with direct polars equivalents
  (column-wise expressions, joins, group-by aggregations) and avoid hard-to-port idioms
  (row-wise `df.apply`, implicit index reliance, `inplace=True` mutation chains).
- New or touched `lib` code returns new frames/series instead of mutating caller-owned
  frames in place. Existing pandas-specific hotspots (`groupby.apply`, temporary helper
  columns, `inplace=True`, implicit index alignment) are remediation targets, not patterns
  to copy.
- Engine selection goes through the existing `DataEngine` enum
  (`io/provider/_provider_entities.py`); providers receive the engine explicitly.

## R9. Product API tiers: one quant core, several entrypoint layers

`alphavar` is a quant product core, not only a notebook helper. The architecture must support
individual quant users and server-side products from the same domain implementation. There are four
entrypoint layers, with one-way dependency direction from outer adapters into the core:

```
adapters (API / worker / CLI / notebooks / agents)
  → services / use cases
  → domain facade (`Option` + components) and producer contracts
  → pure domain logic (`<domain>/lib`) + schemas/entities/dictionary
  → providers / exchanges / storage adapters
```

- **Core/domain API:** pure functions, typed entities, schemas, factories, result classes, and
  producer contracts. This is the most reusable surface and the target for tests, services, flow,
  and future engine/runtime changes.
- **Research API:** `Option` and its component facades (`data`, `chain`, `pricer`, `forecast`,
  `validation`, `analytic`, `chart`) over shared `OptionsData`. This is optimized for quant users,
  notebooks, scripts, and exploratory workflows.
- **Service API:** framework-neutral use-case functions/classes that assemble provider input,
  validation, domain computations, and serializable results. This is the preferred surface for
  server products.
- **Adapters:** FastAPI/HTTP handlers, workers, schedulers, CLIs, dashboards, notebooks, and AI
  agents. They may call services or explicit producer/facade steps, but they must not own domain
  formulas, DataFrame semantics, provider normalization, or schema compatibility.

**`flow` is an assembly mechanism, not a tier.** `alphavar.flow` plans and runs producer steps from
their self-describing contracts; it spans the core/research surfaces and is one of three
interchangeable assemblers (flow · a developer in code · an AI agent). Its consumers are researchers
and AI agents; deterministic backend services call the library directly and do not use it. It never
depends on services. Placement, the consumer→assembler map, and the `flow`↔`services` boundary:
[ADR 0005](decisions/0005-flow-services-etl-boundaries.md), [`api-tiers.md`](api-tiers.md).

`Option` remains the main ergonomic quant facade, but it is not the only product API. Server-side
code must not be forced to drive the product through a mutable research object when a stateless or
explicitly orchestrated service contract is the better fit.

Adding a capability therefore means deciding which surfaces it needs:

1. a pure `lib` function/model/result contract for the domain behavior;
2. an optional `Option` component binding for quant users;
3. an optional service/use-case wrapper for server or batch products;
4. optional adapters outside the domain core.

The concrete symbol → tier inventory (which modules/classes sit in which tier today, their import
path, and stability) lives in [`api-tiers.md`](api-tiers.md): R9 is the invariant, that map is its
realization.

## R10. Server-ready contracts: services are framework-neutral and explicit

Server-side products built on `alphavar` need stable contracts for requests, results, failures,
serialization, and dependency injection. Those contracts belong in the library/application boundary,
not in web handlers.

> **Target, not current work.** The server side (a `services` layer, adapters, a deployed product) is
> a **deferred future task — do not implement it now.** R10 is a **compatibility constraint on present
> work**: keep the quant core shaped so the server layer can be added later without rework (state
> confined to the research facade; pure logic stays pure; results gain pinned/serializable contracts;
> no env/secret reads or framework imports leak into the core). Build the quant/research path now;
> only *keep the door open* for the server path. See [ADR 0005](decisions/0005-flow-services-etl-boundaries.md).

- A service/use-case layer, when added, lives under a framework-neutral package such as
  `alphavar.services`. It must not import FastAPI, Celery, a database client, or deployment-specific
  configuration. Thin adapters may live outside the package or in explicitly adapter-named modules.
- Services receive providers, storage handles, cache handles, clocks, and runtime settings by
  dependency injection. They do not instantiate concrete exchanges implicitly, read secrets from the
  environment, or decide deployment policy.
- Service inputs are typed request models or explicit parameters. Request scoping still uses
  `RequestParameters` and domain-specific typed fields; raw venue symbols and wire-format values stay
  behind the provider/exchange boundary (R2).
- Service outputs are typed result objects and/or schema-pinned DataFrames with documented
  JSON/table representations. Serialized table columns use `Term`/`OptionsTerm`/`ResultTerm` names;
  they do not expose pandas-specific indexes or engine-specific details as part of the public
  contract.
- Validation and failure modes are explicit. A service may return a `ValidationReport`, raise a
  domain exception, or expose a typed error result, but adapters must not infer domain errors by
  inspecting arbitrary strings or partial DataFrames.
- Long-running work (ETL, forecast batches, surface calibration, portfolio/risk jobs) is modeled as a
  service call with explicit inputs and outputs; queues/schedulers are adapters around that service,
  not the owner of the computation. ETL keeps its R6 internals — it is a write-side job-shaped service
  with the scheduler as its adapter ([ADR 0005](decisions/0005-flow-services-etl-boundaries.md)).

### R10.1 Server endpoint contract: user intent in, deterministic use-case out

Server products, including a FastAPI application, expose **product actions** as endpoints. A request
describes the user's intent and parameters; it does not describe alphavar's internal execution graph.

- A server request may carry `asset_code`, strategy legs/specs, timeframe, `as_of`, horizon, model
  selectors, pricing/forecast/risk parameters, and output options. It must not require raw venue
  symbols, provider classes, storage paths, DataFrame column names, `flow` plans, or producer chains.
- A backend service is **deterministic by workflow**: each endpoint maps to a fixed, named use-case
  whose domain steps are coded explicitly. Request values select parameters and policy choices, not
  the cross-cutting computation graph. If data, `as_of`, or configured policy changes, the result may
  change; that context must be explicit in the response.
- Any implicit resolution is part of the contract. For example, `expiration="nearest"` or
  `moneyness="atm"` must resolve through documented policy (minimum days, strike selection rule,
  price source) and the response must include the resolved legs/inputs, not only the final numbers.
- FastAPI/HTTP handlers are adapters. They parse and validate transport input, enforce auth/rate
  limits, map domain errors to HTTP status codes, and call a service. They do not own option formulas,
  DataFrame semantics, provider normalization, strategy resolution rules, or serialization schemas.
- The composition root resolves deployment policy from configuration (`asset_code` to provider,
  default exchange/source, storage/cache, clock, feature flags, strategy-resolution policy) and
  injects those dependencies into the service. The service never reads secrets or environment
  variables and never constructs a concrete exchange/provider as hidden policy.
- Server responses include a typed result plus enough context for reproducibility and audit:
  `asset_code`, provider/exchange/source identity, `as_of`/data timestamp or snapshot id, request
  parameters, resolved strategy/legs when applicable, model/policy identifiers, warnings, and typed
  validation/error information.

This keeps the quant core reusable across notebooks, scripts, servers, and agents while preserving
R1/R2/R3: pure logic stays pure, provider/exchange normalization stays at the I/O boundary, and
facades/services orchestrate without hiding upstream domain computation.

---

> Quality gates and the mandatory owner-verification rule (formerly R9/R10) now live in
> the development document — see [DEVELOPMENT_REQUIREMENTS.md](../../_forge/DEVELOPMENT_REQUIREMENTS.md)
> **D1** (quality gates) and **D2** (owner verification of math / DataFrame / architecture).
> Architectural changes to R0…R10 are themselves subject to D2 (explain + owner approval).
