# 0006 — Dataset storage, identity resolution, and reference layout

- **Status:** Accepted (architecture; implementation pending)
- **Owner:** akuminov@gmail.com
- **References:** R2 (provider pattern), R2.1 (internal identity), R4.6 (reference data),
  R6 (ETL/storage), R7 (path safety), R10 (server-ready deterministic contracts) —
  `ARCHITECTURE_REQUIREMENTS.md`; D2 (owner verifies architecture). Backlog T45-T48.

## Context

The file provider currently treats `DATA_PATH/{exchange_code}` as the local data source and stores
reference data as `_asset.json` plus `_meta.parquet` under an asset folder. That is too coarse for
the product direction:

- one vendor can provide data for multiple exchanges;
- one exchange can be available through multiple providers;
- the same text symbol can exist on multiple venues (`T` on different exchanges);
- a canonical asset (`AAPL`) can have several listings/instruments (US stock, London ADR, vendor
  aliases) that must not be collapsed;
- option and future contracts need different reference tables;
- quote fields (`price`, `bid`, `ask`, `iv`, volume, greeks) must stay in time-series files, not in
  reference.

## Decision

### 1. Physical storage is dataset → exchange → asset

`DATA_PATH` contains datasets. A dataset is a local, named collection of data from one provider/feed
configuration. A dataset may contain several exchanges, so `exchange_code` must be a directory level
inside the dataset.

```text
DATA_PATH/
  deribit_direct/
    dataset.yaml
    DERIBIT/
      BTC/
        asset.yaml
        reference/
          option_contracts.parquet
          future_contracts.parquet
          contract_specs.parquet
        option/
          EOD/
            2025.parquet

  yahoo_markets/
    dataset.yaml
    MOEX/
      T/
        asset.yaml
        ...
    NYSE/
      T/
        asset.yaml
        ...
```

This prevents symbol collisions: `MOEX/T` and `NYSE/T` are distinct storage locations even when they
share the same `asset_code` text.

Names starting with `_` or `.` are **reserved** for service directories, not datasets: `_catalog/`
(the economic-asset catalog, §3.1) and `.alphavar/` (disposable resolver cache) live directly in
`DATA_PATH` alongside datasets, so a dataset must not be named with a leading `_`/`.`.

### 2. Identity is a tuple, not a synthetic key

`asset_code` is the library's canonical, human-readable asset identity **inside an `exchange_code`
namespace**. It is unique within an exchange, links every instrument kind on it (the same
`asset_code` carries the underlying's options, futures and spot), and is **preserved across
providers of the same exchange** (two datasets feeding NYSE both use `asset_code=T`). It is not the
raw venue symbol and is not globally unique; do not encode exchange/provider/kind into it — the
storage path and resolved context already carry those. It stays short and path-usable.

The `_code` / `_id` suffix carries meaning and is used consistently: `_code` (`asset_code`,
`exchange_code`, `provider_code`, `dataset_code`) is a **human-readable, scoped, non-unique** code;
`_id` (`economic_asset_id`) is an **opaque canonical/surrogate** identity. `asset_code` is a code by
design — renaming it to `asset_id` would falsely imply global uniqueness and blur it into
`economic_asset_id`.

**Key hierarchy.** Identity is expressed by tuples of normalized fields, not by minted surrogate keys:

- `dataset_code` — unique within `DATA_PATH` (one provider/feed configuration);
- `(dataset_code, exchange_code, asset_code)` — the concrete **storage/load key**;
- `(exchange_code, asset_code)` — the **listing** (an asset on a venue); its serialized form
  `exchange_code:asset_code` (e.g. `NYSE:T`) is **input sugar only** — see below;
- structural contract key — `(asset_code, expiration_date, strike, option_right)` for an option,
  `(asset_code, expiration_date)` for a future; `exch_symbol` is the venue's label for it;
- `economic_asset_id` — the cross-venue / cross-rename economic object (catalog only, §3.1).

**Identifiers deliberately not introduced.** No `listing_id` and no `contract_id` — each would only
serialize a tuple the system already carries, duplicating what the path and columns already state:

- a **listing** is `(exchange_code, asset_code)`; a `listing_id` field adds no information;
- a **contract** is its structural key; `exch_symbol` is a mutable venue label. Symbol renames are
  captured by the SCD reference (a symbol change closes one record and opens a new one under the
  same structural key), so no stable surrogate is needed. A corporate action that alters the strike
  itself is the one case the structural key cannot absorb — deferred to an optional
  `contract_aliases` remap table if ever needed, not built now.

**`exchange_code:asset_code` is resolver input sugar.** The composite `NYSE:T` is accepted only in
the resolver's `asset_code` argument: if the value contains `:` the resolver splits it into
`exchange_code` + `asset_code` (a value with `:` can never be a real `asset_code`, which is
path-safe). An explicit `exchange_code` that conflicts with the parsed one is an error. **The
resolver returns everything normalized** — providers, facades and `lib` functions receive only
`exchange_code` / `asset_code`, never a composite string.

The venue/provider encodings are normalized fields, each with a single home (never folded into
`asset_code`):

- `exchange_code` — venue namespace (`DERIBIT`, `MOEX`, `NYSE`); a directory level;
- `provider_code` — who supplied the data (`deribit`, `yahoo`, vendor name); in `dataset.yaml`;
- `exchange_asset_code` — the venue's own code for the asset, when it differs from `asset_code`; in
  `asset.yaml`;
- `provider_asset_code` — the provider's code for the same asset; in `asset.yaml`;
- `exch_symbol` — raw exchange symbol for a concrete tradable contract; in the reference tables/quotes;
- `economic_asset_id` — optional cross-venue/cross-rename economic identity; in the curated catalog
  (§3.1) only, never in dataset/asset metadata.

### 3. Metadata is local: ETL writes it into datasets; the catalog is curated

There is no central `datasets.json` source of truth. Each dataset carries its own metadata:

```yaml
# DATA_PATH/deribit_direct/dataset.yaml
dataset_code: deribit_direct
layout_version: v2                 # on-disk storage layout (this ADR); lets a future migration detect it
provider_code: deribit
provider_mode: direct_exchange
status: active
priority: 100
normalization_version: v1          # data normalization version — distinct from layout
```

Per-asset metadata is stored under the exchange/asset folder (no `economic_asset_id`/`title` — those
are curated in the catalog, §3.1):

```yaml
# DATA_PATH/deribit_direct/DERIBIT/BTC/asset.yaml
asset_code: BTC
asset_class: crypto
currency: USD          # quote currency — what the instrument is priced/paid in
```

Venue/provider encodings for the asset live in the same `asset.yaml` — there is one listing per
`(dataset, exchange, asset)`, so no separate listing file is needed:

```yaml
# DATA_PATH/deribit_direct/DERIBIT/BTC/asset.yaml (venue/provider fields)
exchange_asset_code: BTC        # venue's own code, when it differs from asset_code
provider_asset_code: BTC        # provider's code for the same asset
calendar_code: DERIBIT_CRYPTO_24_7
```

(`provider_code` is recorded once in `dataset.yaml`, not per asset.)

**Currency.** `currency` on the asset is the **quote currency** — what the instrument is priced/paid
in — with one fixed meaning (it is the single owner of that fact). Every other currency concept is a
domain extension introduced only where it is needed, not in the general schema: `settlement_currency`
for inverse crypto contracts (when it differs from `currency`), `base_currency`/`quote_currency` for
FX pairs (`asset_class=currency`), a notional/multiplier currency at spec level. A per-contract
currency override is likewise a domain concern that appears only with a mixed-currency asset.

**Who writes it.** ETL writes data + reference + `asset.yaml` into its own fixed **update** directory
in the target layout but **without** a dataset level (ETL is dataset-agnostic — the dataset is the
top folder, assigned later). A separate **apply** utility ("накат") merges an update tree into a
named dataset under `DATA_PATH/{dataset}/...`; the dataset is specified at this step. If an incoming
update disagrees with an existing dataset on a metadata field, the apply utility **raises an error**
and the user reconciles it — it never silently overwrites. A human adds only extra curated fields;
the resolver tolerates missing metadata, inferring defaults from the folder structure.

The resolver scans dataset folders and YAML metadata on construction and holds the index **in memory
for the session** — correct by construction, no staleness. An on-disk cache is a deferred
optimization: if discovery is ever measurably slow, a disposable
`DATA_PATH/.alphavar/dataset_index.json` may be added, invalidated on a daily TTL or on an ETL apply.
Such a cache must never be edited by users.

### 3.1 Canonical economic assets are a separate resolver catalog

Cross-dataset/cross-exchange comparison needs a small human-authored asset master catalog. It is the
**single owner** of `economic_asset_id` and its membership: `economic_asset_id` is **not** stored in
datasets or `asset.yaml` — ETL and datasets never need it. The catalog maps each economic identity to
the concrete `(exchange_code, asset_code)` members that belong to it, so the linkage lives in one
curated place. `economic_asset_id` is resolver/catalog knowledge: providers, facades, and `lib`
functions should not require it for ordinary loading or analytics. It links an asset across venues
**and** across a ticker rename on the same exchange (e.g. `FB`→`META`): both `asset_code`s are members
of one economic identity, and the pre-rename history stays in its own `NASDAQ/FB/...` folder (stored
data is never rewritten). `title` is likewise best curated here rather than per dataset.

```text
DATA_PATH/
  _catalog/
    assets.yaml
```

Example:

```yaml
assets:
  - economic_asset_id: BTC
    display_code: BTC
    title: Bitcoin
    members:                                        # (exchange, asset) that are this object
      - {exchange_code: DERIBIT, asset_code: BTC}
      - {exchange_code: BINANCE, asset_code: BTC}

  - economic_asset_id: META_PLATFORMS
    display_code: META
    title: Meta Platforms Inc.
    members:
      - {exchange_code: NASDAQ, asset_code: META}
      - {exchange_code: NASDAQ, asset_code: FB}     # pre-rename, history preserved
```

Dataset-local `asset.yaml` files do **not** reference the catalog — they carry no `economic_asset_id`
and no `title`; the `members` list above is the only link, resolved catalog-side by
`(exchange_code, asset_code)`.

The resolver can use the catalog to answer cross-listing questions, but `(exchange_code, asset_code)`
remains the concrete load identity, and cross-listing/cross-rename stitching is **not part of the
plain load path** (a plain load resolves one asset). Typical resolver behavior:

- input `NYSE:T` or `asset_code=T, exchange_code=NYSE` resolves the concrete NYSE asset first;
- if that asset has an `economic_asset_id`, the resolver can return all known datasets/assets for
  the same economic object (other venues, or a pre-rename `asset_code` such as `FB` for `META`) in
  `related_matches` — only when the caller asks for the group explicitly;
- if no catalog link exists, the resolver returns only the concrete asset and reports that no
  cross-listing group is known.

### 4. Reference is domain-specific, not a single `_meta.parquet`

Reference tables live under `reference/` and are split by domain/use:

```text
reference/
  option_contracts.parquet
  future_contracts.parquet
  contract_specs.parquet
```

`option_contracts.parquet` contains option contract identity and slowly-changing reference. The
structural key `(asset_code, expiration_date, strike, option_right)` is the stable identity;
`exch_symbol` is the venue label (SCD-tracked, so a rename becomes a new record under the same key):

```text
asset_code
exchange_code           # optional — not populated by default
provider_code           # optional — not populated by default
exch_symbol
expiration_date
strike
option_right
option_style
underlying_asset_code
valid_from
valid_to
```

`exchange_code` and `provider_code` are **optional** columns here (and in `future_contracts`): the
path already implies them, so **no writer — neither ETL nor the apply utility — populates them**; they
are left empty. They exist only to be filled deliberately (by reconciliation/analysis tooling) for
cross-provider matching or to close data gaps/errors between providers.

`future_contracts.parquet` contains future contract identity and slowly-changing reference; the
structural key is `(asset_code, expiration_date)`:

```text
asset_code
exchange_code           # optional — not populated by default
provider_code           # optional — not populated by default
exch_symbol
expiration_date
underlying_asset_code
valid_from
valid_to
```

`contract_specs.parquet` contains specs that may apply to options, futures, or both, keyed by the
contract's `exch_symbol` (or `asset_code` for asset-wide specs):

```text
exch_symbol      # or asset_code for asset-wide specs
contract_size
multiplier
tick_size
settlement_type
valid_from
valid_to
```

(No `currency` column here — the asset's `currency` (quote) owns that fact; a per-contract or
settlement currency is a domain extension added only when a venue actually needs it.)

Quotes stay in `{instrument_kind}/{timeframe}/{year}.parquet`. `price`, `bid`, `ask`, `iv`,
`volume`, `open_interest`, greeks, and other observations do not belong in reference.

### 5. Resolution is a separate component

Providers read a concrete dataset path. They do not search across `DATA_PATH`.

A dataset resolver is responsible for:

1. discovering datasets under `DATA_PATH`;
2. indexing exchanges, assets, instrument kinds, and timeframes;
3. applying explicit user filters (`dataset_code`, `exchange_code`, `provider_code`,
   `instrument_kind`);
4. resolving ambiguity by status/priority policy;
5. returning a resolved context for provider construction and server responses.

Resolution input (`asset_code` carries the `exchange:asset` sugar; there is no separate
`symbol`/`listing_id` field):

```text
asset_code                    # "T" or sugared "NYSE:T"
instrument_kind optional
exchange_code optional        # must not conflict with a sugared asset_code
provider_code optional
dataset_code optional
economic_asset_id optional    # catalog query, resolver-only
timeframe optional
```

Resolution output is fully normalized (no composite fields):

```text
dataset_code
dataset_path
provider_code
provider_mode
exchange_code
asset_code
economic_asset_id optional
available_instrument_kinds
priority
related_matches optional      # other datasets/assets for the same economic_asset_id (renames, cross-venue)
```

If the user specifies `asset_code + exchange_code` and the default dataset does not contain it, the
resolver searches registered datasets for that exchange/asset. If multiple matches remain, it applies
priority/status policy or returns an explicit ambiguity error with choices.

Composite-symbol parsing is a resolver/input concern:

- `asset_code="NYSE:T"` parses to `exchange_code="NYSE"`, `asset_code="T"`;
- `asset_code="T"` is treated as a bare `asset_code` and may be ambiguous across exchanges;
- an explicit `exchange_code` that conflicts with a sugared `asset_code` is an error;
- providers receive resolved fields and paths, never an unresolved composite string.

Cross-listing and cross-rename lookup is also a resolver concern, but **not part of the plain load
path**: a plain load resolves one `(dataset, exchange, asset_code)`. A caller that wants the
`FB`→`META` history or a cross-venue group asks explicitly and reads `related_matches` (assets linked
through the same `economic_asset_id`). The provider factory consumes only the selected match.

### 6. Provider factory consumes resolver output

The target construction flow is:

```python
match = resolver.resolve(asset_code="BTC", exchange_code="DERIBIT", instrument_kind="option")
provider = ProviderFactory.from_dataset(match)
option = Option(provider, asset_code="BTC", params=params)
```

Server endpoints keep the user contract small (`asset_code` plus strategy/request parameters) and
return the resolved dataset/provider/exchange context for reproducibility.

## Consequences

- The current `_asset.json` + `_meta.parquet` layout is not the final storage contract.
- The `{dataset}` level is mandatory — there is no legacy bare-`{exchange}/{asset}` mode; the owner
  migrates existing data into `{dataset}/{exchange}/{asset}/...` (a one-time owner-run step).
- ETL stays dataset-agnostic: it writes the target layout (minus the dataset level) into its own
  update tree; an apply utility binds an update to a named dataset and **errors on any metadata
  conflict** rather than overwriting.
- Metadata is YAML; the resolver scans on init and holds the index in memory for the session — an
  on-disk JSON cache is a deferred optimization (daily TTL or invalidate-on-apply if added).
- `asset_code` remains the load identity within an exchange, but it is not globally unique and must
  not encode exchange, provider, instrument kind, or contract identity; keep the `_code` name (a
  scoped readable code), reserve `_id` for opaque canonical identities.
- No `listing_id` or `contract_id` is introduced: a listing is `(exchange_code, asset_code)`, a
  contract is its structural key with `exch_symbol` as the venue label — both are tuples the path
  and columns already carry. Symbol renames are absorbed by the SCD reference.
- `{exchange_code}:{asset_code}` is input sugar accepted in `asset_code`; the resolver returns only
  normalized fields.
- `economic_asset_id` (and `title`) are owned solely by the curated catalog (`_catalog/assets.yaml`),
  which holds the `(exchange_code, asset_code)` membership; they are not stored in datasets. The
  catalog powers cross-venue discovery and cross-rename (`FB`→`META`) stitching in the resolver and is
  not part of the ordinary provider read path.
