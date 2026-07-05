# A9c — options `lib/` inventory by shape (Shape 1 / Shape 2 / kernel)

> **A9 made concrete** ([TASKS](TASKS.md) A9c): classify every existing `options/lib/` function so the
> contract work (A4a schemas, Sh1 column-deps) has a target list, and the producer/kernel granularity
> (A8) is explicit. Test per the concept: *"one output per input row?"* → **Shape 1** enrichment
> (row-aligned `df+cols`/Series; compat = **column presence**). Else a new grain → **Shape 2** reduction
> (new tidy **kind**; compat = **schema**). A pure math/selection/object/IO helper that is **not** a
> chain node = **kernel** (A8: reused at the lib level, no contract).

## Classification

### Shape 1 — enrichment (row-aligned; compat = column presence)
| function | file | in → out |
|---|---|---|
| `add_intrinsic_and_time_value` | enrichment/price.py | df_hist → +intrinsic/time-value cols |
| `add_atm_itm_otm_by_chain` / `add_atm_itm_otm_exp` | enrichment/price.py | df_hist → +moneyness class col |
| `join_option_with_future` | enrichment/_option_with_future.py | df_hist(+df_fut) → +future col (row-aligned join) |
| `date.py` enrichers | enrichment/date.py | df → +date/expiration cols |
| `df_columns_to_timestamp` / `normalize_timestamp` | normalization/datetime_conversion.py | df → same df, cols dtype-normalized |
| `fill_option_price` / interim `source price` | normalization/price.py | df → +`exch_price`/`price` col |
| `years_to_expiry` | pricer/_enrich.py | (Series,Series) → Series |
| `add_model_iv` / `add_fair_price` | pricer/_enrich.py | df → +`iv`/fair-price col |
| `add_smile_iv` | pricer/_smile_enrich.py | df → +smooth-`iv` col |
| `get_chain_atm_itm_otm` | chain/price_status.py | df_chain → +moneyness col |
| `join_reference_asof` | reference/_scd.py | df_quotes → +reference cols (as-of join) |

### Shape 2 — reduction (new tidy kind; compat = schema)
| producer | file | new kind | interchange today |
|---|---|---|---|
| `select_chain` | chain/chain_selector.py | **chain** (one expiration slice) | tidy df ✓ |
| `convert_chain_to_desk` | chain/desk.py | **desk** (call/put pivot by strike) | tidy df ✓ |
| `chain_payoff` | analytic/risk/payoff.py | **payoff_curve** + **payoff_summary** (returns 2 frames) | tidy df ✓ |
| `time_value_series_*` (×3) | analytic/price/_time_values.py | **time_value_series** | tidy df ✓ |
| `SmileModel.fit` (svi/sabr/quadratic) | pricer/smile/* | **smile_fit** (`SmileResult`) | **object, no `to_frame`** ✗ |
| `fit_smile_slices` | pricer/_smile_enrich.py | **smile_fit[]** (per slice) | dict/list of objects ✗ |
| forecast `*.fit`→engine `.run` | lib/forecast/** | **forecast_dist** / **smile_forecast** / **surface_forecast** | `to_frame` ✓ (neutral `to_interchange` = V1) |
| `*_price_series` | forecast/_series.py | **price_series** | **(np.ndarray, idx) tuple** → tidy in V1 ✗ |
| `check_*` (input/model) | validation/* | **validation_report** (issues) | **list[ValidationIssue]**, not a frame ✗ |
| `split_reference` / `apply_reference` | reference/_split.py | **reference_split** (wide↔slim layers) | `ReferenceSplit` dataclass ✗ |
| `extract_reference` | reference/_migration.py | **asset_meta + contract_scd_history** | tuple(obj, df) ✗ |

### Edge — kind-preserving reductions (fewer rows, **same** schema/kind)
| function | file | note |
|---|---|---|
| `timeframe_resample` | normalization/timeframe_resample.py | resample grid: many rows → fewer, kind unchanged |
| `clean` | validation/clean.py | drops bad rows, kind unchanged |
| `as_of` | reference/_scd.py | point-in-time SCD slice, kind unchanged |
> The "one output per row?" test says Shape 2, but compat is still **column/kind presence**, not a *new*
> kind. Needs a ruling (see remaining-design **D-a**).

### Kernels (not chain nodes; reused at lib level — A8)
`black_scholes.{norm_cdf,bs_forward_price,bs_vega,bs_implied_vol}` · smile `_optimize` (nelder-mead) ·
forecast `_stats`/`_garch`/`_lognormal`/`engine/*` internals · `_calc_atm_distance` /
`_get_nearest_to_distance_strike` · chain scalar getters (`get_*_date`, `get_chain_atm_strike`,
`get_chain_atm_nearest_strikes`) · `parse_expiration_date` · `validate_path_segment` · `make_issue` ·
factories `make_smile_model` / `make_forecast_model` / `make_engine` · `reference/_store.{read,write}`
(**I/O**, not a pure transform — see **D-h**).

## Findings → feed the remaining component-design (non-flow)
1. **A9 maps onto the real lib cleanly** — three buckets are populated; the abstraction is not invented.
2. **Most Shape-2 producers have no tidy interchange yet** (`SmileResult`, validation issues,
   `ReferenceSplit`, `price_series` tuple, `chain_payoff` two-frames). Pinning each = **A4a generalized**
   (the catalog **D-b**). Only chain/desk/time-value/forecast already emit a tidy frame.
3. **Producer ≠ every function** (A8): the kernel column is large — the contract granularity is *coarser*
   than the function list. Need an explicit producer-vs-kernel rule (**D-d**).
4. **Kind-preserving reductions** (resample/clean/as_of) are a real third pattern (**D-a**).
5. **I/O lives inside `lib/reference/_store`** — under the unified graph that is a `load`/`store`
   producer (P-data), not a pure transform (**D-h**).
6. **Sh1 surface already half-exists** — `OPTION_COLUMN_DEPENDENCIES` is exactly the Shape-1 enrichers'
   column→required map; it needs params + produced-column (**D-c**).
