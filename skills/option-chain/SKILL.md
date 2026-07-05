---
name: option-chain
description: Build and query an option chain (strikes × expiries) with alphavar — select a chain slice, classify moneyness (ATM/ITM/OTM), find ATM strikes, and add intrinsic/time value. Use when a user asks to work with an options chain, pick strikes around the money, or split premium into intrinsic vs time value.
when_to_use: Use when a user asks to work with an options chain, pick strikes around the money, or split premium into intrinsic vs time value.
owner: alphavar
---

# Work with an option chain in alphavar

USAGE skill (concept → function → how to apply). Verified against `src/` — re-confirm the
API before relying on it.

## Concept

An **option chain** is the grid of strikes × expirations for one underlying. Per strike you
classify **moneyness** (ATM / ITM / OTM), and you can split **premium = intrinsic + time
value** (call intrinsic `max(0, S−K)`, put `max(0, K−S)`; time value decays to 0 at expiry).

> Source: Hull, *Options, Futures, and Other Derivatives*. In alphavar, `price`/`iv` are the
> normalized model output; the venue's raw values are `exch_*` (R4 / dictionary v2).

## Implementing functions

`alphavar.options.chain_class.OptionsChain` (constructed from `OptionsData`):

- `select_chain(...)` — pick a chain slice (strikes / expiry).
- `add_atm_itm_otm()` → `Self` — classify each row's moneyness.
- `get_atm_strike()` → `float`, `get_atm_nearest_strikes()` → `list` — ATM selection.
- `get_max_settlement_valid_expired_date()`, `get_settlement_longest_period_expired_date(...)`
  — expiry helpers.

Intrinsic / time value (DataFrame enrichment):
`alphavar.options.lib.enrichment.price.add_intrinsic_and_time_value(df_hist)`.
Moneyness primitives: `alphavar.options.lib.chain.price_status`.

## How to apply

```python
from alphavar.options.chain_class import OptionsChain

chain = OptionsChain(options_data)        # options_data: OptionsData
chain.add_atm_itm_otm()                   # adds moneyness classification
atm = chain.get_atm_strike()              # ATM strike
near = chain.get_atm_nearest_strikes()    # strikes bracketing ATM
df = chain.df_chain                       # the working DataFrame
```

```python
from alphavar.options.lib.enrichment.price import add_intrinsic_and_time_value
df = add_intrinsic_and_time_value(df_hist)   # adds intrinsic + time-value columns
```

## Failure modes

- A chain must validate first (`validate_chain`) — malformed columns raise.
- ATM helpers need a populated underlying/forward; an empty or single-strike slice yields
  no meaningful ATM.
- Intrinsic/time value uses the normalized `price` column, not `exch_*` — feeding raw
  venue prices conflates conventions.
