---
name: pricing-and-iv
description: Price options and solve implied volatility with alphavar's Black-76 (forward) model — fair price from a vol, implied vol from a market price, and vega. Use when a user asks to price an option, compute or back out implied volatility, add a model price/IV column to a chain, or get vega. (Other Greeks — delta/gamma/theta/rho — are not implemented yet.)
when_to_use: Use when a user asks to price an option, compute or back out implied volatility, add a model price/IV column to a chain, or get vega.
owner: alphavar
---

# Price options & solve implied vol (Black-76) in alphavar

USAGE skill (concept → function → how to apply). Verified against `src/` — re-confirm the
API before relying on it.

## Concept

alphavar prices **futures-settled** options with **Black-76** (the forward variant of
Black-Scholes): price is a function of *forward*, strike, time, vol, and rate. **Implied
volatility** is the σ that makes the model reproduce an observed market price, solved
numerically.

> Source: Hull (Black-76, implied volatility). `iv`/`price` are the normalized model
> output; the venue's quoted values are `exch_iv` / `exch_mark_iv` / `exch_mark_price` (R4).

## Implementing functions

**Pure functions** — `alphavar.options.lib.pricer.black_scholes`:
- `bs_forward_price(forward, strike, t_years, sigma, is_call, rate=0.0)` — Black-76 price.
- `bs_vega(forward, strike, t_years, sigma, rate=0.0)` — vega (∂price/∂σ).
- `implied_vol(price, forward, strike, t_years, is_call, rate=0.0, lo=1e-6, hi=5.0, iters=100)`
  — implied vol by **bisection**; returns `NaN` where the price is outside the
  no-arbitrage bracket (no σ reproduces it). Vectorized.

**DataFrame enrichment** — `alphavar.options.lib.pricer`:
`add_fair_price(...)`, `add_model_iv(...)`, `years_to_expiry(...)`.

**Facade** — `alphavar.options.pricer_class.OptionsPricer` (from `OptionsData`):
- `add_iv(market_col=EXCH_MARK_PRICE, rate=0.0)` → `Self` — add a model-IV column.
- `add_price(vol_col=IV, rate=0.0)` → `Self` — add a fair-price column.
- `get_iv(...)` → DataFrame; `fit_smile(...)` → smile fit (see
  [fit-volatility-smile](../fit-volatility-smile/SKILL.md)).

## How to apply

```python
import numpy as np
from alphavar.options.lib.pricer.black_scholes import bs_forward_price, bs_vega, implied_vol

price = bs_forward_price(forward=100.0, strike=105.0, t_years=0.25, sigma=0.6, is_call=True)
vega  = bs_vega(forward=100.0, strike=105.0, t_years=0.25, sigma=0.6)
iv    = implied_vol(price=4.2, forward=100.0, strike=105.0, t_years=0.25, is_call=True)
```

```python
from alphavar.options.pricer_class import OptionsPricer
OptionsPricer(options_data).add_iv().add_price()   # adds model iv + fair price columns
```

## Conventions & failure modes

- **Black-76 uses the forward**, not spot — pass `forward`, not the underlying spot.
- **Vol in decimals** (`0.6`, not `60`); **time in years**; rate is continuously compounded.
- `implied_vol` returns **NaN** for prices outside `[intrinsic, forward-bound]` — check for
  NaN rather than trusting a silent value.
- **Only `bs_vega` exists** among the Greeks. Delta/gamma/theta/rho are **not implemented**
  — do not claim them; they are a tracked gap (see [`skills/README.md`](../README.md)).
