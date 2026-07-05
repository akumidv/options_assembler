---
name: fit-volatility-smile
description: Fit an implied-volatility smile (SVI / SABR / quadratic) to option-chain IVs across log-moneyness using alphavar, and query the fitted IV plus a no-arbitrage (butterfly) check. Use when a user asks to fit/parametrize a vol smile or surface slice, interpolate/extrapolate IV across strikes, or check a smile for butterfly arbitrage.
when_to_use: Use when a user asks to fit/parametrize a vol smile or surface slice, interpolate/extrapolate IV across strikes, or check a smile for butterfly arbitrage.
owner: alphavar
---

# Fit a volatility smile with alphavar

USAGE skill (concept → function → how to apply). Verified against `src/` — re-confirm
the API before relying on it; the public surface drifts.

## Concept

A **volatility smile** is the implied volatility as a function of strike (here,
**log-moneyness** `k = ln(K / F)`) for a single expiry. A *parametrization* (SVI, SABR,
a quadratic) fits a smooth curve to observed IVs so you can interpolate/extrapolate IV at
any strike and test the slice for **butterfly (static) no-arbitrage**.

## Implementing function

`alphavar.options.lib.pricer.smile`:

- `make_smile_model(name)` → a `SmileModel` (`name` ∈ `SMILE_MODELS`; default
  `DEFAULT_SMILE_MODEL` = SVI). Or use the classes directly: `SVISmile`, `SABRSmile`,
  `QuadraticSmile`.
- `SmileModel.fit(k, iv, t_years, weights=None)` → a **`SmileResult`**.
- `SmileResult.iv(k)` — fitted IV at log-moneyness `k`; `.total_variance(k)`;
  `.is_butterfly_free(k_grid=None, tol=-1e-6)` — numeric no-arbitrage check.

The DataFrame-level driver (fit per expiry over a chain) lives in
`alphavar.options.lib.pricer._smile_enrich`.

## Inputs & conventions

- **`k`** — log-moneyness `ln(K/F)` (a numpy array), **not** raw strike.
- **`iv`** — implied vols at those `k` (decimals, e.g. `0.62`, not `62`).
- **`t_years`** — time to expiry in **years**.
- **`weights`** — optional per-point weights (e.g. by liquidity).

## Failure modes

- Too few points / degenerate slice → fit raises or returns a poor result; check
  `is_butterfly_free()` before trusting it.
- A fit can succeed numerically yet be **arbitrageable** — always run the butterfly check.
- `iv` in percent instead of decimals is the most common silent error.

## Example

```python
import numpy as np
from alphavar.options.lib.pricer.smile import make_smile_model

k  = np.array([-0.20, -0.10, 0.0, 0.10, 0.20])   # log-moneyness
iv = np.array([0.72, 0.65, 0.60, 0.63, 0.69])    # implied vols (decimals)

model = make_smile_model("svi")                  # or SVISmile(); default is SVI
res = model.fit(k, iv, t_years=0.25)

res.iv(np.array([-0.05, 0.05]))                  # interpolated IV at new strikes
res.is_butterfly_free()                          # True if static no-arbitrage holds
```

For a whole chain (fit per expiry over a DataFrame), use the `_smile_enrich` driver rather
than calling `fit` per slice by hand.
