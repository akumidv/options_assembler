# Greeks

↑ Index: [options/pricing/](README.md)

> Source: Hull, *Options, Futures, and Other Derivatives* (Greek letters chapter).
> **Status: planned — partly implemented ([T36](../../../_aitna/TASKS.md)).** No USAGE skill
> until the rest lands (akmon knowledge→impl→usage rule).

Sensitivities of option value V to inputs:
- **Delta** `∂V/∂S` — directional exposure (calls 0…1, puts −1…0). _planned (T36)._
- **Gamma** `∂²V/∂S²` — convexity / how delta moves; largest near ATM, near expiry. _planned (T36)._
- **Vega** `∂V/∂σ` — sensitivity to implied volatility. **Implemented:** `bs_vega` in
  [`src/alphavar/options/lib/pricer/black_scholes.py`](../../../src/alphavar/options/lib/pricer/black_scholes.py)
  (usage: [`skills/pricing-and-iv`](../../../skills/pricing-and-iv/SKILL.md)).
- **Theta** `∂V/∂t` — time decay (usually negative for long options). _planned (T36)._
- **Rho** `∂V/∂r` — sensitivity to the risk-free rate. _planned (T36)._

Portfolio risk aggregates Greeks across positions (see
[../../risk/](../../risk/README.md)). Add closed-form Black-76 Greeks with source when each
lands; **owner-verify (D2)**.
