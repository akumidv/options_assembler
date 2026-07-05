# Sortino ratio

↑ Index: [risk/ratios/](README.md)

> Source: [Investopedia: Sortino](https://www.investopedia.com/terms/s/sortinoratio.asp);
> Sortino & Price (1994).
> **Status: planned — not yet implemented ([T36](../../../_aitna/TASKS.md)).** No USAGE skill
> until the code lands (akmon knowledge→impl→usage rule).

- **Formula:** `Sortino = (Rp − T) / DD`, where `T` is the target/minimum acceptable
  return (often `Rf` or 0) and **DD** is the **downside deviation** — the standard
  deviation of returns **below** `T` only.
- **vs Sharpe:** Sharpe penalizes all volatility (up and down); Sortino penalizes only
  harmful (downside) volatility, so it rewards strategies with upside asymmetry — relevant
  for option strategies with skewed payoffs.
- **Downside deviation:** `DD = sqrt( mean( min(0, Rᵢ − T)² ) )` over the sample.

_Owner-verify (D2) the exact target/annualization choices when implemented._
