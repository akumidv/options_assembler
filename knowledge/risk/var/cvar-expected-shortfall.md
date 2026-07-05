# CVaR / Expected Shortfall (ES)

↑ Index: [risk/var/](README.md)

> Source: Rockafellar & Uryasev, *Optimization of Conditional Value-at-Risk* (2000);
> Hull (Expected Shortfall).

- **Definition:** CVaR₍α₎ = expected loss **given** the loss exceeds VaR₍α₎ (the average
  of the tail beyond VaR). Also called Expected Shortfall / Conditional VaR.
- **Why prefer it:** **coherent** risk measure (sub-additive — diversification never
  increases it), unlike VaR; captures tail severity, not just the threshold.
- **Computation:** from the same simulated/empirical P&L distribution as VaR — mean of the
  worst `α` fraction of outcomes.

> **Status: planned — not yet implemented (risk/portfolio domain,
> [T31–T33](../../../_aitna/TASKS.md)).** No USAGE skill until the code lands; owner-verify
> (D2) before code relies on it.
