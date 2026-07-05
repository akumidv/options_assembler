# Design — smile fit on sparse / live data via smile-shift (T30)

Pricer-side (R5). The implementation backlog carries a one-line entry linking here. Math is
**D2 owner-verified** (Type C).

## Problem

Intraday/live we often have only a handful of quotes for an expiration — too few to recalibrate
a full SVI smile (which needs ~5 points).

## Idea (owner)

Take the **last well-calibrated smile** (e.g. yesterday's EOD fit) and **translate / shift** it
to the new state instead of refitting from scratch: recompute log-moneyness `k = ln(K/F)`
against the **new underlying** F and the **new TTE** τ (re-anchor to today's forward and tenor),
then apply a small **shift** solved from the **few live points available** — a low-DoF
correction of the prior smile rather than a free fit.

## Decided (owner) — both, combined

- **Work in total-variance space.** The shift is **additive in total variance** `w = σ²·τ` (not
  in σ): natural for the no-arb checks and for re-anchoring to the new TTE (when τ changes,
  holding `w` and recomputing `σ = √(w/τ)` is the correct reparametrization). The re-anchor step
  recomputes `k = ln(K/F)` against the **new forward F** and reads the prior at the **new τ** in
  `w`-space first.
- **Adaptive degrees of freedom by live-point count:** 1 point → **parallel** `w`-level shift;
  2 → **level + slope** (skew); ≥3 → **level + slope + light curvature** — solved by least
  squares on the live residuals; never more DoF than points (a sparse slice can't over-fit).

## Plan (sketch)

`options/lib/pricer/smile/_shift.py` — `shift_smile(prior: SmileResult, live_points,
new_forward, new_tte) → SmileResult`:
1. re-anchor the prior into the new `(k, τ)` frame in `w`-space,
2. solve the additive-`w` shift (DoF = min(point-count rule, n_points)) by LS on the live
   residuals,
3. keep the butterfly no-arb check.

Facade hook on `OptionsPricer` (e.g. `fit_smile(..., fallback='shift', prior=...)`) so a sparse
slice degrades gracefully to a shifted prior.

## Open questions (owner — lock before build)

1. **Prior source** — where does the prior smile come from? (a) last EOD `fit_smile` result
   persisted to the reference sidecar (`_meta.parquet` / a new `_smile.parquet`, per T25); (b)
   refit on-the-fly from the last full slice in `df_hist`; (c) caller-supplied `prior=`. Default?
2. **Staleness guard** — max age of the prior before a shift is refused (raise / warn / fall
   back to a flat-vol guess)? E.g. reject if prior older than N sessions / underlying moved > X%.
3. **Minimum live points** — is 1 point enough to act on (parallel shift), or require ≥2? Guard
   when live points are all on one wing (e.g. only OTM puts) — cap DoF / widen prior weight?
4. **DoF→shift mapping confirm** — slope/curvature in `w`-space as a low-order polynomial in `k`
   (level = `c₀`, slope = `c₁·k`, curvature = `c₂·k²`)? Any cap on curvature to stay no-arb?
5. **Re-anchor when τ is unchanged** (same-day intraday) — skip the TTE recompute and shift only
   on the forward move, or always re-evaluate in the new frame?

## Relations

Composes with T27 (a shifted live smile can seed the forecast θ; pairs with the analogue model
it.6). Sibling of T29 surface fitting (the surface-level joint calibration).
