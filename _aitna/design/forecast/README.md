# Design — `forecast` capability area

Durable design for the `forecast` capability area (T27). The implementation backlog carries a
one-line entry linking here; this doc holds the architecture, the locked decisions, and the
model catalog (built + planned). Math is **D2 owner-verified** — see the
[D2 ledger](../../D2_VERIFICATION.md) rows referenced below.

Architecture is recorded in **[ADR 0002](../../../docs/dev/decisions/0002-forecast-model-factory-axes.md)**
(target × process × engine axes) + R3 model-factory pattern + PROJECT_OVERVIEW §12. Resolves
the T21 `forecast` placeholder.

## Goal

A `forecast` capability area (R3 facade over `OptionsData`, R5 pure lib) that produces a
**distribution** of a target at a future horizon — feeding VaR/CVaR. A factory of models along
three **orthogonal axes**, not one "approach" knob:

- **Target** (what) — the forecast state vector + its observable mapping.
- **Process / Model** — dynamics + how params are estimated from history.
- **Engine** (inference) — how the fitted process becomes a distribution.

## Design (owner-verified architecture — route math to D2)

- Horizon is **calendar ACT/365** (`h_years`, same convention as `SmileResult.t_years` /
  `years_to_expiry`). `Horizon = pd.Timedelta | float(days) | pd.Timestamp(expiration)`; a
  `Timestamp` auto-computes `h_years = (expiration − as_of)/365d`, `as_of` = last history ts.
  Trading-time (≈252) scaling is a later option.
- `ForecastResult` is **distributional**: `point()`, `quantiles(qs)`, `scenarios(n)` (empirical
  sample or an analytic `Distribution`); plus the change view `ΔS = S_{t+h} − S₀` (`.change()` /
  `change_quantiles(q)`) and `.to_frame(quantiles)`. Default engine = `montecarlo` (universal);
  `analytic` = closed-form fast path where it exists. `seed: int|None` for reproducibility.
- smile/surface are **not** special-cased: their state is the calibrated SVI/SABR **parameter
  vector** (reuse `make_smile_model`); the same Process × Engine axes apply.
- Pure-numpy only (no scipy — repo convention); optimizers via `minimize_nelder_mead`;
  inverse-normal CDF hand-rolled (Acklam) for analytic quantiles.

## Layout

`options/lib/forecast/` (`_base.py`, `_stats.py`, `_factory.py`, `engine/`,
`price/ vol/ smile/ surface/`) + facade `options/forecast_class.py` → `Option.forecast` (R3).
R3 (facade) + R5 (pure lib) — mirrors the `lib/pricer/smile/` factory.

## Engines (`engine/`)

- `analytic` — closed-form distribution → quantiles.
- `montecarlo` — simulate paths → empirical distribution (default).
- `bootstrap` — resample historical residuals/returns (model-free empirical; moving-block).

## Targets & models — catalog

Legend: **built** = implemented (D2-pending where noted) · **planned** = catalogued, not built.

### Target `price` (state = log-price)

Output view exposes both level `S_{t+h}` and change `ΔS = S_{t+h} − S₀`.

*Endogenous* (price from its own history) — **built** (D2 ledger "T27 forecast price models
(it.1)" + "(it.5)", Type C):
- `random_walk` — driftless log RW baseline; analytic (lognormal) + MC.
- `gbm` — drift + vol from log returns; analytic (lognormal) + MC. Log-drift ν = mean(r)/dt;
  σ²_ann = var(r)/dt; sdlog = σ√H, meanlog = lnS₀ + ν·H.
- `garch` — GARCH(1,1) Gaussian-MLE vol dynamics via `minimize_nelder_mead` (unconstrained
  reparam: ω = exp(·), persistence φ & α-share via sigmoids ⇒ ω>0, α,β≥0, α+β<1); **MC only**
  (terminal not lognormal — `analytic` raises); simulates `round(H/dt)` steps, <10 returns ⇒
  constant-variance fallback.
- `ar1` — OLS AR(1) on log-price → mean-reverting **log-normal** terminal: `meanlog =
  μ+φ^n(x_T−μ)`, `var = σ_ε²(1−φ^{2n})/(1−φ²)`, φ clipped to ±0.9999; analytic + MC.
- `empirical` — model-free: terminal = `S₀·exp(Σ of n resampled historical log-returns)`; no
  analytic form; pairs with the `bootstrap` engine (moving-block, block≈n^{1/3}).

*Exogenous / factor-conditional* (price driven by external factors — rates, futures-spot basis,
realized vol, macro) — **planned**, each raises `NotImplementedError` via the factory.
**Deferred behind the composable result-chain
([ADR 0003](../../../docs/dev/decisions/0003-composable-result-chain.md))**, which is their
input contract: they need the factor series **and** a horizon factor scenario (assumed, or
itself a forecast → composable). `rate` is a *pricing* input today (Black-76), distinct from a
forecast driver.
- `factor_linear` — regression of returns on exogenous factors (incl. rates).
- `var` — vector-autoregression over (price, rates, …) jointly.

**Price-series source** (facade `source=`, orthogonal to the model):
- `future` — a `df_fut` series selected by `expiration_date` (default = most-populated
  expiration ≈ front; fallback to `underlying_price`).
- `underlying` — per-timestamp `underlying_price` from `df_hist` (deduped to one value/ts).
- `front` — continuous front-contract series (nearest expiry ≥ now+roll_buffer; **proportional
  back-adjustment** anchored at the latest leg).

### Target `vol` — **built** (D2 ledger "T27 forecast vol models (it.2)", Type C)

Observable = annualized vol over the horizon; point models analytic-only, `garch` adds MC.
`spot` = trailing realized vol (change ref).
- `realized` — trailing annualized.
- `ewma` — RiskMetrics λ=0.94, flat term structure.
- `garch` — variance term structure analytic + realized-vol MC (shared `_garch.py`, reused by
  price + vol).
- `har` — HAR-RV, RV=r² proxy, d/w/m = 1/5/22.

### Target `smile` — **built** (D2 ledger "T27 forecast smile models (it.3)", Type C)

State = SVI θ=(a,b,ρ,m,σ) history per expiration (fit via `make_smile_model`); decode terminal
θ back to σ(k) with clamp (b≥0, |ρ|<1, σ>0) reusing SVI `_raw_w`; no-arb via
`is_butterfly_free`. Result type `SmileForecast` (expected_smile / iv_quantiles bands /
scenario_smiles / to_frame / is_butterfly_free). Own sibling factory
`make_smile_forecast_model` (parameter-vector state + distinct result ⇒ separate from the
scalar `make_forecast_model`, which redirects SMILE).
- `param_rw` (driftless multivariate RW on θ, **default**) · `param_var` (VAR(1), mean-
  reverting, OLS, driftless-RW fallback when under-identified) · `param_pca` (RW on top-k PCA
  modes of the θ increments). All reduce to a Gaussian terminal on θ (mean+cov).
- Engines `analytic` (expected smile from mean θ) / `montecarlo` (PSD-safe MVN θ draws → σ(k)
  quantile bands).
- **Maturity convention** (both built): **A `fixed_expiration`** — model one expiration's θ,
  present at target tenor τ = E−(as_of+H) (mixes tenors across history — flagged `4VERIFY`);
  **B `constant_maturity`** — interpolate to a fixed tenor before modelling (correct dynamics;
  single CM node at the target tenor), built with the surface work; `resolve_maturity` selects.

### Target `surface` — **built** (D2 ledger "T27 forecast surface models (it.4)", Type C)

State = SVI θ stacked across **constant-maturity tenor nodes** (default 1w/2w/1m/2m/3m): per
timestamp fit a smile per expiration, interpolate total variance across expirations to each node
(`interp_total_variance`: linear-in-w + flat-`w/τ` T-extrapolation), refit SVI per node →
stacked θ history. Dynamics **reuse the verified smile θ-models** on the longer vector.
- `svi_surface` (RW, default) · `svi_surface_var` (VAR(1)) · `pca_factor` (PCA).
- Engines `analytic`/`montecarlo` → `SurfaceForecast` (expected surface + scenario σ(k,τ) bands
  via `decode_surface`; butterfly per node + **calendar** no-arb).

### Target `analogue` / pattern-matching — **planned** (it.6, applies to every target)

Forecast by **searching history for a market situation similar to the present** (by vol level,
smile shape, or whole surface) and projecting its **subsequent realized evolution** over the
horizon — "history repeats". Fits the factory as a `Process` (`analogue`/`pattern_match`):
- state = a window descriptor (recent vol / smile-θ / surface-θ trajectory);
- fit = a distance/similarity search over the instrument's history (k-NN on the descriptor);
- engine = the empirical distribution of the matched windows' forward paths (`montecarlo`/
  `bootstrap` → scenarios; `analytic` → matched-mean path).
- Cross-target: a `price` analogue uses the return-window descriptor; `smile`/`surface` use the
  θ-trajectory descriptor + decode (reuse `decode_smile`/`decode_surface`).
- **Owner to confirm before build:** descriptor / distance metric / window length / # neighbours.

## Status

Iterations 1 (price) + 2 (vol) + 3 (smile) + 4 (surface, + smile maturity B) + 5 (price:
endogenous/model-free) are **code-complete, D2-pending** (per-iteration ledger rows above; each
has `4VERIFY` headers in the named modules). **Remaining:** factor-conditional price models
(deferred behind ADR 0003) and the historical-analogue model (it.6). The result-chain V1
(T37) reshaped the forecast *wiring* structurally — no math change.

## Verification

Every committed math/DataFrame/architecture change is **not "done" until owner-verified** (D2).
Add `4VERIFY` headers + a D2 ledger row before marking any model done.
