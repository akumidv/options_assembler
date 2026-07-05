# 0002 — Forecast as a model factory: target × process × engine

- **Status:** Accepted
- **Owner:** akuminov@gmail.com
- **References:** R3, R5 (`ARCHITECTURE_REQUIREMENTS.md`); D2 (`DEVELOPMENT_REQUIREMENTS.md`);
  backlog T27 (forecast); generalizes the smile factory `make_smile_model` (T21).

## Context

The `forecast` capability area (R3, the last planned facade component) must cover several
different things the owner asked for under one roof: forecasting **price** after a period,
**volatility**, a **smile**, or a whole **smile surface**; and within each, several *approaches*
the owner named — "probabilistic", "statistical", "simulational", and others (rates/factor-driven).

Those approaches are easy to conflate into a single "model type" knob, but they are not one axis:
GARCH is *statistical* (estimated from data) **and** can be solved *analytically* (variance term
structure) **or** by *simulation*; GBM is *probabilistic* (closed-form log-normal) **and** trivially
*simulated*; historical/bootstrap is model-free. "How the dynamics are specified/estimated" and "how
the distribution at the horizon is extracted" are independent.

## Decision

`forecast` is a **model factory along three orthogonal axes**, producing a **distribution** (not a
point), so it feeds VaR/CVaR directly (alpha + VaR):

1. **Target** (`ForecastTarget`) — *what* is forecast and its state vector: `price`, `vol`,
   `smile`, `surface`. smile/surface are **not** special-cased — their state is the calibrated
   parameter vector (reuse `make_smile_model`), so the same axes apply.
2. **Process / model** (`ForecastModel`, stateless `fit → FittedProcess`) — the dynamics + how
   parameters are estimated from history. This is where "statistical/econometric" lives
   (GARCH/EWMA/HAR/AR), alongside "probabilistic" parametric processes (GBM/random-walk) and
   model-free/empirical and (planned) exogenous/factor processes (rates, basis).
3. **Engine** (`ForecastEngine`) — *inference*: how a `FittedProcess` becomes a distribution.
   `analytic` (closed form) and `montecarlo` (simulation, the universal default); `bootstrap`
   planned. **The engine is a separate axis, not a model parameter** — most processes support more
   than one engine, declared by a `supports` capability set; invalid (model, engine) pairs raise.

Supporting decisions:
- **Output is distributional.** `ForecastResult` exposes `point()` / `quantiles(q)` /
  `scenarios(n)` and a **change** view (`ΔS = S_{t+h} − S₀`); point forecasts are a degenerate
  distribution, so every model flows through the same result type.
- **Horizon is calendar ACT/365** (`to_horizon_years`), the same convention as `SmileResult.t_years`
  / `years_to_expiry`. A `pd.Timestamp` horizon is an expiration date (auto time-to-horizon); a
  `float` is calendar days. Trading-time (≈252) scaling is a later per-model option.
- **Mechanics:** ABC + name→class registry + `make_forecast_model(target, name)` / `make_engine`,
  catalogued-but-unbuilt names raise `NotImplementedError` (planned), unknown names `ValueError`.
  Pure-numpy only (no scipy, R5); optimizers via the existing `minimize_nelder_mead`.

This **generalizes the smile factory** (`make_smile_model`: ABC `SmileModel` + registry + factory),
which is the established pattern for a capability area offering interchangeable algorithms. It is a
sibling of the `validation` approach (a non-mutating, two-stage `OptionsValidation` over the same
`OptionsData`) — both are capability components per R3, differing only in what they compute.

## Consequences

- A new model = a `ForecastModel` subclass + a registry entry; a new inference method = a
  `ForecastEngine`; neither forces caller changes (mirrors the R2 provider promise inside `lib`).
- Pure models/engines live in `options/lib/forecast` (R5, Polars-port surface, R8); the facade
  `OptionsForecast` only selects the input series and wires it in. No new stored columns or schema
  impact — forecasts are computed outputs, not persisted.
- The estimator math (GBM/GARCH/EWMA/HAR, lognormal quantiles, Acklam `norm_ppf`, ACT/365) is
  **owner-verified per D2** (Type C ledger rows) before any iteration is "done".

## Rollout (phased; each gates on `uv run pytest`)

1. **Iteration 1 — price** + the skeleton (`_base`/`_stats`/`_factory`/engines): `random_walk`,
   `gbm` (analytic + MC), `garch` (MC).
2. **Iteration 2 — vol:** `ewma`, `garch` (analytic term structure + MC realized vol), `har`,
   `realized`.
3. **Iteration 3 — smile (done, D2-pending):** forecast the SVI parameter vector θ (`param_rw` /
   `param_var` / `param_pca`) over `make_smile_model`, decode the terminal θ to a smile via a sibling
   factory (`make_smile_forecast_model`) + `SmileForecast` result. **The maturity convention is a
   sub-axis with two options, both recorded:** **A `fixed_expiration`** (model one expiration's θ and
   present it at the target tenor — built; mixes tenors across history, the acknowledged
   approximation) and **B `constant_maturity`** (interpolate to a fixed tenor *before* modelling — the
   dynamically-correct option). **B is deliberately deferred to iteration 4**: it needs the
   cross-expiration interpolation that the surface model builds, so the two land together and B becomes
   selectable alongside A. Until then `maturity='constant_maturity'` raises `NotImplementedError`.
4. **Iteration 4 — surface (done, D2-pending):** state = SVI θ stacked across constant-maturity
   tenor nodes (cross-expiration total-variance interpolation + flat-`w/τ` T-extrapolation); models
   `svi_surface` (RW) / `svi_surface_var` (VAR) / `pca_factor` (PCA) **reuse the smile θ-dynamics on
   the longer vector**; engines analytic/MC → `SurfaceForecast` (butterfly + calendar no-arb). This
   brought the cross-expiration interpolation that **unlocks smile maturity convention B**
   (`constant_maturity`), now built and selectable alongside A.
5. **Iteration 5 — price (endogenous/model-free done, D2-pending):** `ar1` (OLS AR(1) on log-price
   → mean-reverting **log-normal** terminal, analytic + MC), `empirical` (model-free: terminal =
   `S₀·exp(Σ resampled returns)`), the `bootstrap` **engine** (model-free moving-block resample via a
   `FittedProcess.bootstrap_terminal` hook), and the `front` rolled-series source (proportional
   back-adjustment). The **exogenous/factor** processes `factor_linear` / `var` remain deferred — they
   need an exogenous-factor *input contract* (factor series **and** a horizon factor scenario), which
   is the composable result-chain decided in **ADR 0003**; they still raise `NotImplementedError`.
6. **Iteration 6 — historical-analogue (planned, cross-target):** a `Process` that forecasts by
   *history repeating* — search the instrument's past for a window similar to the present (vol /
   smile-θ / surface-θ descriptor, k-NN) and project the matched windows' realized forward evolution;
   the engine is the empirical/`bootstrap` distribution over those matched forward paths. Same three
   axes (the descriptor is the target's state), so it slots in without new machinery.

Exogenous/factor processes (rates, basis) and the historical-analogue model are catalogued as
planned (T27), reachable without re-opening these axes; the factor processes additionally depend on
the composable result-chain (**ADR 0003**) for their exogenous inputs. Surface/smile **fitting**
(calibration) extensions — a joint surface fit and a sparse-data smile-shift — are tracked separately
on the pricer side (T29 / T30), not here.
