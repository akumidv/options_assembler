# Project Overview — alphavar

> A document for developing and extending the project, including with the help of AI
> assistants (Claude, GitHub Copilot, etc.). It describes the architecture, modules,
> extension points, and code conventions. It complements `AGENTS.md` and `README.md`.
>
> **Project status:** under active development (developing stage). The API may change.

---

## 1. Purpose

`alphavar` is a Python library for analyzing and visualizing options (and futures).
The name reads as **alpha + VaR** (alpha = returns above the market, VaR = Value-at-Risk).
It provides a single interface for:

- retrieving options/futures data from various sources (exchange APIs and local files);
- enriching data with computed metrics (intrinsic/time value, ATM/ITM/OTM, Greeks);
- working with option chains and building "desks";
- risk analytics (payoff of combinations) and time-value analysis;
- visualization (Plotly/matplotlib);
- ETL processes for accumulating historical snapshots of quotes.

**Language:** Python `>=3.14`. **Package manager:** [uv](https://docs.astral.sh/uv/) (build backend: hatchling).

### Ecosystem & AI agents

`alphavar` is one component of a wider goal: an **ecosystem for extracting alpha** from
financial markets — options/derivatives today, with equities (fundamental analysis) and
bonds, plus broad market/macro context, to follow. Alongside this library the ecosystem
includes [`catcher-bot`](https://github.com/akumidv/catcher-bot) for automated trading;
general entities and domains (e.g. `options`) are expected to graduate into git submodules.

The ecosystem is built and operated **through AI agents** (locally now, possibly
server-side later), split into two classes:

- **build agent** — develops this codebase (bound by R#/D#);
- **operate ("desk") agents** — use the library + bot on the market: investment/options
  analysis, strategy backtesting, fundamental analysis for equity/bond forecasts, and
  trading — coordinated by an orchestrator and bound by runtime guardrails (G#).

Agents share a domain knowledge base (destined for MCP) and improve the system via a learn
loop routed through the build agent. Full model:
[`../../agents/README.md`](../../agents/README.md); guardrails:
[`../../agents/GUARDRAILS.md`](../../agents/GUARDRAILS.md).

---

## 2. Repository map

```
alphavar/
├── src/
│   └── alphavar/
│       ├── core/            # domain-NEUTRAL base
│       │   ├── dictionary/          # plain-string term registry (Term) + classification axes
│       │   └── migration/           # parquet schema migration (dictionary v2)
│       │
│       ├── io/              # domain-NEUTRAL I/O infrastructure (R0)
│       │   ├── provider/            # AbstractProvider, RequestParameters, PandasLocalFileProvider
│       │   ├── exchange/            # AbstractExchange + Deribit/MOEX/Binance, cache, factories
│       │   └── messanger/           # Telegram / console notifications
│       │
│       └── options/         # DOMAIN: options + futures — by layer, then function (R0)
│           │                #   ── facade (stateful), flat at the domain root:
│           ├── option_class.py          # Option (main entry point)
│           ├── option_data_class.py     # OptionsData — data management
│           ├── enrichment_class.py      # OptionsEnrichment
│           ├── chain_class.py           # OptionsChain
│           ├── analytic_class.py        # OptionsAnalytic  (+ analytic_price_class, analytic_risk_class)
│           ├── chart_class.py           # ChartClass      (+ chart_price_class)
│           │                #   ── domain foundation (used by every layer):
│           ├── dictionary/              # term registry (v2) + legacy enums + classification axes
│           ├── entities/                # Pydantic entities (OptionsLeg, …)
│           ├── schemas/                 # pandera models (validation contracts)
│           │                #   ── pure computational logic (DataFrame in/out, no I/O):
│           ├── lib/
│           │   ├── analytic/            # payoff (risk) + time values (price)
│           │   ├── chain/               # chain selection, ATM/ITM/OTM, desk
│           │   ├── chart/               # chart data preparation
│           │   ├── enrichment/          # pure enrichment functions
│           │   └── normalization/       # price/date normalization, timeframe resampling
│           │                #   ── I/O orchestration:
│           └── etl/                     # ETL — accumulating quote snapshots
│
├── tests/                   # pytest, mirrors src/alphavar/ (see §9)
├── demo/                    # example notebooks (incl. for Google Colab)
├── docs/                    # documentation site (Next.js + Markdoc)
│   └── dev/                 # development docs (this file)
├── agents/                  # AI build/operate agents (knowledge, skills, tools)
├── pyproject.toml           # dependencies, pytest/ruff config
├── .env                     # environment variables for tests (gitignored)
├── AGENTS.md                # guidance for AI agents (CLAUDE.md links here)
└── README.md
```

---

## 3. Architecture: key layers

The three-layer separation (R1) holds **inside each domain** (here `options`); `io/` and
`core/` are domain-neutral and shared.

```
┌─────────────────────────────────────────────────────────────┐
│  options/*_class.py  — facade (stateful, public), flat        │
│    Option → {OptionsData, OptionsEnrichment, OptionsChain,       │
│              OptionsAnalytic, ChartClass}                      │
└───────────────┬───────────────────────────────────────────────┘
                │ uses pure logic
┌───────────────▼───────────────────────────────────────────────┐
│  options/lib/  — stateless functions (DataFrame in/out)        │
│    analytic, chain, chart, enrichment, normalization           │
│    (+ options/{dictionary,entities,schemas} — domain foundation)│
└───────────────┬───────────────────────────────────────────────┘
                │ obtains data through (injected provider)
┌───────────────▼───────────────────────────────────────────────┐
│  io/provider/  — AbstractProvider (data source interface)      │
│    ├─ PandasLocalFileProvider (Parquet files)                  │
│    └─ io/exchange/ — DeribitExchange, MoexExchange, Binance…   │
└────────────────────────────────────────────────────────────────┘
```

**Separation principle:**
- `options/*_class.py` — stateful facade classes (hold the DataFrame, provider, parameters).
- `options/lib/*` — pure functions/utilities that take and return a `pandas.DataFrame`;
  no I/O, no `io`/facade imports, no global state.
- `io/provider/` + `io/exchange/` — deliver raw data (domain-neutral infrastructure).

Keep this separation in mind when extending: **new computational logic goes into
`options/lib`, a new data source goes into `io/provider` / `io/exchange`, and a convenient
API method goes into a facade `options/*_class.py`.**

---

## 4. Main facade — the `Option` class

`src/alphavar/options/option_class.py`

```python
class Option:
    """Base option class to work with option data different ways."""
    def __init__(self, provider: AbstractProvider, option_symbol: str,
                 params: RequestParameters | None = None,
                 option_columns: list | None = None,
                 future_columns: list | None = None)
```

It aggregates five components (each receives the shared `OptionsData` via DI):

| Component | Class | File | Purpose |
|-----------|-------|------|---------|
| `data` | `OptionsData` | `options/option_data_class.py` | load/store `df_hist`, `df_fut`, `df_chain` |
| `enrichment` | `OptionsEnrichment` | `options/enrichment_class.py` | computed columns |
| `chain` | `OptionsChain` | `options/chain_class.py` | chains, ATM/ITM/OTM, desk |
| `analytic` | `OptionsAnalytic` | `options/analytic_class.py` | risk (payoff) + time value |
| `chart` | `ChartClass` | `options/chart_class.py` | visualization (Plotly) |

### OptionsData
- Properties: `option_symbol`, `period_from`, `period_to`, `timeframe`.
- DataFrames: `df_hist` (options), `df_fut` (futures), `df_chain` (chain).
- `update_option_chain()` — load the chain via the provider.

### OptionsEnrichment
- `enrich_options(columns, force)` — add columns with automatic dependency resolution
  (`OPTION_COLUMNS_DEPENDENCIES`).
- Supports: `UNDERLYING_PRICE`, `INTRINSIC_VALUE`, `TIMED_VALUE`, `PRICE_STATUS`.

### OptionsChain
- `select_chain()`, `add_atm_itm_otm()`, `get_atm_strike()`,
  `get_atm_nearest_strikes()`, `get_desk()`.

### OptionsAnalytic
- `.risk` → `OptionsAnalyticRisk` — payoff of combinations (`OptionsLeg` legs).
- `.price` → `OptionsAnalyticPrice` — time-value series
  (`time_value_series_by_strike_to_atm_distance()`, `time_value_series_by_atm_distance()`).

### ChartClass
- `.price` → `ChartPriceClass`; methods `show()`, `init(title)`,
  `time_values()`, `time_values_for_strike()`, `time_values_for_distance()`.

---

## 5. Data dictionary (`options/dictionary/`)

DataFrame columns are described by **enums** (not string literals) — use them when
extending the code instead of "magic strings". (A v2 plain-string registry — `core`'s
`Term` + `options/dictionary` `OptionsTerm` + pandera `schemas/` — runs in parallel and is
the migration target; see T23.)

| Enum | Values / purpose |
|------|------------------|
| `OptionsColumns` | 40+ columns: `TIMESTAMP`, `STRIKE`, `EXPIRATION_DATE`, `OPTION_TYPE`, `PRICE`, `ASK`, `BID`, `OPEN_INTEREST`, `VOLUME`, `UNDERLYING_PRICE`, `INTRINSIC_VALUE`, `TIMED_VALUE`, `PRICE_STATUS`, Greeks `DELTA/GAMMA/VEGA/THETA/RHO`, metadata `ASSET_CODE/ASSET_TYPE/CURRENCY/OPTION_STYLE` |
| `FuturesColumns` | futures columns |
| `SpotColumns` | spot columns |
| `OptionsType` | `CALL` ("call"/"c"), `PUT` ("put"/"p") |
| `OptionsPriceStatus` | `ATM`, `ITM`, `OTM` |
| `OptionsStyle` | `AMERICAN`, `EUROPEAN` |
| `AssetKind` | `OPTIONS`, `FUTURES`, `SPOT` |
| `AssetType` | `SHARE`, `COMMODITY`, `INDEX`, `CURRENCY`, `CRYPTO` |
| `Timeframe` | `EOD`, `1m`, `5m`, `15m`, `30m`, `1h`, `4h` (with `mult`/`offset`) |
| `LegType` | `OPTIONS_CALL`, `OPTIONS_PUT`, `FUTURES` |
| `Currency` | currency codes |

**Column dependencies during enrichment:**
```python
OPTION_COLUMNS_DEPENDENCIES = {
    INTRINSIC_VALUE: [UNDERLYING_PRICE],
    TIMED_VALUE:     [INTRINSIC_VALUE],
    PRICE_STATUS:    [UNDERLYING_PRICE],
}
```

**Key entity** (`options/entities/`):
```python
class OptionsLeg(BaseModel):
    strike: float
    lots: int
    type: LegType   # a leg of an options/futures combination
```

---

## 6. Data providers

### The `AbstractProvider` interface (`io/provider/_abstract_provider_class.py`)
```python
class AbstractProvider(ABC):
    exchange_code: str
    options_columns: list
    futures_columns: list

    def get_assets_list(asset_kind: AssetKind) -> list[str]: ...
    def get_asset_history_years(asset_code, asset_kind, timeframe) -> list[int]: ...
    def load_options_history(asset_code, params, columns) -> pd.DataFrame: ...
    def load_options_book(asset_code, settlement_datetime, timeframe) -> pd.DataFrame: ...
    def load_futures_history(asset_code, params, columns) -> pd.DataFrame: ...
    def load_futures_book(asset_code, settlement_datetime, timeframe) -> pd.DataFrame: ...
    def load_options_chain(asset_code, settlement_datetime, expiration_date) -> pd.DataFrame | None: ...
```

### `RequestParameters` (`io/provider/_provider_entities.py`)
```python
class RequestParameters(BaseModel):
    period_from: int | date | datetime | None = None
    period_to:   int | date | datetime | None = None
    timeframe:   Timeframe = Timeframe.EOD
```

### Implementations
- **`PandasLocalFileProvider`** (`io/provider/_local_provider.py`) — Parquet from disk.
  Target path structure:
  `{dataset_code}/{exchange_code}/{asset_code}/{instrument_kind}/{timeframe}/{year}.parquet`.
  Human-authored metadata lives beside the data (`dataset.yaml`, `asset.yaml`,
  `listings/*.yaml`); domain reference tables live under `reference/`. Dataset selection is
  performed by a resolver before provider construction.
- **`AbstractExchange`** (`io/exchange/_abstract_exchange.py`) — subclass of `AbstractProvider`
  with a `RequestClass` (httpx) and `request_api(endpoint_path, signed=False, **kwargs)`.
  - `DeribitExchange` (`io/exchange/deribit.py`) — crypto futures/options/spot (+ combo).
  - `MoexExchange` (`io/exchange/moex.py`) — MOEX ISS option-calc.
  - `BinanceExchange` (`io/exchange/binance.py`).
- Cache: `io/exchange/cache.py` — TTL cache (128 items, reset at midnight).
- Factories: `ExchangeFabric`, `ExchangeProviderFactory`.

---

## 7. ETL (`options/etl/`)

Accumulation of quote snapshots on a schedule (APScheduler).

- **`EtlOptions`** (`etl_class.py`) — base class.
  Parameters: `exchange`, `asset_names`, `timeframe`, `update_data_path`, `timeframe_cron`.
  Uses a `ThreadPoolExecutor` for parallel loading; background heartbeat/report/save tasks.
  Structures: `AssetBookData` (option/future/spot snapshot), `SaveTask`.
- **`EtlDeribit`** (`deribit_etl.py`) — extended `AssetBookData` (`future_combo`,
  `option_combo`), `get_symbols_books_snapshot()`, `_save_timeframe_book_update()`.
- **`EtlMoex`** (`moex_etl.py`).
- **`etl_updates_to_history.py`** — integrate accumulated updates into history.

**Data volume estimate (Deribit):** 1m ≈ 275 GB/year, 5m ≈ 60 GB/year, 1h ≈ 4.5 GB/year.

---

## 8. Dependencies (`pyproject.toml`)

**Core:** `pandas >=2.2.3`, `numpy >=2.1`, `pandera >=0.20`, `pydantic >=2.10.5`,
`pyarrow >=21`, `httpx >=0.28.1`, `plotly >=5.24`, `matplotlib >=3.9`,
`cachetools ==6.2.0`, `psutil >=6.0`, `python-dotenv >=1`.

**Extra / groups:**
- `etl` (optional-dependency extra): `apscheduler >=3.11` — `uv sync --extra etl` /
  `pip install 'alphavar[etl]'`
- `dev` group: `setuptools`, `jupyter`, `ruff`, `twine`
- `test` group: `pytest`, `pytest-asyncio`, `pytest-dotenv`

Install: `uv sync --all-extras`

---

## 9. Tests

- `tests/unit/` mirrors `src/alphavar/`: `core/`, `io/{exchange,provider,messanger}/`,
  `options/` (facade tests flat) + `options/{dictionary,entities,schemas,lib,etl}/`.
- pytest config (`pyproject.toml`): `pythonpath=["src"]`, `env_files=[".env"]`,
  `testpaths=["tests"]`.
- Main fixtures (`tests/conftest.py`): `data_path`, `exchange_provider`
  (`PandasLocalFileProvider`), `option_data`, `option_symbol` (default `'BTC'`),
  `exchange_code` (`'DERIBIT'`), `provider_params`, lists of update files.
  Data is cached via the `_CACHE` dictionary.
- Run: `pytest`. Lint: `ruff check src tests tools`.

---

## 10. Configuration / environment

Variables (`.env` and runtime):
- `DATA_PATH` — root of local datasets. A dataset is a provider/feed collection with
  `dataset.yaml`, exchange folders, asset folders, quote parquet files, and reference
  metadata/tables.
- `ETL_TIMEFRAME` — ETL timeframe (`5m`, `1h`, …).
- `TG_BOT_TOKEN`, `TG_CHAT` — Telegram notifications (optional).

Other: `.editorconfig` (max line 120), `sonar-project.properties` (SonarQube),
ruff (`[tool.ruff]`, line length 120; F/E/W/I/UP/B).

---

## 11. Extension points (how to extend)

| Task | Where to change | How |
|------|-----------------|-----|
| Add a new data source | `src/alphavar/io/exchange/` | a new subclass of `AbstractExchange`, implement the abstract methods; register it in the factories |
| Add a computed column | `options/lib/enrichment/` + `options/dictionary/_dataframe_columns.py` | a new function + an entry in `OptionsColumns` and, if needed, in `OPTION_COLUMNS_DEPENDENCIES`; wire it into `OptionsEnrichment` |
| New analytics | `options/lib/analytic/` + a facade `options/analytic_class.py` | a pure function over a DataFrame + a facade method |
| New chart type | `options/lib/chart/` + facade `options/chart_class.py` | data preparation + a render method |
| New ETL source | `options/etl/` | a subclass of `EtlOptions` |
| Notification channel | `io/messanger/` | a subclass of `AbstractMessanger` |

**Code conventions:**
- DataFrame columns — only via the `OptionsColumns`/`FuturesColumns` enums, not strings.
- Pydantic models for entities and request parameters.
- Pure logic (no I/O or state) lives in `options/lib`; state and the provider live in the
  facade `options/*_class.py`; I/O lives in `io/`.
- Absolute imports only — no relative (`from .`/`from ..`) imports.
- Lines ≤ 120 characters; docstrings are not required for private (`_`) and test (`test_`) functions.

---

## 12. Known caveats / TODO for future work

- The project is at an early development stage — the public API is unstable.
- `pricer` is present (`OptionsPricer`, `options/pricer_class.py` over the shared `OptionsData`):
  Black-76 forward pricing + IV and a volatility-smile fit (SVI / quadratic / SABR via
  `make_smile_model`, butterfly no-arb checked) — `fit_smile` makes `price`/`iv` a model output.
- `validation` is present (`OptionsValidation`, `options/validation_class.py`): semantic
  data-quality checks in **two stages** — `validate_input` (pre-analysis gate: completeness, no-arb
  price bounds, strike/timestamp sanity, duplicates) and `validate_model` (post-fit: positive model
  IV, smile butterfly + calendar no-arb, fit residual) — both returning a **non-mutating**
  `ValidationReport`; `clean` is the only opt-in remediation (defaults off). Distinct from the
  pandera schemas (structure/dtype) — see `options/lib/validation`.
- `forecast` is present (`OptionsForecast`, `options/forecast_class.py` over the shared
  `OptionsData`; T27): a model factory along three axes — target × process/model × engine —
  producing a distributional `ForecastResult` (point / quantiles / scenarios / change). Implemented:
  **price** (`random_walk` / `gbm` / `garch`) and **vol** (`ewma` / `garch` / `har` / `realized`),
  engines `analytic` / `montecarlo`, horizon calendar ACT/365. `smile` / `surface` targets remain
  planned. Pure models/engines live in `options/lib/forecast` (pure-numpy, no scipy).
- **`alphavar.flow`** (cross-domain result-chain / pipeline) is in **active co-design** — see
  [`_forge/design/result-chain/`](../../_forge/design/result-chain/README.md) (widens [ADR 0003](decisions/0003-composable-result-chain.md)).
  Direction: producers compose by a described **contract** (compatibility lives in schemas, not in any
  orchestrator); `lib` stays pure `df+params→df` (Shape 1 enrichment = `df+cols`/Series via column
  deps; Shape 2 reduction = a new-kind tidy frame); classes are a binding layer; `flow` (Registry in
  code, `Plan` as declarative data) orchestrates a branching DAG. Not built yet.
- `BinanceExchange` is a minimal implementation.
- The v1 dictionary enums and the v2 `Term`/`OptionsTerm` registry + pandera schemas run in
  parallel until the T23 migration completes.

---

## 13. Quick start for an AI assistant (Copilot, etc.)

1. The source of truth on architecture is this file + the code in `src/`. `AGENTS.md` is the guidance for AI agents (`CLAUDE.md` links to it).
2. Start reading from `src/alphavar/options/option_class.py` (the facade) and
   `src/alphavar/options/dictionary/` (the column and enum dictionary).
3. Before changing data logic, check `OPTION_COLUMNS_DEPENDENCIES`.
4. For a new exchange, copy the pattern in `src/alphavar/io/exchange/deribit.py`.
5. Tests are required: place them in `tests/unit/<area>/` mirroring `src/alphavar/`, and
   reuse existing fixtures.
6. Verify with `pytest` and `ruff check src tests tools` before committing.
