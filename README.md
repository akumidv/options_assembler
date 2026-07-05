# alphavar

**alphavar** is a Python library for options (and futures) analysis and visualization.

The name reads as **alpha + VaR** — *alpha* (returns above the market) combined with
*Value-at-Risk* — reflecting the project's focus: turning raw options data into
risk-aware analytics. It gives you a single interface to fetch options/futures data from
different providers, enrich it with computed metrics, work with option chains, analyze
risk (payoff profiles) and time value, and visualize the results.

> **NB:** Early stage of development — the API may change.
>
> A parallel implementation in Rust is in progress (with a large delay):
> [alphavar-rust](https://github.com/akumidv/alphavar-rust).

## What it does

- **Data** — retrieve options/futures data from exchange APIs (Deribit, MOEX, …) or local files.
- **Enrichment** — add computed metrics: intrinsic/time value, ATM/ITM/OTM, Greeks.
- **Chains** — build and select option chains, assemble option "desks".
- **Analytics** — risk/payoff profiles for option combinations, time-value analysis.
- **Charts** — visualization via Plotly / matplotlib.
- **ETL** — accumulate historical snapshots of quotes.

The library follows a provider pattern: different data sources plug in through the
`AbstractProvider` interface. The main entry point is the `Option` class in
[src/alphavar/options/option_class.py](src/alphavar/options/option_class.py). For the full
public surface — which symbols sit in which tier (core / research facade / service / adapters)
and their stability — see [docs/dev/api-tiers.md](docs/dev/api-tiers.md).

## Ecosystem & roadmap

`alphavar` is the analysis core of a wider **alpha-extraction ecosystem** for financial
markets — options and derivatives today, with **equities** (fundamental analysis) and
**bonds**, plus broad market/macro context, on the roadmap. Alongside the library the
ecosystem includes the [`catcher-bot`](https://github.com/akumidv/catcher-bot) trading bot,
so together they cover both **analysis and trading**. As domains mature, general entities
(e.g. `options`) are expected to graduate into git submodules.

AI assistance follows the **akmon** standard ([`_aitna/akmon/`](_aitna/akmon/), a
cross-project submodule, destined for an **MCP** server). It separates **developing** the
project (the `_aitna/` dev layer — `architect` + `engineer` agents, bound by R#/D#) from
**using** it (the root [`skills/`](skills/) USAGE layer — how an assistant applies
alphavar's public API). The vendor-neutral entry point is [AGENTS.md](AGENTS.md); the model
is in [`_aitna/akmon/README.md`](_aitna/akmon/README.md).

## Quick start

Install all dependencies for development and testing with [uv](https://docs.astral.sh/uv/):

```bash
uv sync --all-extras
```

```python
from alphavar import Option
```

## Demo

The easiest way to see how to use alphavar is to open the Jupyter notebooks in the
`demo/` folder on Google Colab:

1. Open [Google Colab](https://colab.research.google.com/).
2. Click `File / Open notebook`.
3. In the popup, select **GitHub** and paste the repository URL:
   [alphavar](https://github.com/akumidv/alphavar.git).
4. Verify `alphavar` is selected in the Repository dropdown.
5. The list of notebooks appears below — pick one, e.g.
   [Options for Smart Investor](https://github.com/akumidv/alphavar/blob/main/demo/books_and_articles_reproducing/Options%20for%20Smart%20Investor.ipynb).

ETL examples for different exchanges (Deribit, MOEX) are in `demo/etl_example/`.

## Data

The easiest way to get sample data is to download it from the shared
[Google Drive folder](https://drive.google.com/drive/folders/1NJNxkkUYzCfADIlPHyaZQ0jrfW9WJn2I?usp=sharing).
Save the files into a data folder and point `DATA_PATH` (in `.env`) at it.

Target local data is organized by dataset, then exchange, then asset:

```text
DATA_PATH/
  deribit_direct/
    dataset.yaml
    DERIBIT/
      BTC/
        asset.yaml
        listings/
          DERIBIT.yaml
        reference/
          option_contracts.parquet
          future_contracts.parquet
          contract_specs.parquet
        option/
          EOD/
            2025.parquet
```

`dataset.yaml`, `asset.yaml`, and `listings/*.yaml` are human-readable metadata. Quote
history stays in `{instrument_kind}/{timeframe}/{year}.parquet`; contract/reference data
is stored separately under `reference/`.

User-facing symbols may use `{exchange}:{asset}` notation such as `DERIBIT:BTC` or
`NYSE:T`; the resolver expands that shorthand into dataset, provider, exchange, and asset
context before constructing a provider.

The `demo/` notebooks also include `gdrive` snippets showing how to download the data.

## Documentation

User-facing documentation is a Next.js (Markdoc) site in the `docs/` folder.
With Node.js installed:

```bash
cd ./docs
npm install
npm run dev
```

Then open the link printed in the console.

> The docs are still minimal. In the future they may be deployed via Vercel or Netlify.

Architecture, design decisions, and development notes live in
[docs/dev/PROJECT_OVERVIEW.md](docs/dev/PROJECT_OVERVIEW.md).

## For AI agents

The repo supports two usage models:

- **As a library** — import `alphavar` and drive the `Option` facade yourself (see *Quick
  start* and the demo notebooks); the tiered public surface is mapped in
  [docs/dev/api-tiers.md](docs/dev/api-tiers.md).
- **Through an assistant** — the vendor-neutral entry point is [AGENTS.md](AGENTS.md)
  ([CLAUDE.md](CLAUDE.md) points to it); the full model is in
  [`_aitna/akmon/README.md`](_aitna/akmon/README.md).

AI assistance follows the **akmon** standard, which separates two concerns:

- **Developing the project** → the [`_aitna/`](_aitna/) dev layer: the
  [`architect`](_aitna/agents/architect/README.md) (design/docs/ADRs) and
  [`engineer`](_aitna/agents/engineer/README.md) (code/tests) agents, bound by the R#/D#
  requirements. Shared, cross-project rules live in the
  [`akmon/`](_aitna/akmon/) submodule.
- **Using the project** → the root [`skills/`](skills/) USAGE layer: how an assistant
  applies alphavar's public API to a user's task, as a domain-concept → function map.

Knowledge promoted out of a project flows up into akmon (the learn loop), so the shared
standard improves through use.
