# Copilot Instructions for options_assembler

This guide provides essential context and conventions for AI coding agents working in this repository. It is tailored to the actual structure and workflows of the project.

## Project Overview
- Python library for options trading analysis, analytics, and visualization.
- Modular architecture: analytics, data enrichment, option chains, charting, and ETL for multiple exchanges.
- Main entrypoint: `Option` class in `src/options_assembler/option_class.py`.

## Key Components
- `src/options_assembler/`: Main assembler logic (option class, analytics, chain, chart, enrichment, pricer, validation, forecast)
- `src/options_lib/`: Core library (entities, chain, normalization, enrichment, analytic, chart, dictionary)
- `src/exchange/`: Exchange-specific logic (binance, deribit, moex)
- `src/provider/`: Data provider abstractions (abstract, file, local)
- `src/options_etl/`: ETL pipelines for options data (moex, deribit)
- `demo/`: Jupyter notebooks and usage examples
- `tests/`: Unit tests (mirrors main src structure)

## Development Workflow
- **Install dependencies:** `poetry install --with etl,dev,test`
- **Run tests:** `pytest`
- **Lint:** `pylint src/`
- **Jupyter notebooks:** `jupyter notebook` (see `demo/`)
- **Docs:** `cd docs && npm install && npm run dev`

## Patterns & Conventions
- Provider pattern for data sources: implement new providers via `src/provider/_abstract_provider_class.py`.
- Option analytics and enrichment are modular; extend via `src/options_assembler/analytic/` and `enrichment/`.
- ETL scripts are in `src/options_etl/` and `demo/etl_example/` (with Docker support for some pipelines).
- Test config: `tests/` uses `pytest`, config in `pyproject.toml`, test env in `test.env`.
- Pylint config: 120-char lines, protected access allowed in tests, no docstrings required for private/test functions.

## Data & Demos
- Example data: see Google Drive link in README, or use `demo/` notebooks for data download instructions.
- Notebooks in `demo/` are designed for Google Colab.

## Integration Points
- Data providers (file, local, exchange APIs)
- ETL pipelines (MOEX, Deribit)
- Visualization via matplotlib

## References
- See `CLAUDE.md` and `README.md` for further details and up-to-date instructions.

---
If any conventions or workflows are unclear, please request clarification or examples from the maintainers.
