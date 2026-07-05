# Skill: add a new exchange data source

**Goal.** Add a new exchange (e.g. `Bybit`) as a provider, end-to-end, so the rest of the
library uses it unchanged. **Knowledge skill** — no single tool does this; it is an
ordered pipeline across layers, each governed by an architecture rule (R#). Follow the
order; verify each layer before the next.

**When.** A new venue must be queryable through the same `Option`/provider interface as
Deribit/MOEX.

**Preconditions.** Read the USAGE `skills/data-sources/SKILL.md` (venue→class map) for the venue's API; `uv sync
--all-extras`.

## Pipeline (layer by layer)

1. **Register the code.** Add the venue to `ExchangeCode` (`exchange/exchange_entities.py`)
   and to the `_EXCHANGES` map in `exchange/exchange_fabric.py`. Nothing else should need
   to name the venue (R2: providers are injected, callers stay venue-agnostic).

2. **Implement the exchange class** in `exchange/<venue>.py`, subclassing
   `AbstractExchange`. Implement the abstract contract (same names/params as the base —
   `asset_code`, not `symbol`): `get_assets_list`, `get_asset_history_years`,
   `load_options_history`/`_book`, `load_futures_history`/`_book`, `load_options_chain`,
   and `get_options_assets_books_snapshot`. HTTP goes through `RequestClass`; **reuse it,
   don't hand-roll requests**.

3. **Honour the boundary rules** (this is where venues bite):
   - **R2.1** — public methods take the project's internal identity (`asset_code` +
     typed scope). Build the venue's instrument symbol *inside* the class
     (`asset_code → exch_symbol`); never accept a venue symbol as a parameter.
   - **R2.2** — never send a project enum's `.value` on the wire. Add an explicit
     per-venue mapping (project enum → API string), like `_DERIBIT_API_KIND`. (This is
     the `options` vs `option` class of bug — verify the venue's exact spelling.)
   - **R4.x** — normalize the response into the canonical columns via `OptionsCol`/the
     registry: identity (`asset_code`, optional `exch_symbol`, R4.1.1), classification
     axes (`instrument_kind`, `option_right`, … as singular values, R4.5), price/IV
     (`price`/`iv` = ours; `exch_*` = raw venue, R4.2). Keep per-row vs reference split
     (R4.6) — don't broadcast constants.

4. **Tests, hermetic (D1).** Add `tests/unit/exchange/<venue>_test.py`. Record fixtures
   with the recorder tool and replay via the mock — see the
   [`refresh-exchange-fixtures`](refresh-exchange-fixtures.md) skill; add the venue's
   calls to `_aitna/tools/exchange_fixtures/<venue>.py`. Mark heavy multi-asset walks
   `@pytest.mark.integration`.

5. **D2 — owner verification.** The normalization (any DataFrame derivation / column
   semantics) and the API mapping are math/architecture: explain the logic, mark
   `# 4VERIFY (owner)`, and request approval. Passing tests are not enough.

## Done / verify checklist

- [ ] `get_exchange('<venue>')` returns the class; no caller hard-codes the venue.
- [ ] All abstract methods implemented; class instantiates (no `TypeError`).
- [ ] No public method takes `symbol`/`exch_symbol`; no raw enum value sent on the wire.
- [ ] Output uses registry column names + canonical axes; `pytest` schema validation green.
- [ ] Exchange suite hermetic (fast, no network) via recorded fixtures.
- [ ] `pytest` + `ruff check` green; normalization/mapping marked for owner verification.
