---
name: data-sources
description: Fetch options/futures market data from supported exchanges (Deribit, Binance, MOEX) with alphavar — snapshot the options book, resolve instrument kinds, and understand each venue's symbol scheme and field→column mapping. Use when a user asks to pull market data from a venue, normalize exchange fields, or parse an instrument symbol.
when_to_use: Use when a user asks to pull market data from a venue, normalize exchange fields, or parse an instrument symbol.
owner: alphavar
---

# Fetch & normalize exchange data in alphavar

USAGE skill (concept → function → how to apply). Verified against `src/` — venue APIs drift,
so re-confirm both the live docs and the in-repo class before relying on specifics.

## Concept

alphavar reads market data from several venues through one abstraction, then **normalizes**
venue fields to its own columns: the venue's raw values are `exch_*`, the normalized model
values are `price`/`iv` (R4 / dictionary v2). Each venue has its own host(s), symbol scheme,
and field names.

## Implementing classes

All extend `alphavar.io.exchange._abstract_exchange.AbstractExchange`, which exposes the
shared facade:
- `get_options_assets_books_snapshot(asset_codes=None)` → `pd.DataFrame` — the options book.
- `resolve_instrument_kind(native_kind)` → `(InstrumentKind, ContractKind) | None`.
- `request_api(endpoint_path, signed=False, **kwargs)`.

| Venue | Class | Notes (verify in the class) |
|---|---|---|
| Deribit (crypto, European) | `alphavar.io.exchange.deribit.DeribitExchange` | API v2 REST, base `https://www.deribit.com`; symbol `{CCY}-{DDMMMYY}-{STRIKE}-{C\|P}`; `mark_price`→`exch_mark_price`, `creation_timestamp`→`exch_timestamp` |
| Binance (crypto, European opts) | `alphavar.io.exchange.binance.BinanceExchange` | per-product hosts (options host `https://eapi.binance.com`); symbol `{ASSET}-{YYMMDD}-{STRIKE}-{C\|P}`; primarily a spot/underlying source today |
| MOEX (FORTS opts & futures) | `alphavar.io.exchange.moex.MoexExchange` | ISS REST, base `https://iss.moex.com/iss`; `theorprice`→`exch_mark_price`, `updatetime`→`exch_timestamp`; groups by `base_asset_code` |

## How to apply

```python
from alphavar.io.exchange.deribit import DeribitExchange

ex = DeribitExchange(...)                              # see the class for construction
df = ex.get_options_assets_books_snapshot(["BTC"])     # normalized options book
```

(Binance / MOEX follow the same facade — swap the class.)

## Conventions & failure modes

- **Normalized vs raw:** use `price`/`iv` for model logic; `exch_*` are the venue's raw
  values (different conventions/units per venue).
- **Symbol parsing** differs per venue — a parse failure raises `InstrumentParseError`
  (`alphavar.io.exchange.exchange_exception`); don't assume one scheme across venues.
- **Volume / drift:** a snapshot can be thousands of instruments (vectorized parsing);
  endpoint params and rate limits change — verify against live docs, not memory.
- **Recording fixtures for tests** is a *dev* concern, not USAGE — see
  [`../../_aitna/skills/refresh-exchange-fixtures.md`](../../_aitna/skills/refresh-exchange-fixtures.md).
