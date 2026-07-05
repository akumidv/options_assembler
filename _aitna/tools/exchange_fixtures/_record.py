"""Recording engine for exchange HTTP fixtures (AI tool, D4).

Exchange-agnostic capture: patches a client's ``session.get`` to save each raw JSON body
keyed by request path (+sorted query). Per-exchange modules (``deribit.py``, ``moex.py``)
only declare *what to call*, reusing the real `alphavar.io.exchange` API — they never
re-implement HTTP requests (that logic already lives in the exchange classes).

Output: ``tests/unit/io/exchange/fixtures/<exchange>/`` (``index.json`` path→file, ``<n>.json``
bodies). Full responses are large — always run the trimmer (``tests/utils``) afterwards.
"""
from __future__ import annotations

import json
import os

# This file is at _aitna/tools/exchange_fixtures/_record.py -> repo root is three up.
FIXTURES_ROOT = os.path.normpath(
    os.path.join(os.path.dirname(__file__), '..', '..', '..', 'tests', 'unit', 'io', 'exchange', 'fixtures')
)


def request_key(url) -> str:
    """Stable fixture key from an httpx URL: path + sorted query (drop volatile params)."""
    params = sorted((k, v) for k, v in url.params.multi_items() if k not in ('timestamp',))
    query = '&'.join(f'{k}={v}' for k, v in params)
    return f'{url.path}?{query}' if query else url.path


def try_call(label, fn):
    """Run a recording call best-effort; log and continue if the venue rejects it."""
    try:
        return fn()
    except Exception as err:  # noqa: BLE001 - recorder is best-effort
        print(f'  ! skipped {label}: {type(err).__name__}: {str(err)[:80]}')
        return None


def record(name: str, drive) -> None:
    """Record live responses for one exchange.

    ``drive(make_spy)`` builds a client via ``make_spy(client)`` (patches ``session.get``
    to capture bodies) and issues the calls the tests exercise, reusing the real API.
    """
    out_dir = os.path.join(FIXTURES_ROOT, name)
    os.makedirs(out_dir, exist_ok=True)
    captured: dict[str, tuple[int, object]] = {}

    def make_spy(client):
        sess_get = client.session.get

        def spy_get(url, *a, **k):
            r = sess_get(url, *a, **k)
            try:
                captured[request_key(r.request.url)] = (r.status_code, r.json())
            except Exception:  # noqa: BLE001
                pass
            return r
        client.session.get = spy_get
        return client

    drive(make_spy)

    # index maps key -> {file, status}; the body file holds the raw JSON (so the trimmer
    # can shrink it). Non-2xx responses (e.g. 422 "no options") are kept — the client's
    # error handling must see the real status.
    index = {}
    for i, (key, (status, body)) in enumerate(sorted(captured.items())):
        fn = f'{i}.json'
        with open(os.path.join(out_dir, fn), 'w', encoding='utf-8') as f:
            json.dump(body, f, ensure_ascii=False, indent=1, default=str)
        index[key] = {'file': fn, 'status': status}
    with open(os.path.join(out_dir, 'index.json'), 'w', encoding='utf-8') as f:
        json.dump(index, f, ensure_ascii=False, indent=1)
    print(f'[{name}] recorded {len(index)} responses -> {out_dir}')
