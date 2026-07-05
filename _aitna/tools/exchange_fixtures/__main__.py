"""Record exchange HTTP fixtures from live APIs into tests/unit/io/exchange/fixtures/.

This is the AI/dev tool (code, D4) for the hermetic-exchange-test fixtures. It hits the
live venue APIs (needs network; run rarely — when an endpoint's shape changes), reusing
`alphavar.io.exchange` rather than re-implementing requests. Responses are keyed by
path+query and stored with their HTTP status (so 4xx error paths are exercised too).

    uv run python -m _aitna.tools.exchange_fixtures            # all exchanges
    uv run python -m _aitna.tools.exchange_fixtures --only moex

After recording, shrink the fixtures (offline, idempotent) with the trimmer in
tests/utils, then run the (now hermetic) exchange suite:

    uv run python -m tests.utils.exchange_fixtures.trim
    uv run --extra etl pytest tests/unit/io/exchange -q

Full playbook (when/why/verify): _aitna/skills/refresh-exchange-fixtures.md.
"""
import argparse

from _aitna.tools.exchange_fixtures import deribit, moex

_EXCHANGES = {'deribit': deribit.run, 'moex': moex.run}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description='Record exchange API fixtures (needs network).')
    parser.add_argument('--only', choices=tuple(_EXCHANGES), help='Record just one exchange.')
    args = parser.parse_args(argv)
    for name, run in _EXCHANGES.items():
        if args.only in (None, name):
            run()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
