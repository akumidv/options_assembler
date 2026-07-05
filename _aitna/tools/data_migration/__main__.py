"""Data migration & health tool — verify and fix a stored exchange tree.

One general script for the whole "something is wrong with the stored data" loop: quickly
**verify** metadata / structure / types, then **fix** (column conversion + reference meta) and
re-verify. It reuses the shipped library code — diagnosis from the registry + reference, fixes
from the migration modules — so behavior is committed and tested, never a throwaway script.

Run (dry by default; the data path is an *exchange* folder, its sub-folders are asset codes):

    # 1) verify — read-only diagnosis, exits non-zero if any error-level issue
    uv run python -m _aitna.tools.data_migration verify /data/DERIBIT

    # 2) fix — column conversion (any *.parquet) + reference meta (per asset), then re-verify
    uv run python -m _aitna.tools.data_migration fix /data/DERIBIT            # DRY (shows plan)
    uv run python -m _aitna.tools.data_migration fix /data/DERIBIT --apply    # writes (.bak kept)

Checks (see ``_diagnose``): **structure** (legacy/unknown columns), **types** (datetime must be
tz-aware ms-resolution; price/iv/greeks numeric), **metadata** (reference sidecar present, matches
the folder, covers every contract key; a slim series with no sidecar is an error).

Playbook + the rule "encode every new legacy/edge case in the tool, never a throwaway script":
``_aitna/skills/migrate-stored-data.md``.
"""
from __future__ import annotations

import argparse
import sys

from _aitna.tools.data_migration._diagnose import Issue, diagnose_exchange
from alphavar.options.etl.reference_migration import migrate_exchange
from alphavar.options.migration import migrate_options_parquet_tree

_ORDER = {"error": 0, "warn": 1}


def _print_report(issues: list[Issue]) -> int:
    """Print issues grouped by category; return the number of error-level issues."""
    if not issues:
        print("✓ no issues (metadata / structure / types all clean)")
        return 0
    errors = sum(i.severity == "error" for i in issues)
    for category in ("metadata", "structure", "types"):
        rows = sorted((i for i in issues if i.category == category), key=lambda i: _ORDER[i.severity])
        if not rows:
            continue
        print(f"\n[{category}]")
        for i in rows:
            mark = "✗" if i.severity == "error" else "·"
            print(f"  {mark} {i.location}: {i.detail}")
    print(f"\n{errors} error(s), {len(issues) - errors} warning(s).")
    return errors


def _verify(exchange_dir: str) -> int:
    return 1 if _print_report(diagnose_exchange(exchange_dir)) else 0


def _fix(exchange_dir: str, *, apply: bool) -> int:
    print("== column conversion ==")
    n_cols = migrate_options_parquet_tree(exchange_dir, apply=apply)
    print(f"{'migrated' if apply else 'would migrate'} {n_cols} file(s).\n")
    print("== reference meta ==")
    n_meta = migrate_exchange(exchange_dir, apply=apply)
    print(f"{'wrote' if apply else 'would write'} reference for {n_meta} asset(s).")
    if apply:
        print("\n== re-verify ==")
        return _verify(exchange_dir)
    print("\nDry run — re-run with --apply to write (a .bak is kept by column conversion).")
    return 0


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="data_migration", description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="cmd", required=True)
    v = sub.add_parser("verify", help="read-only diagnosis (metadata / structure / types)")
    v.add_argument("exchange_dir")
    f = sub.add_parser("fix", help="column conversion + reference meta, then re-verify")
    f.add_argument("exchange_dir")
    f.add_argument("--apply", action="store_true", help="write changes (default: dry run)")
    args = parser.parse_args(argv)
    if args.cmd == "verify":
        return _verify(args.exchange_dir)
    return _fix(args.exchange_dir, apply=args.apply)


if __name__ == "__main__":
    sys.exit(_main())
