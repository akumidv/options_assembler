"""SCD Type 2 temporal versioning of reference records (R4.6, T25) — pure functions.

Reference records (asset- or contract-level) change slowly. Instead of overwriting, we keep a
history: each record carries ``valid_from`` (inclusive) and ``valid_to`` (exclusive; NaT =
still current). ``as_of`` selects the snapshot valid at a date; ``append_on_change`` folds a
new observation into the history, opening a new version only when an attribute actually
changed (and closing the prior one).

The SCD Type 2 interval semantics and append-on-change rules are owner-verified.
"""
import pandas as pd

from alphavar.options.dictionary import OptionsTerm

VALID_FROM = OptionsTerm.VALID_FROM
VALID_TO = OptionsTerm.VALID_TO


def as_of(history: pd.DataFrame, when: pd.Timestamp, key_cols: list[str]) -> pd.DataFrame:
    """The record per key valid at ``when``: ``valid_from`` <= when < ``valid_to`` (NaT open)."""
    still_open = history[VALID_TO].isna() | (when < history[VALID_TO])
    selected = history[(history[VALID_FROM] <= when) & still_open]
    return selected.drop_duplicates(subset=key_cols, keep="last").reset_index(drop=True)


def join_reference_asof(
    quotes: pd.DataFrame, history: pd.DataFrame, key_cols: list[str], time_col: str
) -> pd.DataFrame:
    """Attach each quote's contract reference as of its own timestamp (interval as-of join).

    For every quote row, pick the contract version whose validity interval covers the row's
    ``time_col`` (``valid_from`` <= t < ``valid_to``, NaT = open) and matching ``key_cols``, and
    add that version's reference columns. A left join: quotes whose key/time has no covering
    version keep their row with the reference columns left NaN. The reference columns it adds are
    only those absent from ``quotes`` (so it never overwrites a column the slim frame still
    carries — and is a no-op when the frame is already wide).
    """
    if history.empty:
        return quotes
    ref_cols = [
        c for c in history.columns if c not in {*key_cols, VALID_FROM, VALID_TO} and c not in quotes.columns
    ]
    if not ref_cols:
        return quotes

    q = quotes.reset_index(drop=True).copy()
    q["__row"] = range(len(q))
    versions = history[[*key_cols, *ref_cols, VALID_FROM, VALID_TO]]
    merged = q[["__row", time_col, *key_cols]].merge(versions, on=key_cols, how="left")
    t = merged[time_col]
    covers = (merged[VALID_FROM] <= t) & (merged[VALID_TO].isna() | (t < merged[VALID_TO]))
    picked = merged[covers].drop_duplicates("__row", keep="last")
    out = q.merge(picked[["__row", *ref_cols]], on="__row", how="left")
    return out.drop(columns="__row")


def _row_changed(snapshot: pd.DataFrame, merged: pd.DataFrame, attr_cols: list[str]) -> pd.Series:
    """NaN-safe row-wise 'any attribute differs from the open record' mask."""
    changed = pd.Series(False, index=merged.index)
    for col in attr_cols:
        new = merged[col]
        old = merged[f"{col}__open"]
        changed |= ~((new == old) | (new.isna() & old.isna()))
    return changed


def append_on_change(
    history: pd.DataFrame,
    snapshot: pd.DataFrame,
    when: pd.Timestamp,
    key_cols: list[str],
    attr_cols: list[str] | None = None,
) -> pd.DataFrame:
    """Fold ``snapshot`` (observed at ``when``) into the SCD-2 ``history``.

    - key not currently open  → append an open record (``valid_from``=when, ``valid_to``=NaT);
    - key open but an attribute changed → close the old record (``valid_to``=when) + append new;
    - key open and unchanged → no-op;
    - key open but absent from the snapshot → left open (reference is not auto-expired).
    """
    snap = snapshot.copy()
    if attr_cols is None:
        attr_cols = [c for c in snap.columns if c not in key_cols]

    if history.empty:
        snap[VALID_FROM] = when
        snap[VALID_TO] = pd.Series(pd.NaT, index=snap.index, dtype=snap[VALID_FROM].dtype)  # open
        return snap.reset_index(drop=True)

    is_open = history[VALID_TO].isna()
    open_rows = history[is_open]
    settled = history[~is_open]  # already-closed versions, never touched

    open_keyed = open_rows[key_cols + attr_cols].rename(columns={c: f"{c}__open" for c in attr_cols})
    open_keyed["__in_open"] = True
    merged = snap.merge(open_keyed, on=key_cols, how="left")
    merged["__in_open"] = merged["__in_open"].notna()  # True only where a key matched an open record

    is_new = ~merged["__in_open"].to_numpy()
    changed = (merged["__in_open"] & _row_changed(snap, merged, attr_cols)).to_numpy()

    new_versions = snap[is_new | changed].copy()
    new_versions[VALID_FROM] = when
    new_versions[VALID_TO] = pd.Series(pd.NaT, index=new_versions.index, dtype=new_versions[VALID_FROM].dtype)

    # close the open records whose key changed (a left-merge of the changed keys)
    changed_keys = snap.loc[changed, key_cols].drop_duplicates()
    close = open_rows.merge(changed_keys.assign(__close=True), on=key_cols, how="left")
    close.index = open_rows.index
    open_rows = open_rows.copy()
    open_rows.loc[close["__close"].notna().to_numpy(), VALID_TO] = when  # close changed keys

    return pd.concat([settled, open_rows, new_versions], ignore_index=True)
