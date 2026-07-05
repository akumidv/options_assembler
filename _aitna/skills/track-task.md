# Skill: track-task (durable, resumable task records)

**Knowledge skill** (no dedicated tool). The concrete procedure + record template behind
[**D6**](../DEVELOPMENT_REQUIREMENTS.md). Goal: any non-trivial task leaves a
record that survives interruption, so a *different* session / machine / agent can resume it
and judge its progress **independently** — from the record alone, not by re-reading the diff.

## When

Any task worth more than a single obvious edit: a feature, a refactor, a multi-step fix, a
migration. Skip it only for trivial one-liners. If unsure, write the record — it is cheap.

## Where

One home: [`_aitna/TASKS.md`](../TASKS.md). Do **not** start a second tracker. Math /
DataFrame / architecture verification lives in the [D2 ledger](../D2_VERIFICATION.md)
(`4VERIFY`); commits stay under [D5](../DEVELOPMENT_REQUIREMENTS.md). This skill
only governs the *task record*; it points at those, never duplicates them.

## Procedure

1. **Describe + plan, save first.** Add an entry to `TASKS.md` *before* coding (template
   below). An interruption right after this step must still leave a resumable record.
2. **Do, keep it current.** Set status `in progress` and tick increments as you land them.
   The record must never lag the code — a reader sees done vs. pending at a glance.
3. **Verify against the saved acceptance check** (the one from step 1, not a convenient
   new one). Route any math/DataFrame/architecture item to D2 (`4VERIFY` + ledger row);
   it stays *pending owner verification*, never "Done" here.
4. **Mark done + compact.** Replace the verbose initial/intermediate narrative with a
   compact final entry: outcome + where code/tests live + pointers (D2 row, ADR, R#/D#).
   Push long history into git history of the file.

## Record template

```markdown
### T<id>. <short title>
**Status:** planned | in progress (increment N/M) | done | pending owner verification (D2)
**Goal:** <what "done" means, in one or two lines>
**Plan:**
- [ ] 1. <increment / step>
- [ ] 2. <increment / step>
**Acceptance check:** <exact command / test / observation that proves done>
       e.g. `uv run pytest -q tests/unit/<area>` green + `ruff check` clean
**Notes:** <only durable context: decisions, gotchas, D2 ledger row, ADR/R#/D# pointers>
```

On completion the entry collapses to a few compact lines (status `done` + outcome +
pointers); the checklist and scaffolding are removed, not left half-ticked.

## Done / verify (for this skill itself)

- The `TASKS.md` entry existed **before** the first code change.
- At any moment the entry alone tells a fresh reader what is done vs. pending.
- The completion was checked against the *recorded* acceptance check.
- The final entry is compact and carries pointers (D2 / tests / ADR), not narrative.
