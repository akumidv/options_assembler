# Owner owns commits (D5)

The build agent never makes landing/"final" commits on its own — leave work in the tree (or
a `backup/*`/`refactor/*` branch) and report; "done" ≠ "committed". push / tag / merge /
commit-on-`main` / PRs only on explicit per-time owner approval.

**Durability:** this is enforced mechanically by `.claude/hooks/git-commit-guard.py`
(PreToolUse), so it holds even after the session grows and the docs fall out of context — the
hook asks/denies at commit time and re-states the reason. Do not duplicate the rule text;
full rule is **D5** in `_aitna/DEVELOPMENT_REQUIREMENTS.md`. Sibling of
[[owner-verifies-math-and-architecture]] (D2).
