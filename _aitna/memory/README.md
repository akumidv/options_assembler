# Memory index

Durable, LLM-agnostic notes. One file per fact/decision. Promote stable items into
`AGENTS.md` / `ARCHITECTURE_REQUIREMENTS.md` and link back here.

- [owner-verifies-math-and-architecture.md](owner-verifies-math-and-architecture.md) —
  DataFrame/quant-math/architecture changes need explanation + explicit owner
  verification; never "done" until verified (codified as D2).
- [owner-owns-commits.md](owner-owns-commits.md) — the agent never makes landing commits on
  its own (backup branches ok; push/main/PR on request); codified as D5, enforced by the
  `git-commit-guard` PreToolUse hook.
- [env-and-test-running.md](env-and-test-running.md) — uv + Python 3.14, pyarrow<25 for
  cp314 wheels, tests need `--extra etl` + DATA_PATH, `.tmp/` outputs, `_aitna.*` imports.

### akmon AI-assist model (2026-06-20)

- [akmon-ai-assist-model.md](akmon-ai-assist-model.md) — the standard: three layers/axes,
  names (`_aitna` / `akmon` / repo `ai_akmon`); the model's root note.
- [akmon-role-vs-agent.md](akmon-role-vs-agent.md) — role = type (in akmon) vs agent
  = instance (in project); pipeline steps owned once, no duplication.
- [akmon-knowledge-layer.md](akmon-knowledge-layer.md) — knowledge→impl→usage chain;
  knowledge is OPTIONAL (else skill + docstring); root `knowledge/`.
- [operate-desk-out-of-akmon.md](operate-desk-out-of-akmon.md) — `agents/` = OPERATE
  (lifted from `agents/desk/`); OPERATE deferred from the model (ROADMAP O1).
- [akmon-edits-go-to-submodule.md](akmon-edits-go-to-submodule.md) — `_aitna/akmon/`
  is the `ai_akmon` submodule; edits commit there + bump pin, separate from this repo.
