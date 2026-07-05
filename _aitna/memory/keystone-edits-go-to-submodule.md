# Edits under _aitna/akmon/ go to the ai_akmon repo

[`../akmon/`](../akmon/) is a **git submodule** (repo `ai_akmon`). Any edit under
`_aitna/akmon/**` (README, roles, guardrails, profiles, pipelines, ARCHETYPES, ROADMAP)
**belongs to the `ai_akmon` repo**: commit + push there, then bump the submodule pin in
this repo (`git add _aitna/akmon`). This repo stores only the pinned commit.

So a session that changes both layers produces **two commits in two repos**:
- `_aitna/akmon/**` → the `ai_akmon` repo;
- everything else (`_aitna/agents/`, `_aitna/{skills,tools,memory}/`, root `skills/`,
  `knowledge/`, `_aitna/TASKS.md`, `AGENTS.md`, `agents/`, `src/`, …) → this (alphavar) repo.

This is the PROMOTE/PROPAGATE end of the akmon learn loop. Part of
[[akmon-ai-assist-model]]. Commits are the owner's to make ([[owner-owns-commits]]).
