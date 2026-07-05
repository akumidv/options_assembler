# akmon AI-assist model — layers, axes, names

The repo's AI assistance follows the **akmon** standard (a personal, cross-project
baseline, destined to become a product / MCP server). Canonical text lives in
[`../akmon/README.md`](../akmon/README.md); this note is the durable summary.

**Names (decided 2026-06-20):**
- `_aitna/` — the project's **dev layer** (meta/non-prod; a real Python package — tools
  import as `_aitna.tools.*`). Replaced the old `agents/_dev/` (interim `_ai_dev/`).
- `_aitna/akmon/` — the **SHARED cross-project standard**, a git submodule.
- Submodule **repo = `ai_akmon`** (`github.com/akumidv/ai_akmon`), **mount path =
  `_aitna/akmon`** (repo name ≠ mount path, wired in `.gitmodules`).

**Three layers (the layer axis — a decision tree, not a grid):** DEVELOP → SHARED
(`_aitna/akmon/`) / LOCAL (`_aitna/{skills,tools,memory,agents}/`); USE → USAGE (root
`skills/`). There is no "shared usage" cell.

**Three orthogonal axes:** Layer (SHARED/LOCAL/USAGE) · Role (architect/engineer) · Project
type (package/service/mcp/…). See [[akmon-role-vs-agent]] and [[akmon-knowledge-layer]].

**This project:** archetype = `package`, language = python, profile = `quant`.

Editing under `_aitna/akmon/**` commits to the `ai_akmon` repo — see
[[akmon-edits-go-to-submodule]].
