# Role ≠ agent (akmon Role axis)

In the akmon model the **Role** axis classifies dev work, and **role ≠ agent**.

- **Role** = the *definition* (pipeline + requirements + guardrails). Cross-project, in
  [`../akmon/roles/`](../akmon/roles/) (`architect.md`, `engineer.md`).
- **Agent** = a role *instanced in this project*:
  [`../agents/`](../agents/)`{architect,engineer}/` — inherits the akmon role and adds
  alphavar specifics. An agent is **not** an axis value; it is a point where axes meet
  (role × layer × project type). One role can have several agents.

Two roles: **architect** (design/docs/ADRs; pipeline `design-flow` — an iterative loop with
a living design concept + a rejected-branches register) and **engineer** (code/tests;
pipeline `code-flow`).

**No-duplication rule:** pipeline **steps** are owned by `akmon/pipelines/*` only. Role
files and agent charters **link** the pipeline; they do **not** restate the steps. Part of
[[akmon-ai-assist-model]].
