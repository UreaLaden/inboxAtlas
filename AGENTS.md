# AGENTS.md

This repository uses a structured multi-agent workflow for InboxAtlas.

## What this repository is

`inboxAtlas` is the source repository for the InboxAtlas project. It contains:
- application source code
- workflow and handoff files under `.ai/`
- role definitions under `.claude/commands/`

InboxAtlas is an inbox discovery and classification platform that analyzes large mailboxes,
extracts operational signal from noisy communication, and establishes a foundation for future
routing and workflow automation.

## Session initialization

At the start of every session:

1. Determine the requested role from the user input
2. Load the corresponding role file from `.claude/commands/`
3. Follow that role strictly
4. If no role is explicitly requested, default to **Orchestrator**

Full codebase reference:
- `.ai/references/repo-map.md`

## User shorthand

| What the user says | What it means |
|---|---|
| "review the handoff" / "review latest handoff" / "review current.md" | Read `.ai/handoffs/current.md` and evaluate the completed work |
| "evaluate the pr review" / "review the pr review" | Read `.ai/handoffs/coderabbit-eval.md` and route via Orchestrator |
| "run the prompt" / "execute the prompt" | Read `.ai/handoffs/prompt.md` and execute it using the role declared inside |
| "show findings" | Read `.ai/references/analysis.md` and summarize it |

## Workflow roles

When a role is selected, read the corresponding file completely and follow it as the source of truth.

| Role | File | Action |
|---|---|---|
| orchestrator | `.claude/commands/orchestrator.md` | Workflow governance, routing, handoffs |
| analyzer | `.claude/commands/analyzer.md` | Investigation, planning, structured analysis |
| implementor | `.claude/commands/implementor.md` | Execution, verification, handoff completion |
| verifier | `.claude/commands/verifier.md` | Validation of analyzer/implementor output and task close readiness |

## Role invocation

Treat any of the following as a role selection:

- `role: orchestrator`
- `role: analyzer`
- `role: implementor`
- `role: verifier`

or

- `run orchestrator`
- `run analyzer`
- `run implementor`
- `run verifier`

or

- `use orchestrator`
- `use analyzer`
- `use implementor`
- `use verifier`

Do **not** assume bare slash commands like `/analyzer` or `/implementor` exist.

If custom prompts are installed in the Codex prompts directory, they may be invoked separately using the prompt command format supported by the Codex environment. Those prompts are optional convenience wrappers and do not replace the role files.

## Execution rules

- Never mix roles in a single pass
- Always load the selected role file before proceeding
- Treat the selected role file as the source of truth for behavior
- Respect all `.ai/` workflow constraints
- Prefer repository artifacts over chat context
- If a task is unclear, route to Orchestrator first
- If a task references another prompt file, resolve that prompt first before acting

## Prompt execution

If the user says "run the prompt" or "execute the prompt":

1. Read `.ai/handoffs/prompt.md`
2. Treat that file as the authoritative task definition
3. Identify the target role declared in that prompt
4. Load that role file
5. Execute only within that role’s rules

Do not automatically run a full multi-role pipeline just because `prompt.md` was referenced.

## Pipeline note

A pipeline prompt may coordinate multiple roles, but role boundaries still apply.

Default flow for end-to-end task execution:
1. Analyzer
2. Implementor
3. Verifier

If the resolved prompt specifies only one role, execute only that role.

If the prompt is ambiguous or spans multiple unresolved concerns, route to Orchestrator first.

## Verifier-specific note

Verifier is a first-class workflow role.

Use Verifier when the user asks to:
- validate completed work
- confirm a fix is correct
- check whether a handoff is really done
- review Analyzer + Implementor output before closeout

Do not use Verifier for implementation or design decisions.

## Source-of-truth rule

For workflow state, prefer these artifacts in order:

1. `.ai/handoffs/current.md`
2. `.ai/handoffs/prompt.md`
3. `.ai/references/analysis.md`
4. `.ai/backlog/work.md`
5. `.ai/references/repo-map.md`

Do not rely on chat memory when repository artifacts provide the answer.