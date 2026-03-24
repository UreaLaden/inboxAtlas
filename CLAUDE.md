# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repository is

`inboxAtlas` is the **source repository** for the InboxAtlas project. It contains both the application source code and AI collaboration files (`.ai/`).

InboxAtlas is an **inbox discovery and classification platform that analyzes large mailboxes, extracts operational signal from noisy communication, and establishes a foundation for future routing and workflow automation.**

## Session initialization

At the start of every session, invoke the appropriate custom slash command based on
the task type. See the Custom slash commands table below.
Full codebase reference: `.ai/references/repo-map.md`

## User shorthand

| What the user says | What it means |
|---|---|
| "review the handoff" / "review latest handoff" / "review current.md" | Read `.ai/handoffs/current.md` and evaluate the completed work |
| "evaluate the pr review" / "review the pr review" | Read `.ai/handoffs/coderabbit-eval.md` — invoke `/orchestrator` to evaluate |
| "run the prompt" / "execute the prompt" | Read and execute `.ai/handoffs/prompt.md` |

## Custom slash commands

When the user references any of these commands, read the corresponding file and execute it immediately — do not wait to be told to "invoke" it.

| Command | File | Action |
|---|---|---|
| `/orchestrator` | `.claude/commands/orchestrator.md` | Load context and act as Orchestrator |
| `/analyzer` | `.claude/commands/analyzer.md` | Load context and act as Analyzer |
| `/implementor` | `.claude/commands/implementor.md` | Load context and act as Implementor |
| `/implement` | `.claude/commands/implement.md` | Read and execute `.ai/handoffs/prompt.md` |
| `/findings` | `.claude/commands/findings.md` | Read and present `.ai/references/analysis.md` |
