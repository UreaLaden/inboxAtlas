# Handoff — Task Execution Record

```yaml
task_id: <TASK-ID>
task_title: <Short title of the task>
status: <pending | in-progress | completed | blocked>
owner: <codex | claude | chatgpt | human>
branch: <git branch name>
started: <ISO timestamp>
last_updated: <ISO timestamp>
blocked_by: <none | task-id | reason>
repo_map_impact: <updated .ai/repo-map.md | no repo-map update required>
```

## 1. Task Summary

Short description of the work being performed.

Explain:

* what problem this task solves
* what part of the system it touches
* any architectural constraints

---

## 2. Mapped Backlog Scope

Record the exact mapped work items.

Example:

* Epic `2` Detection Engine
* Feature `2.2` Log Normalization
* Task `2.2.1` Normalize raw log input

Explicitly state what is out of scope.

---

## 3. Scope Variance

State whether any work fell outside the mapped scope.

Example:

* none
  or
* added `FailurePhase`; useful, but aligns more naturally with `2.6.2`

Do not leave this implicit.

---

## 4. Planned File Changes (Pre-Execution)

List the files expected to change before implementation begins.

If unknown, say:

```text
TBD — will update after implementation
```

---

## 5. Implementation Steps

Describe the concrete steps taken.

Keep this concise but clear.

---

## 6. Files Changed

List all modified files.

If files were created, note that.

---

## 7. Commands Run

Document commands used to verify the implementation.

Include results if relevant.

---

## 8. Test / Build Results

State clearly whether the build and tests succeeded.

Example:

```text
go test ./...              → PASS
go build ./cmd/logsage     → SUCCESS
```

If failures occurred, explain.

---

## 9. Test Coverage

Record test additions and coverage measurement for changed production code.

Required fields:

* test files added or updated
* coverage commands run
* coverage result for affected package(s)
* whether the 90% minimum was met

Example:

```text
Test files:
- internal/normalize/normalize_test.go

Coverage commands:
- go test ./internal/normalize -coverprofile=coverage.out
- go tool cover -func=coverage.out

Coverage result:
- internal/normalize: 86.4%

Threshold check:
- 90% minimum met: yes
```

If no tests were added, explicitly justify why.

---

## 10. Assumptions Made

List any assumptions made during implementation.

---

## 11. Risks or Concerns

Note any potential issues.

If coverage is below 90%, explain why here as well.

---

## 12. Recommended Next Steps

List follow-up work.

---

## 13. Completion Status

Final status update.

Example:

```text
status: completed
```

Or if blocked:

```text
status: blocked
reason: coverage below 90% and additional tests required
```