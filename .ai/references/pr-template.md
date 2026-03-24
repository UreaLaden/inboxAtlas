# Pull Request

## Summary

Brief description of the change.

Explain:
- what problem this PR solves
- what part of the system it affects
- why the change was necessary

---

## Backlog Mapping

This PR must map to the defined LogSage backlog.

| Epic | Feature | Task |
|-----|-----|-----|
| | | |

Example:

| Epic | Feature | Task |
|-----|-----|-----|
| 2 Detection Engine | 2.2 Log Normalization | 2.2.2 Plaintext Log Parser |

If multiple tasks are included, explain why grouping was necessary.

---

## Scope

### In Scope
List the exact functionality introduced.

Example:
- plaintext log parsing
- timestamp extraction
- level detection

### Out of Scope
Explicitly confirm what was intentionally not implemented.

Example:
- JSON parsing
- logfmt parsing
- multiline grouping
- signal extraction

---

## Architecture Compliance

Confirm the change follows system architecture.

Checklist:

- [ ] CLI remains a thin adapter
- [ ] Engine owns analysis logic
- [ ] No CLI dependency introduced into engine packages
- [ ] Deterministic output preserved
- [ ] No non-deterministic map iteration affecting output

Reference: `.references/tech-spec.md`

---

## Files Changed

List all modified files.

Example:

```

internal/normalize/plaintext.go
internal/normalize/plaintext_parser_test.go
.ai/handoffs/current.md

```

If new files were added, note them.

---

## Tests Added / Updated

List test files created or updated.

Example:

```

internal/normalize/plaintext_parser_test.go

```

Describe what scenarios are covered:

- timestamp parsing
- level detection
- message extraction
- non-structured lines

---

## Test Coverage

Coverage must be **≥ 90% for affected packages**.

Commands used:

```

go test ./internal/normalize -coverprofile=coverage.out
go tool cover -func=coverage.out

```

Coverage result:

```

internal/normalize: XX.X%

```

Threshold check:

- [ ] 90% minimum met

If below threshold, explain why.

---

## Verification Steps

Commands used to validate the change.

```

go test ./...
go build ./cmd/logsage

```

Expected result:

```

PASS
SUCCESS

```

---

## Behavior Change

Describe the observable behavior change introduced by this PR.

Example:

Before:
```

raw lines returned with no parsing

```

After:
```

plaintext logs parsed into structured LogEntry values

```

---

## Risks / Concerns

Document any potential concerns.

Examples:

- edge cases in timestamp parsing
- log level detection ambiguity
- performance considerations

---

## Handoff Record

The implementation must update:

```

.ai/handoffs/current.md

```

Confirm:

- [ ] handoff updated
- [ ] commands run recorded
- [ ] coverage recorded
- [ ] mapped backlog scope recorded

---

## Screenshots / Output (Optional)

If CLI output changed, include examples.

Example:

```

logsage analyze logs.txt

```

Output sample:

```

Top Likely Causes

1. ConnectionRefused (high confidence)

```

---

## Reviewer Checklist

Reviewers should verify:

- [ ] PR maps to a backlog task
- [ ] scope is limited to that task
- [ ] architecture invariants preserved
- [ ] tests added where required
- [ ] coverage ≥ 90%
- [ ] build passes
- [ ] handoff updated
```

---

## Why This Template Works for Your Workflow

It enforces the rules you've been following:

1. **Backlog-driven development**
2. **One task per PR**
3. **Architecture guardrails**
4. **90% test coverage minimum**
5. **AI handoff traceability**

This prevents the common problems you called out earlier:

* AI agents drifting scope
* undocumented implementation decisions
* missing coverage
* broken architecture boundaries
