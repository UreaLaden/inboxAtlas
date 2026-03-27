# Troubleshooting

## Table Of Contents

- [Credentials File Not Found](#credentials-file-not-found)
- [Auth Mode Confusion](#auth-mode-confusion)
- [Mailbox Resolution Errors](#mailbox-resolution-errors)
- [Report Export Summary Errors](#report-export-summary-errors)
- [PDF Export Unavailable](#pdf-export-unavailable)
- [Lint And Test Verification](#lint-and-test-verification)

## Credentials File Not Found

Symptom:

```text
credentials file not found at ...
```

Check:

- `credentials.json` exists
- `INBOXATLAS_CREDENTIALS_PATH` is correct when overridden
- the configured path is readable by the current user

## Auth Mode Confusion

If desktop OAuth and service-account credentials behave differently than
expected, verify which credential file is present. InboxAtlas detects the
credential type at runtime.

## Mailbox Resolution Errors

If `--account` fails, confirm whether you are using:

- the full mailbox email, or
- the alias assigned during authentication

## Report Export Summary Errors

Snapshot-capable exports require a structured `summary.md`.

If `report export` fails because of `--summary-file`, make sure the file
includes:

- `## Key Takeaway`
- `## Secondary Takeaway`
- `## Snapshot`
- `## What This Means`
- `## Opportunities to Improve`
- `## Bottom Line`

## PDF Export Unavailable

If PDF export fails with an unavailable renderer error, that is currently
expected unless a concrete PDF renderer has been wired into the export flow.
Use `excel` or `html` until that dependency is configured.

## Lint And Test Verification

Typical validation commands:

```bash
go test ./...
go build ./...
go vet ./...
make lint
```
