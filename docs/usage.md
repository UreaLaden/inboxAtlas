# Usage

## Table Of Contents

- [Action Summary](#action-summary)
- [Authentication](#authentication)
- [Mailbox Management](#mailbox-management)
- [Sync](#sync)
- [Reports](#reports)
- [Classification](#classification)
- [Export](#export)

## Action Summary

Typical InboxAtlas flow is: authenticate a mailbox, confirm the mailbox record,
sync metadata, inspect reports, then run classification or export artifacts from
the generated report set as needed.

## Authentication

Per-user OAuth:

```bash
inboxatlas auth gmail --account user@example.com
inboxatlas auth gmail --account user@example.com --alias mywork
inboxatlas auth status
```

Domain-wide delegation:

```bash
inboxatlas auth gmail --account user@example.com --delegated
inboxatlas auth status
```

## Mailbox Management

```bash
inboxatlas mailbox list
inboxatlas mailbox remove --account user@example.com
inboxatlas mailbox remove --account user@example.com --force
```

## Sync

```bash
inboxatlas sync gmail --account user@example.com
inboxatlas sync status --account user@example.com
```

## Reports

Mailbox-scoped or all-accounts reports:

```bash
inboxatlas report domains  --account <id|alias> [--format table|csv|json] [--limit 25]
inboxatlas report senders  --account <id|alias> [--format table|csv|json] [--limit 25]
inboxatlas report subjects --account <id|alias> [--format table|csv|json] [--limit 25]
inboxatlas report volume   --account <id|alias> [--format table|csv|json]
inboxatlas report summarize --reports-dir <dir> [--output-file <path>] [--owner-email <email>] [--owner-domain <domain>] [--provider-command <cmd>] [--provider-arg <arg> ...] [--prompt-file <path>]
```

`--account` and `--all-accounts` are mutually exclusive.

## Classification

```bash
inboxatlas classify run --account <id|alias>
inboxatlas classify suggestions --account <id|alias> [--format table|json]
inboxatlas classify promote --account <id|alias> --pattern-type <type> --pattern-value <value> --category <category> [--priority n]
```

## Export

Export operates on an existing reports directory rather than reading SQLite
directly:

```bash
inboxatlas report summarize \
  --reports-dir <dir> \
  [--output-file <path>] \
  [--owner-email <email>] \
  [--owner-domain <domain>] \
  [--provider-command <cmd>] \
  [--provider-arg <arg> ...] \
  [--prompt-file <path>]

inboxatlas report export \
  --reports-dir <dir> \
  --output-dir <dir> \
  --format excel|html|pdf|all \
  [--owner-email <email>] \
  [--owner-domain <domain>] \
  [--summary-file <path>]
```

Notes:

- `report summarize` writes canonical `summary.md` output; if `--output-file` is omitted it defaults to `<reports-dir>/summary.md`
- `report summarize` requires `--provider-command` or `INBOXATLAS_SUMMARY_PROVIDER_CMD`
- the first-party provider binary in this repo is `cmd/openai-summary-provider`
- the OpenAI provider binary uses `OPENAI_API_KEY` and optionally `OPENAI_MODEL`, `OPENAI_BASE_URL`, `OPENAI_TIMEOUT_SECONDS`, and `OPENAI_DEBUG`
- `excel` needs only the report CSV inputs
- `html`, `pdf`, and `all` require `--summary-file`
- output filenames are deterministic: `inbox-report-<owner>-<period>.<ext>`
- PDF export is still gated by renderer availability

Example with the first-party provider:

```bash
go build -o ./bin/openai-summary-provider ./cmd/openai-summary-provider

OPENAI_API_KEY=... inboxatlas report summarize \
  --reports-dir ./reports \
  --owner-email owner@company.com \
  --provider-command ./bin/openai-summary-provider
```
