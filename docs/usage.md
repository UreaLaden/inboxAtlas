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
inboxatlas report export \
  --reports-dir <dir> \
  --output-dir <dir> \
  --format excel|html|pdf|all \
  [--owner-email <email>] \
  [--owner-domain <domain>] \
  [--summary-file <path>]
```

Notes:

- `excel` needs only the report CSV inputs
- `html`, `pdf`, and `all` require `--summary-file`
- output filenames are deterministic: `inbox-report-<owner>-<period>.<ext>`
- PDF export is still gated by renderer availability
