# Usage

## Table Of Contents

- [Action Summary](#action-summary)
- [Authentication](#authentication)
- [Mailbox Management](#mailbox-management)
- [Sync](#sync)
- [Reports](#reports)
- [Classification](#classification)
  - [Command reference](#command-reference)
  - [Step-by-step walkthrough](#step-by-step-walkthrough)
  - [Iteration loop](#iteration-loop)
  - [Reducing unknowns](#reducing-unknowns)
  - [Notes](#notes)
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

Classification is mailbox-scoped and operator-invoked. The workflow is:
sync → review suggestions → promote rules → run → review results → repeat.

### Command reference

```bash
# Discover valid values
inboxatlas classify categories
inboxatlas classify intents
inboxatlas classify pattern-types

# Review and promote
inboxatlas classify suggestions --account <id|alias> [--format table|json]
inboxatlas classify promote --account <id|alias> \
  --pattern-type <type> --pattern-value <value> --category <category> [--priority n]

# Run and review
inboxatlas classify run     --account <id|alias>
inboxatlas classify messages --account <id|alias> [--category <category>] [--intent <intent>] [--format table|csv|json] [--limit 100]
inboxatlas classify results --account <id|alias> [--format table|json]

# Manage active seeds
inboxatlas classify seeds list   --account <id|alias> [--format table|json]
inboxatlas classify seeds delete --account <id|alias> --id <seed-id>
```

Valid `--pattern-type` values: `domain`, `sender_email`, `sender_prefix`, `has_attachment`, `subject_term`

Valid `--category` values: `internal`, `client`, `vendor`, `government`, `system-generated`, `newsletter/marketing`, `social`, `unknown`

Deterministic intent values: `invoice`, `request-for-information`

### Step-by-step walkthrough

**1. Sync your mailbox first.**
Classification runs on locally stored messages — sync must complete before any classify command.

```bash
inboxatlas sync gmail --account your@email.com
```

**2. Discover valid values (optional).**

```bash
inboxatlas classify categories
inboxatlas classify intents
inboxatlas classify pattern-types
```

**3. Review suggestions for your mailbox.**
Read-only. Shows patterns from your mailbox that could become classification rules.

```bash
inboxatlas classify suggestions --account your@email.com
```

**4. Promote the suggestions you agree with.**
Each promote activates one rule for your mailbox only. Safe to run more than once for the same rule.

```bash
inboxatlas classify promote \
  --account your@email.com \
  --pattern-type domain \
  --pattern-value law360.com \
  --category newsletter/marketing

**5. Review classified messages directly when you need automation-friendly output.**
Use `classify messages` when you need per-message rows instead of aggregate counts. Combine
`--category` and `--intent` to narrow to combinations such as `client + invoice`. JSON
includes `message_id`, and `--format csv` exports
`MessageID,Timestamp,Sender,Domain,Intent,Category,HasAttachment`.

```bash
inboxatlas classify messages --account your@email.com --category client --intent invoice --format json
```

inboxatlas classify promote \
  --account your@email.com \
  --pattern-type sender_email \
  --pattern-value acr@acrbookkeepingplus.com \
  --category vendor
```

**5. Check what seeds are active.**

```bash
inboxatlas classify seeds list --account your@email.com
```

Remove a seed promoted by mistake (use the ID shown in `seeds list`):

```bash
inboxatlas classify seeds delete --account your@email.com --id 7
```

**6. Run classification.**
Applies all built-in global rules plus your promoted seeds to every message. Safe to re-run after promoting more seeds.

```bash
inboxatlas classify run --account your@email.com
```

Output shows total count, a per-category breakdown, and unknown percentage.

**7. Review results.**

```bash
inboxatlas classify results --account your@email.com
```

A high unknown percentage means many messages did not match any rule. Promote more seeds and re-run.

### Iteration loop

```bash
inboxatlas classify run --account your@email.com
inboxatlas classify results --account your@email.com

# Unknown% is high — promote more rules and re-run
inboxatlas classify suggestions --account your@email.com
inboxatlas classify promote --account your@email.com \
  --pattern-type domain --pattern-value github.com --category vendor
inboxatlas classify run --account your@email.com
inboxatlas classify results --account your@email.com
```

### Reducing unknowns

After `classify run`, a high unknown percentage means messages that matched no rule.
Use this process to identify and address the remaining unknown sources.

**Step 1 — Check current coverage.**

```bash
inboxatlas classify results --account your@email.com
```

Note the unknown count and percentage.

**Step 2 — Find high-volume unknown sources.**

```bash
inboxatlas report domains --account your@email.com --format table --limit 200
inboxatlas classify seeds list --account your@email.com
```

Domains that appear in `report domains` but not in `classify seeds list` are your unknown
contributors. Sort by count and address the highest-volume ones first.

**Step 3 — Generate promote commands for unseeded suggestions.**

The following script outputs ready-to-run promote commands for every suggestion that has
not yet been promoted. Replace `<CATEGORY>` with the correct category before running.

```bash
ACCOUNT=your@email.com

comm -23 \
  <(inboxatlas classify suggestions --account "$ACCOUNT" \
      | awk 'NR>1 {print $1 ":" $2}' | sort) \
  <(inboxatlas classify seeds list --account "$ACCOUNT" \
      | awk 'NR>1 {print $2 ":" $3}' | sort) \
| while IFS=: read ptype pvalue; do
    echo "inboxatlas classify promote --account $ACCOUNT --pattern-type $ptype --pattern-value $pvalue --category <CATEGORY>"
  done
```

This prints only the suggestions you have not yet promoted. Review each line, fill in
the category, and run the commands you agree with.

**Step 4 — Re-run and repeat.**

```bash
inboxatlas classify run --account your@email.com
inboxatlas classify results --account your@email.com
```

Repeat steps 2–4 until unknown% reaches an acceptable floor. Domains below the
suggestion threshold (too few messages) will not appear in `classify suggestions` and
cannot be promoted — this is expected and represents a natural classification floor.

### Notes

- `--account` is required for all mailbox-scoped classify commands.
- `classify run` does not trigger a sync — it operates only on messages already stored locally.
- `classify results` is read-only and reports on classifications already stored.
- `classify suggestions` is read-only and does not activate any seed.
- `classify seeds list` and `classify seeds delete` operate only on mailbox-scoped seeds; global built-in defaults are protected from deletion.
- `classify promote` is idempotent for the same mailbox, pattern, category, and priority.
- `classify promote` only accepts patterns that appear in `classify suggestions` — low-volume domains below the suggestion threshold cannot be promoted.

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
