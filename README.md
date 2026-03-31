# InboxAtlas

Inbox discovery and classification platform for large mailboxes.

[![CI](https://github.com/UreaLaden/inboxatlas/actions/workflows/ci.yml/badge.svg)](https://github.com/UreaLaden/inboxatlas/actions/workflows/ci.yml)

---

## Docs

- [Documentation Index](docs/index.md)
- [Installation](docs/install.md)
- [Usage](docs/usage.md)
- [Troubleshooting](docs/troubleshooting.md)

---

## Table Of Contents

- [Docs](#docs)
- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Authentication](#authentication)
- [Mailbox Management](#mailbox-management)
- [Syncing](#syncing)
- [Reports](#reports)
- [Classification](#classification)
- [Development](#development)
- [Project Status](#project-status)

---

## Overview

InboxAtlas is a read-only metadata discovery system for Gmail. It analyzes large
mailboxes, extracts operational signal from noisy communication, and establishes
a foundation for future routing and workflow automation.

Its first responsibility is understanding:

- who emails come from
- what organizations and domains appear most often
- what common subject and message patterns exist
- what categories naturally emerge from the inbox

This discovery-first approach reduces risk, avoids premature routing mistakes, and
produces the evidence needed to design reliable classification rules.

**Current capabilities:**

- Gmail authentication via per-user OAuth 2.0 desktop flow or domain-wide delegation
- Full mailbox metadata sync with checkpoint resume
- Discovery reports: top domains, senders, subject terms, and monthly volume
- Deterministic mailbox-scoped classification workflows: run, review suggestions, and promote reviewed seeds

**InboxAtlas does not:**

- Send, delete, or modify any messages
- Access message bodies — metadata only (sender, subject, labels, timestamps)
- Support email providers other than Gmail in the current release

---

## Architecture

InboxAtlas is structured in five layers:

```
CLI (cmd/inboxatlas) → engine (internal/engine) → analysis/classification/storage → provider (internal/providers/gmail)
```

| Package | Responsibility |
|---|---|
| `cmd/inboxatlas` | Cobra command tree, flag parsing, thin handler dispatch |
| `internal/engine` | CLI-safe workflow orchestration for classify flows |
| `internal/config` | TOML config loading, env var overrides, directory initialization |
| `internal/auth` | OAuth 2.0 desktop flow, service-account delegation, token storage |
| `internal/storage` | SQLite CRUD, mailbox registry, message upsert, checkpoint CRUD, report queries |
| `internal/ingestion` | Synchronous page loop, exponential backoff, checkpoint save/resume |
| `internal/analysis` | Report queries, subject tokenization, table/CSV/JSON rendering |
| `internal/export` | Reports-directory parsing, normalized export model, command-backed AI summary generation/validation, workbook generation, snapshot HTML rendering, and PDF adapter contracts |
| `internal/aisummary/openai` | OpenAI-specific config, schema-constrained request construction, retry/backoff, and response parsing for the first-party summary provider binary |
| `internal/classification` | Deterministic classification rules, default baseline seeds, mailbox bootstrap suggestions |
| `internal/normalization` | Message normalization: lowercase domain, parse From header, trim fields |
| `internal/providers/gmail` | Gmail REST API adapter — metadata-only, implements `models.MailProvider` |
| `pkg/models` | Shared data types: `Mailbox`, `MessageMeta`, `MailProvider` interface |

All mailbox data is stored in a single SQLite database file on the local filesystem.
No data is sent to external services. The SQLite driver (`modernc.org/sqlite`) is
pure Go — no CGo required.

---

## Prerequisites

- **Go 1.26** or later
- **Gmail API credentials** — either a desktop OAuth client (`credentials.json`) or a
  Google Workspace service account key with domain-wide delegation enabled. Obtain
  credentials from the Google Cloud Console and place the file at
  `~/.config/inboxatlas/credentials.json` (or override via `INBOXATLAS_CREDENTIALS_PATH`).
- **golangci-lint** — required for local development and lint checks

---

## Installation

```bash
git clone https://github.com/UreaLaden/inboxatlas.git
cd inboxatlas
go build -o inboxatlas ./cmd/inboxatlas
```

Both `inboxatlas` and `ia` are supported binary names. To build both at once:

```bash
make build
```

To build the first-party OpenAI summary provider binary:

```bash
go build -o ./bin/openai-summary-provider ./cmd/openai-summary-provider
```

The `ia` binary is functionally identical to `inboxatlas` — help output and command
names adjust automatically based on which binary is invoked.

---

## Configuration

The configuration file is loaded from `~/.config/inboxatlas/config.toml`. The file
and its parent directory are created automatically on first run. All fields are
optional — defaults apply when the file is absent or a field is omitted.

Config precedence: **defaults < config file < environment variables**

```toml
# ~/.config/inboxatlas/config.toml

storage_path     = "~/.local/share/inboxatlas/inboxatlas.db"
log_level        = "info"
token_dir        = "~/.config/inboxatlas/tokens"
default_provider = "gmail"
credentials_path = "~/.config/inboxatlas/credentials.json"
token_storage    = "keyring"
sync_delay_ms    = 100
```

| Field | Default | Environment variable |
|---|---|---|
| `storage_path` | `~/.local/share/inboxatlas/inboxatlas.db` | `INBOXATLAS_STORAGE_PATH` |
| `log_level` | `info` | `INBOXATLAS_LOG_LEVEL` |
| `token_dir` | `~/.config/inboxatlas/tokens` | `INBOXATLAS_TOKEN_DIR` |
| `default_provider` | `gmail` | `INBOXATLAS_DEFAULT_PROVIDER` |
| `credentials_path` | `~/.config/inboxatlas/credentials.json` | `INBOXATLAS_CREDENTIALS_PATH` |
| `token_storage` | `keyring` | `INBOXATLAS_TOKEN_STORAGE` |
| `sync_delay_ms` | `100` | `INBOXATLAS_SYNC_DELAY_MS` |

**Token storage modes:**

- `keyring` (default) — tokens are stored in the OS-native credential store
  (Windows Credential Manager, macOS Keychain, Linux Secret Service). Falls back
  to file storage automatically if the keyring is unavailable.
- `file` — tokens are written to `<token_dir>/gmail/<hash>.json` with mode 0600.
  Set `token_storage = "file"` in config or `INBOXATLAS_TOKEN_STORAGE=file` to
  force file-only mode.

---

## Authentication

Place `credentials.json` at `~/.config/inboxatlas/credentials.json` before running
any auth command. Override the path with `INBOXATLAS_CREDENTIALS_PATH`.

### Option B — Per-user OAuth (desktop app)

Use this option with a Google Cloud project that has the Gmail API enabled and a
desktop OAuth 2.0 client credential configured.

```bash
# Authenticate a mailbox (opens a browser window)
inboxatlas auth gmail --account user@example.com

# Optionally assign a short alias
inboxatlas auth gmail --account user@example.com --alias mywork

# View auth state for all registered mailboxes
inboxatlas auth status
```

### Option A — Domain-wide delegation (service account)

Use this option with a Google Workspace service account that has domain-wide
delegation enabled in the Google Admin console. The `credentials.json` must be a
service account key file.

```bash
# Validate delegation and register the mailbox
inboxatlas auth gmail --account user@example.com --delegated

# View auth state for all registered mailboxes
inboxatlas auth status
```

InboxAtlas detects the credential type at runtime. If the credentials file contains
a service account key, delegation mode is used automatically for all operations.

---

## Mailbox Management

```bash
# List all registered mailboxes
inboxatlas mailbox list

# Remove a mailbox and purge its local InboxAtlas data (prompts for confirmation)
inboxatlas mailbox remove --account user@example.com

# Remove without confirmation prompt
inboxatlas mailbox remove --account user@example.com --force
```

The `--account` flag accepts either a full email address or the alias assigned
during authentication. `mailbox remove` deletes the local InboxAtlas mailbox
record together with synced messages, derived stats, checkpoints, mailbox-scoped
classification seeds, and saved classifications for that mailbox only. It does
not delete anything from the remote provider mailbox.

---

## Syncing

```bash
# Sync all message metadata for a mailbox
inboxatlas sync gmail --account user@example.com

# Check sync status and progress
inboxatlas sync status --account user@example.com
```

Sync progress is written to stdout as each page is processed. If a sync is
interrupted (Ctrl-C, network failure, process exit), the next run automatically
resumes from the last completed checkpoint — no messages are re-fetched.

---

## Reports

All report commands require either `--account <id|alias>` to scope results to a
single mailbox or `--all-accounts` to aggregate across all registered mailboxes.
The two flags are mutually exclusive.

```bash
inboxatlas report domains  --account <id|alias> [--format table|csv|json] [--limit 25]
inboxatlas report senders  --account <id|alias> [--format table|csv|json] [--limit 25]
inboxatlas report subjects --account <id|alias> [--format table|csv|json] [--limit 25]
inboxatlas report volume   --account <id|alias> [--format table|csv|json]
inboxatlas report summarize --reports-dir <dir> [--output-file <path>]
                            [--owner-email <email>] [--owner-domain <domain>]
                            [--provider-command <cmd>] [--provider-arg <arg> ...]
                            [--prompt-file <path>]
inboxatlas report export   --reports-dir <dir> --output-dir <dir> [--format excel|html|pdf|all]
                           [--owner-email <email>] [--owner-domain <domain>] [--summary-file <path>]
```

| Flag | Default | Description |
|---|---|---|
| `--account` | — | Mailbox email or alias |
| `--all-accounts` | false | Aggregate across all mailboxes |
| `--format` | `table` | Output format: `table`, `csv`, or `json` |
| `--limit` | `25` | Maximum rows returned (domains, senders, subjects) |

**Example — top sending domains:**

```
$ inboxatlas report domains --account work

DOMAIN              COUNT
github.com          142
gmail.com            87
slack.com            73
google.com           61
atlassian.com        45
```

`report summarize` builds deterministic summary input from the existing report
artifacts, invokes a configured external AI provider command, validates the
structured response, and writes canonical `summary.md` content for later
snapshot export use. If `--output-file` is omitted, it writes `summary.md`
inside `--reports-dir`. The provider command may also be supplied through
`INBOXATLAS_SUMMARY_PROVIDER_CMD`.

The first-party provider binary added in this repo is `cmd/openai-summary-provider`.
It uses these environment variables:

- `OPENAI_API_KEY` required
- `OPENAI_MODEL` optional
- `OPENAI_BASE_URL` optional
- `OPENAI_TIMEOUT_SECONDS` optional
- `OPENAI_DEBUG` optional

Example:

```bash
go build -o ./bin/openai-summary-provider ./cmd/openai-summary-provider

OPENAI_API_KEY=... inboxatlas report summarize \
  --reports-dir ./reports \
  --owner-email owner@company.com \
  --provider-command ./bin/openai-summary-provider
```

`report export` packages artifacts from an existing reports directory rather
than querying SQLite directly. `excel` needs only the report CSV inputs.
`html`, `pdf`, and `all` still require `--summary-file`; that file may be
manually written or generated first through `report summarize`. Output
filenames are deterministic and use the pattern
`inbox-report-<owner>-<period>.<ext>` inside the selected output directory.
PDF export currently depends on a renderer adapter and will fail explicitly
until a concrete PDF engine is configured in a later feature.

---

## Classification

Classification is mailbox-scoped and operator-invoked. The initial workflow keeps
global baseline defaults separate from mailbox bootstrap suggestions and requires
explicit promotion before mailbox-specific suggestions become active seeds.

```bash
# Run deterministic classification for one mailbox
inboxatlas classify run --account <id|alias>

# Review category counts and unknown% after a classify run
inboxatlas classify results --account <id|alias> [--format table|json]

# Review mailbox bootstrap suggestions
inboxatlas classify suggestions --account <id|alias> [--format table|json]

# List active mailbox-scoped seeds
inboxatlas classify seeds list --account <id|alias> [--format table|json]

# Delete one active mailbox-scoped seed
inboxatlas classify seeds delete --account <id|alias> --id <seed-id>

# List valid categories and pattern types
inboxatlas classify categories
inboxatlas classify pattern-types

# Promote one reviewed suggestion into the active mailbox-scoped seed set
inboxatlas classify promote --account <id|alias> \
  --pattern-type <domain|sender_email|sender_prefix|subject_term> \
  --pattern-value <value> \
  --category <category> \
  [--priority 100]
```

| Command | Purpose |
|---|---|
| `classify run` | Loads synced message metadata for one mailbox, persists mailbox-scoped classifications, and prints a per-category breakdown with unknown percentage |
| `classify results` | Shows mailbox-scoped classification totals, per-category counts, and unknown percentage |
| `classify suggestions` | Shows read-only mailbox bootstrap suggestions derived from observed mailbox discovery data, excluding patterns already covered by global defaults |
| `classify seeds list` | Lists active mailbox-scoped seeds and excludes global defaults from the mailbox operator view |
| `classify seeds delete` | Deletes one active mailbox-scoped seed by ID and refuses to delete global defaults |
| `classify categories` | Prints the valid deterministic classification taxonomy values |
| `classify pattern-types` | Prints the valid deterministic seed pattern types |
| `classify promote` | Validates one suggestion for the target mailbox and persists it as an active mailbox-scoped operator seed |

Notes:

- `--account` is required for mailbox-scoped classify commands.
- `classify run` does not trigger sync; it operates on messages already stored locally and prints the post-run category breakdown immediately.
- `classify results` is read-only and reports on classifications already stored locally.
- `classify suggestions` is read-only and does not activate any seed.
- `classify seeds list` and `classify seeds delete` operate only on mailbox-scoped active seeds; global defaults remain protected.
- `classify promote` is idempotent for the same mailbox, pattern, category, and priority.

---

## Development

```bash
make fmt            # format all Go source files
make lint           # run golangci-lint
make test           # go test ./...
make build          # build inboxatlas, ia, and openai-summary-provider
make coverage       # generate coverage profile (coverage.out)
make coverage-func  # function-level coverage breakdown
make coverage-total # total repository coverage summary line
```

Total repository coverage must be **≥ 90%** at all times. This threshold is
enforced in CI — pull requests that drop total coverage below 90% will fail the
`Validate Test Coverage` job.

---

## Project Status

| Epic | Status | Description |
|---|---|---|
| 1 — Foundation | Complete | Repository setup, config model, logging baseline |
| 2 — Mailbox Registry | Complete | Mailbox entity, CLI commands (`list`, `remove`) |
| 3 — Gmail Authentication | Complete | Per-user OAuth flow and domain-wide delegation |
| 4 — Metadata Sync | Complete | Ingestion pipeline, checkpoint resume, sync CLI |
| 5 — Discovery Reports | Complete | Domain, sender, subject, and volume reports |
| 6 — Classification Foundations | Complete | Classification storage, rule engine, default baseline seeds |
| 8 — Classification Onboarding and Operations | In progress | Seed generalization, per-inbox execution, and initial classify workflow |

Classification foundations and the first mailbox-scoped classify workflow are now
implemented. Remaining classification work is focused on operational refinement:
additional review flows, future automation hooks, and broader orchestration.
