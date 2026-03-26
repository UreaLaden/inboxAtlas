# InboxAtlas

Inbox discovery and classification platform for large mailboxes.

[![CI](https://github.com/UreaLaden/inboxatlas/actions/workflows/ci.yml/badge.svg)](https://github.com/UreaLaden/inboxatlas/actions/workflows/ci.yml)

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

**InboxAtlas does not:**

- Send, delete, or modify any messages
- Access message bodies — metadata only (sender, subject, labels, timestamps)
- Support email providers other than Gmail in the current release

---

## Architecture

InboxAtlas is structured in four layers:

```
CLI (cmd/inboxatlas) → analysis (internal/analysis) → storage (internal/storage) → provider (internal/providers/gmail)
```

| Package | Responsibility |
|---|---|
| `cmd/inboxatlas` | Cobra command tree, flag parsing, thin handler dispatch |
| `internal/config` | TOML config loading, env var overrides, directory initialization |
| `internal/auth` | OAuth 2.0 desktop flow, service-account delegation, token storage |
| `internal/storage` | SQLite CRUD, mailbox registry, message upsert, checkpoint CRUD, report queries |
| `internal/ingestion` | Synchronous page loop, exponential backoff, checkpoint save/resume |
| `internal/analysis` | Report queries, subject tokenization, table/CSV/JSON rendering |
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

# Remove a mailbox (prompts for confirmation)
inboxatlas mailbox remove --account user@example.com

# Remove without confirmation prompt
inboxatlas mailbox remove --account user@example.com --force
```

The `--account` flag accepts either a full email address or the alias assigned
during authentication.

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

---

## Development

```bash
make fmt            # format all Go source files
make lint           # run golangci-lint
make test           # go test ./...
make build          # build both inboxatlas and ia binaries
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
| 6 — Classification Foundations | Not started | Data-dependent — requires real sync + report data |

Epic 6 is gated on at least one full mailbox sync and one report run against real
mailbox data. The classification categories, seed rules, and matching logic must
be grounded in what the actual corpus reveals before any design work begins.
