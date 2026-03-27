# Installation

## Table Of Contents

- [Prerequisites](#prerequisites)
- [Clone And Build](#clone-and-build)
- [Credentials Location](#credentials-location)
- [Configuration](#configuration)
- [Next Steps](#next-steps)

## Prerequisites

- Go 1.26 or later
- Gmail API credentials
  - desktop OAuth client for per-user auth, or
  - Google Workspace service account key for domain-wide delegation
- `golangci-lint` for local linting and development checks

## Clone And Build

```bash
git clone https://github.com/UreaLaden/inboxatlas.git
cd inboxatlas
go build -o inboxatlas ./cmd/inboxatlas
```

To build both supported binary names:

```bash
make build
```

## Credentials Location

Place `credentials.json` at:

```text
~/.config/inboxatlas/credentials.json
```

Override that path with `INBOXATLAS_CREDENTIALS_PATH` when needed.

## Configuration

InboxAtlas loads configuration from:

```text
~/.config/inboxatlas/config.toml
```

Precedence is:

```text
defaults < config file < environment variables
```

Minimal example:

```toml
storage_path     = "~/.local/share/inboxatlas/inboxatlas.db"
log_level        = "info"
token_dir        = "~/.config/inboxatlas/tokens"
default_provider = "gmail"
credentials_path = "~/.config/inboxatlas/credentials.json"
token_storage    = "keyring"
sync_delay_ms    = 100
```

## Next Steps

After installation, continue with [Usage](usage.md).
