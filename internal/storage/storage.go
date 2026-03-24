// Package storage persists messages, sync checkpoints, and derived aggregates to SQLite.
package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite" // register "sqlite" driver

	"github.com/UreaLaden/inboxatlas/pkg/models"
)

// Store wraps a SQLite database connection and provides typed access to
// InboxAtlas persistent data.
type Store struct {
	db *sql.DB
}

// Open opens (or creates) the SQLite database at path and runs schema
// migrations. Use ":memory:" for an in-memory database suitable for testing.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return s, nil
}

// Close closes the underlying database connection.
func (s *Store) Close() error {
	return s.db.Close()
}

const schema = `
CREATE TABLE IF NOT EXISTS mailboxes (
    id             TEXT PRIMARY KEY,
    alias          TEXT UNIQUE,
    provider       TEXT NOT NULL,
    created_at     TEXT NOT NULL,
    last_synced_at TEXT
);
`

func (s *Store) migrate() error {
	_, err := s.db.Exec(schema)
	return err
}

// CreateMailbox inserts mb into the mailboxes table. The ID is canonicalised
// to lowercase before insertion. Returns an error if the ID or alias already
// exists.
func (s *Store) CreateMailbox(ctx context.Context, mb models.Mailbox) error {
	mb.ID = strings.ToLower(mb.ID)
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO mailboxes (id, alias, provider, created_at) VALUES (?, ?, ?, ?)`,
		mb.ID,
		nullableString(mb.Alias),
		mb.Provider,
		time.Now().UTC().Format(time.RFC3339),
	)
	if err != nil {
		return fmt.Errorf("create mailbox: %w", err)
	}
	return nil
}

// GetMailbox fetches a mailbox by email address (if idOrAlias contains "@")
// or by alias. Returns (nil, nil) when no matching mailbox is found.
func (s *Store) GetMailbox(ctx context.Context, idOrAlias string) (*models.Mailbox, error) {
	var row *sql.Row
	if strings.Contains(idOrAlias, "@") {
		row = s.db.QueryRowContext(ctx,
			`SELECT id, alias, provider, created_at, last_synced_at FROM mailboxes WHERE id = ?`,
			strings.ToLower(idOrAlias),
		)
	} else {
		row = s.db.QueryRowContext(ctx,
			`SELECT id, alias, provider, created_at, last_synced_at FROM mailboxes WHERE alias = ?`,
			idOrAlias,
		)
	}
	return scanMailbox(row)
}

// ListMailboxes returns all registered mailboxes ordered by creation time.
func (s *Store) ListMailboxes(ctx context.Context) ([]models.Mailbox, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, alias, provider, created_at, last_synced_at FROM mailboxes ORDER BY created_at ASC`,
	)
	if err != nil {
		return nil, fmt.Errorf("list mailboxes: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []models.Mailbox
	for rows.Next() {
		mb, err := scanMailbox(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *mb)
	}
	return out, rows.Err()
}

// DeleteMailbox removes the mailbox identified by email address or alias.
// Returns an error if no matching mailbox is found.
func (s *Store) DeleteMailbox(ctx context.Context, idOrAlias string) error {
	var res sql.Result
	var err error
	if strings.Contains(idOrAlias, "@") {
		res, err = s.db.ExecContext(ctx,
			`DELETE FROM mailboxes WHERE id = ?`,
			strings.ToLower(idOrAlias),
		)
	} else {
		res, err = s.db.ExecContext(ctx,
			`DELETE FROM mailboxes WHERE alias = ?`,
			idOrAlias,
		)
	}
	if err != nil {
		return fmt.Errorf("delete mailbox: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("mailbox %q not found", idOrAlias)
	}
	return nil
}

// UpdateLastSynced sets the last_synced_at timestamp for the mailbox with the
// given canonical email ID.
func (s *Store) UpdateLastSynced(ctx context.Context, id string, t time.Time) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE mailboxes SET last_synced_at = ? WHERE id = ?`,
		t.UTC().Format(time.RFC3339),
		strings.ToLower(id),
	)
	if err != nil {
		return fmt.Errorf("update last synced: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("mailbox %q not found", id)
	}
	return nil
}

// ResolveMailbox fetches a mailbox by email or alias and returns a descriptive
// error if not found, directing the user to 'inboxatlas mailbox list'.
func ResolveMailbox(ctx context.Context, st *Store, idOrAlias string) (*models.Mailbox, error) {
	mb, err := st.GetMailbox(ctx, idOrAlias)
	if err != nil {
		return nil, err
	}
	if mb == nil {
		return nil, fmt.Errorf("mailbox %q not found — run 'inboxatlas mailbox list' to see registered mailboxes", idOrAlias)
	}
	return mb, nil
}

// scanner is satisfied by both *sql.Row and *sql.Rows.
type scanner interface {
	Scan(dest ...any) error
}

// scanMailbox reads one mailbox row from s. Returns (nil, nil) on sql.ErrNoRows.
func scanMailbox(s scanner) (*models.Mailbox, error) {
	var mb models.Mailbox
	var alias sql.NullString
	var createdAt string
	var lastSyncedAt sql.NullString

	err := s.Scan(&mb.ID, &alias, &mb.Provider, &createdAt, &lastSyncedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("scan mailbox: %w", err)
	}

	if alias.Valid {
		mb.Alias = alias.String
	}

	mb.CreatedAt, err = time.Parse(time.RFC3339, createdAt)
	if err != nil {
		return nil, fmt.Errorf("parse created_at %q: %w", createdAt, err)
	}

	if lastSyncedAt.Valid {
		t, err := time.Parse(time.RFC3339, lastSyncedAt.String)
		if err != nil {
			return nil, fmt.Errorf("parse last_synced_at %q: %w", lastSyncedAt.String, err)
		}
		mb.LastSyncedAt = &t
	}

	return &mb, nil
}

// nullableString converts an empty string to nil so SQLite stores NULL.
func nullableString(s string) any {
	if s == "" {
		return nil
	}
	return s
}
