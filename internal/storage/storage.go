// Package storage persists messages, sync checkpoints, and derived aggregates to SQLite.
package storage

import (
	"context"
	"database/sql"
	"encoding/json"
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

CREATE TABLE IF NOT EXISTS messages (
    id              TEXT PRIMARY KEY,
    mailbox_id      TEXT NOT NULL REFERENCES mailboxes(id),
    provider        TEXT NOT NULL,
    provider_id     TEXT NOT NULL,
    thread_id       TEXT,
    from_email      TEXT,
    from_name       TEXT,
    domain          TEXT,
    subject         TEXT,
    snippet         TEXT,
    received_at     TEXT,
    labels          TEXT,
    UNIQUE (provider_id, mailbox_id)
);

CREATE TABLE IF NOT EXISTS sync_checkpoint (
    mailbox_id      TEXT NOT NULL,
    provider        TEXT NOT NULL,
    page_cursor     TEXT,
    messages_synced INTEGER NOT NULL DEFAULT 0,
    status          TEXT NOT NULL DEFAULT 'running',
    started_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    FOREIGN KEY (mailbox_id) REFERENCES mailboxes(id) ON DELETE CASCADE,
    PRIMARY KEY (mailbox_id, provider)
);

CREATE TABLE IF NOT EXISTS sender_stats (
    mailbox_id    TEXT NOT NULL REFERENCES mailboxes(id),
    from_email    TEXT NOT NULL,
    from_name     TEXT,
    domain        TEXT,
    message_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (mailbox_id, from_email)
);

CREATE TABLE IF NOT EXISTS domain_stats (
    mailbox_id    TEXT NOT NULL REFERENCES mailboxes(id),
    domain        TEXT NOT NULL,
    message_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (mailbox_id, domain)
);

CREATE TABLE IF NOT EXISTS subject_term_stats (
    mailbox_id    TEXT NOT NULL REFERENCES mailboxes(id),
    term          TEXT NOT NULL,
    message_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (mailbox_id, term)
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

// SyncCheckpoint records progress for a single mailbox sync session.
// Status values: "running", "completed", "interrupted".
type SyncCheckpoint struct {
	MailboxID      string
	Provider       string
	PageCursor     string // next page token to resume from; empty means start
	MessagesSynced int
	Status         string
	StartedAt      time.Time
	UpdatedAt      time.Time
}

// DomainCount is a single domain aggregate row returned by QueryMessagesByDomain.
type DomainCount struct {
	Domain string
	Count  int
}

// SenderCount is a single sender aggregate row returned by QueryMessagesBySender.
type SenderCount struct {
	Email  string
	Name   string
	Domain string
	Count  int
}

// VolumeCount is a single monthly volume row returned by QueryMessagesByVolume.
type VolumeCount struct {
	Period string // "YYYY-MM"
	Count  int
}

// QueryMessagesByDomain returns domain aggregate counts sorted by count desc.
// When mailboxID is empty, results aggregate across all mailboxes.
func (s *Store) QueryMessagesByDomain(ctx context.Context, mailboxID string, limit int) ([]DomainCount, error) {
	var rows *sql.Rows
	var err error
	if mailboxID != "" {
		rows, err = s.db.QueryContext(ctx,
			`SELECT domain, COUNT(*) FROM messages
			 WHERE mailbox_id = ? AND domain != ''
			 GROUP BY domain ORDER BY COUNT(*) DESC LIMIT ?`,
			mailboxID, limit,
		)
	} else {
		rows, err = s.db.QueryContext(ctx,
			`SELECT domain, COUNT(*) FROM messages
			 WHERE domain != ''
			 GROUP BY domain ORDER BY COUNT(*) DESC LIMIT ?`,
			limit,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("query domains: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []DomainCount
	for rows.Next() {
		var dc DomainCount
		if err := rows.Scan(&dc.Domain, &dc.Count); err != nil {
			return nil, fmt.Errorf("scan domain row: %w", err)
		}
		out = append(out, dc)
	}
	return out, rows.Err()
}

// QueryMessagesBySender returns sender aggregate counts sorted by count desc.
// When mailboxID is empty, results aggregate across all mailboxes.
func (s *Store) QueryMessagesBySender(ctx context.Context, mailboxID string, limit int) ([]SenderCount, error) {
	var rows *sql.Rows
	var err error
	if mailboxID != "" {
		rows, err = s.db.QueryContext(ctx,
			`SELECT from_email, from_name, domain, COUNT(*) FROM messages
			 WHERE mailbox_id = ? AND from_email != ''
			 GROUP BY from_email ORDER BY COUNT(*) DESC LIMIT ?`,
			mailboxID, limit,
		)
	} else {
		rows, err = s.db.QueryContext(ctx,
			`SELECT from_email, from_name, domain, COUNT(*) FROM messages
			 WHERE from_email != ''
			 GROUP BY from_email ORDER BY COUNT(*) DESC LIMIT ?`,
			limit,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("query senders: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []SenderCount
	for rows.Next() {
		var sc SenderCount
		var fromName sql.NullString
		if err := rows.Scan(&sc.Email, &fromName, &sc.Domain, &sc.Count); err != nil {
			return nil, fmt.Errorf("scan sender row: %w", err)
		}
		if fromName.Valid {
			sc.Name = fromName.String
		}
		out = append(out, sc)
	}
	return out, rows.Err()
}

// QueryMessagesByVolume returns monthly message counts sorted by period asc.
// When mailboxID is empty, results aggregate across all mailboxes.
func (s *Store) QueryMessagesByVolume(ctx context.Context, mailboxID string) ([]VolumeCount, error) {
	var rows *sql.Rows
	var err error
	if mailboxID != "" {
		rows, err = s.db.QueryContext(ctx,
			`SELECT strftime('%Y-%m', received_at), COUNT(*) FROM messages
			 WHERE mailbox_id = ?
			 GROUP BY 1 ORDER BY 1 ASC`,
			mailboxID,
		)
	} else {
		rows, err = s.db.QueryContext(ctx,
			`SELECT strftime('%Y-%m', received_at), COUNT(*) FROM messages
			 GROUP BY 1 ORDER BY 1 ASC`,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("query volume: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []VolumeCount
	for rows.Next() {
		var vc VolumeCount
		if err := rows.Scan(&vc.Period, &vc.Count); err != nil {
			return nil, fmt.Errorf("scan volume row: %w", err)
		}
		out = append(out, vc)
	}
	return out, rows.Err()
}

// QuerySubjects returns all non-empty subject strings for the given mailbox.
// When mailboxID is empty, subjects are returned across all mailboxes.
func (s *Store) QuerySubjects(ctx context.Context, mailboxID string) ([]string, error) {
	var rows *sql.Rows
	var err error
	if mailboxID != "" {
		rows, err = s.db.QueryContext(ctx,
			`SELECT subject FROM messages WHERE mailbox_id = ? AND subject != ''`,
			mailboxID,
		)
	} else {
		rows, err = s.db.QueryContext(ctx,
			`SELECT subject FROM messages WHERE subject != ''`,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("query subjects: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []string
	for rows.Next() {
		var subject string
		if err := rows.Scan(&subject); err != nil {
			return nil, fmt.Errorf("scan subject: %w", err)
		}
		out = append(out, subject)
	}
	return out, rows.Err()
}

// UpsertMessage inserts msg into the messages table. Duplicate inserts (same
// provider_id + mailbox_id) are silently ignored — re-syncing is idempotent.
func (s *Store) UpsertMessage(ctx context.Context, msg models.MessageMeta) error {
	labels := msg.Labels
	if labels == nil {
		labels = []string{}
	}
	labelsJSON, err := json.Marshal(labels)
	if err != nil {
		return fmt.Errorf("marshal labels: %w", err)
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO messages
		 (id, mailbox_id, provider, provider_id, thread_id, from_email, from_name, domain, subject, snippet, received_at, labels)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		msg.ProviderID,
		msg.MailboxID,
		msg.Provider,
		msg.ProviderID,
		msg.ThreadID,
		msg.FromEmail,
		msg.FromName,
		msg.Domain,
		msg.Subject,
		msg.Snippet,
		msg.ReceivedAt.UTC().Format(time.RFC3339),
		string(labelsJSON),
	)
	if err != nil {
		return fmt.Errorf("upsert message: %w", err)
	}
	return nil
}

// GetCheckpoint returns the sync checkpoint for the given mailbox and provider.
// Returns (nil, nil) when no checkpoint exists.
func (s *Store) GetCheckpoint(ctx context.Context, mailboxID, provider string) (*SyncCheckpoint, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT mailbox_id, provider, page_cursor, messages_synced, status, started_at, updated_at
		 FROM sync_checkpoint WHERE mailbox_id = ? AND provider = ?`,
		mailboxID, provider,
	)
	return scanCheckpoint(row)
}

// SaveCheckpoint inserts or replaces the sync checkpoint for a mailbox and provider.
func (s *Store) SaveCheckpoint(ctx context.Context, cp SyncCheckpoint) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO sync_checkpoint
		 (mailbox_id, provider, page_cursor, messages_synced, status, started_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		cp.MailboxID,
		cp.Provider,
		cp.PageCursor,
		cp.MessagesSynced,
		cp.Status,
		cp.StartedAt.UTC().Format(time.RFC3339),
		cp.UpdatedAt.UTC().Format(time.RFC3339),
	)
	if err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}
	return nil
}

// DeleteCheckpoint removes the sync checkpoint for the given mailbox and provider.
// Returns an error if no matching checkpoint exists.
func (s *Store) DeleteCheckpoint(ctx context.Context, mailboxID, provider string) error {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM sync_checkpoint WHERE mailbox_id = ? AND provider = ?`,
		mailboxID, provider,
	)
	if err != nil {
		return fmt.Errorf("delete checkpoint: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("checkpoint not found for mailbox %q provider %q", mailboxID, provider)
	}
	return nil
}

// scanCheckpoint reads one sync_checkpoint row from s. Returns (nil, nil) on sql.ErrNoRows.
func scanCheckpoint(s scanner) (*SyncCheckpoint, error) {
	var cp SyncCheckpoint
	var startedAt, updatedAt string

	err := s.Scan(
		&cp.MailboxID, &cp.Provider, &cp.PageCursor,
		&cp.MessagesSynced, &cp.Status, &startedAt, &updatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("scan checkpoint: %w", err)
	}

	cp.StartedAt, err = time.Parse(time.RFC3339, startedAt)
	if err != nil {
		return nil, fmt.Errorf("parse checkpoint started_at %q: %w", startedAt, err)
	}
	cp.UpdatedAt, err = time.Parse(time.RFC3339, updatedAt)
	if err != nil {
		return nil, fmt.Errorf("parse checkpoint updated_at %q: %w", updatedAt, err)
	}
	return &cp, nil
}
