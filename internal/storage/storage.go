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
	if _, err := db.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("enable foreign keys: %w", err)
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
    has_attachment  INTEGER NOT NULL DEFAULT 0,
    attachment_types TEXT NOT NULL DEFAULT '[]',
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

CREATE TABLE IF NOT EXISTS classification_seeds (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    mailbox_id    TEXT,
    pattern_type  TEXT NOT NULL,
    pattern_value TEXT NOT NULL,
    category      TEXT NOT NULL,
    source        TEXT NOT NULL DEFAULT 'seed',
    priority      INTEGER NOT NULL DEFAULT 100,
    created_at    TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uix_classification_seeds
    ON classification_seeds (COALESCE(mailbox_id, ''), pattern_type, pattern_value);

CREATE TABLE IF NOT EXISTS message_classifications (
    message_id    TEXT NOT NULL REFERENCES messages(id),
    mailbox_id    TEXT NOT NULL REFERENCES mailboxes(id),
    category      TEXT NOT NULL,
    intent        TEXT NOT NULL DEFAULT '',
    matched_rule  TEXT,
    source        TEXT NOT NULL,
    classified_at TEXT NOT NULL,
    PRIMARY KEY (message_id, mailbox_id)
);

CREATE TABLE IF NOT EXISTS ai_inference_suggestions (
    mailbox_id       TEXT NOT NULL REFERENCES mailboxes(id),
    message_id       TEXT NOT NULL REFERENCES messages(id),
    pattern_type     TEXT NOT NULL,
    pattern_value    TEXT NOT NULL,
    category         TEXT NOT NULL,
    confidence       REAL NOT NULL,
    confidence_band  TEXT NOT NULL,
    evidence_json    TEXT NOT NULL,
    review_required  INTEGER NOT NULL DEFAULT 1,
    created_at       TEXT NOT NULL,
    PRIMARY KEY (mailbox_id, message_id)
);
`

func (s *Store) migrate() error {
	if _, err := s.db.Exec(schema); err != nil {
		return err
	}
	if err := s.migrateAttachmentColumns(); err != nil {
		return err
	}
	if err := s.migrateClassificationIntentColumn(); err != nil {
		return err
	}
	return s.migrateClassificationMessagesIndex()
}

func (s *Store) migrateAttachmentColumns() error {
	columns, err := s.messageColumns()
	if err != nil {
		return err
	}
	if _, ok := columns["has_attachment"]; !ok {
		if _, err := s.db.Exec(`ALTER TABLE messages ADD COLUMN has_attachment INTEGER NOT NULL DEFAULT 0`); err != nil {
			return fmt.Errorf("add messages.has_attachment: %w", err)
		}
	}
	if _, ok := columns["attachment_types"]; !ok {
		if _, err := s.db.Exec(`ALTER TABLE messages ADD COLUMN attachment_types TEXT NOT NULL DEFAULT '[]'`); err != nil {
			return fmt.Errorf("add messages.attachment_types: %w", err)
		}
	}
	return nil
}

func (s *Store) messageColumns() (map[string]struct{}, error) {
	return s.tableColumns("messages")
}

func (s *Store) migrateClassificationIntentColumn() error {
	columns, err := s.tableColumns("message_classifications")
	if err != nil {
		return err
	}
	if _, ok := columns["intent"]; ok {
		return nil
	}
	if _, err := s.db.Exec(`ALTER TABLE message_classifications ADD COLUMN intent TEXT NOT NULL DEFAULT ''`); err != nil {
		return fmt.Errorf("add message_classifications.intent: %w", err)
	}
	return nil
}

func (s *Store) migrateClassificationMessagesIndex() error {
	_, err := s.db.Exec(`
		CREATE INDEX IF NOT EXISTS idx_message_classifications_mailbox_category_intent
		ON message_classifications (mailbox_id, category, intent)`)
	if err != nil {
		return fmt.Errorf("add message_classifications index: %w", err)
	}
	return nil
}

func (s *Store) tableColumns(table string) (map[string]struct{}, error) {
	rows, err := s.db.Query(fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return nil, fmt.Errorf("pragma table_info(%s): %w", table, err)
	}
	defer func() { _ = rows.Close() }()

	columns := make(map[string]struct{})
	for rows.Next() {
		var (
			cid        int
			name       string
			typ        string
			notNull    int
			defaultV   sql.NullString
			primaryKey int
		)
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultV, &primaryKey); err != nil {
			return nil, fmt.Errorf("scan table_info(%s): %w", table, err)
		}
		columns[name] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate table_info(%s): %w", table, err)
	}
	return columns, nil
}

func validateMailboxAlias(alias string) error {
	if strings.Contains(alias, "@") {
		return fmt.Errorf("invalid alias: contains '@'")
	}
	return nil
}

// CreateMailbox inserts mb into the mailboxes table. The ID is canonicalised
// to lowercase before insertion. Returns an error if the ID or alias already
// exists.
func (s *Store) CreateMailbox(ctx context.Context, mb models.Mailbox) error {
	mb.ID = strings.ToLower(mb.ID)
	if err := validateMailboxAlias(mb.Alias); err != nil {
		return err
	}
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

// DeleteMailbox removes the mailbox identified by email address or alias and
// purges all local mailbox-scoped InboxAtlas data in a single transaction.
// Returns an error if no matching mailbox is found.
func (s *Store) DeleteMailbox(ctx context.Context, idOrAlias string) error {
	mb, err := s.GetMailbox(ctx, idOrAlias)
	if err != nil {
		return fmt.Errorf("delete mailbox: %w", err)
	}
	if mb == nil {
		return fmt.Errorf("mailbox %q not found", idOrAlias)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("delete mailbox begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	for _, step := range []struct {
		name  string
		query string
		arg   any
	}{
		{name: "delete classifications", query: `DELETE FROM message_classifications WHERE mailbox_id = ?`, arg: mb.ID},
		{name: "delete mailbox seeds", query: `DELETE FROM classification_seeds WHERE mailbox_id = ?`, arg: mb.ID},
		{name: "delete sender stats", query: `DELETE FROM sender_stats WHERE mailbox_id = ?`, arg: mb.ID},
		{name: "delete domain stats", query: `DELETE FROM domain_stats WHERE mailbox_id = ?`, arg: mb.ID},
		{name: "delete subject term stats", query: `DELETE FROM subject_term_stats WHERE mailbox_id = ?`, arg: mb.ID},
		{name: "delete messages", query: `DELETE FROM messages WHERE mailbox_id = ?`, arg: mb.ID},
		{name: "delete checkpoints", query: `DELETE FROM sync_checkpoint WHERE mailbox_id = ?`, arg: mb.ID},
		{name: "delete mailbox", query: `DELETE FROM mailboxes WHERE id = ?`, arg: mb.ID},
	} {
		res, err := tx.ExecContext(ctx, step.query, step.arg)
		if err != nil {
			return fmt.Errorf("%s: %w", step.name, err)
		}
		if step.name == "delete mailbox" {
			n, _ := res.RowsAffected()
			if n == 0 {
				return fmt.Errorf("delete mailbox: mailbox %q disappeared before final delete", mb.ID)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("delete mailbox commit: %w", err)
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

// UpdateMailboxAlias sets the alias for the mailbox with the given canonical
// email ID.
func (s *Store) UpdateMailboxAlias(ctx context.Context, id, alias string) error {
	if err := validateMailboxAlias(alias); err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx,
		`UPDATE mailboxes SET alias = ? WHERE id = ?`,
		nullableString(alias),
		strings.ToLower(id),
	)
	if err != nil {
		return fmt.Errorf("update mailbox alias: %w", err)
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

// ClassificationCount is a single category aggregate row returned by
// QueryClassificationsByMailbox.
type ClassificationCount struct {
	Category string `json:"category"`
	Intent   string `json:"intent"`
	Count    int    `json:"count"`
}

// ClassifiedMessage is one per-message row returned by QueryClassifiedMessages,
// joining messages with their classification result.
type ClassifiedMessage struct {
	MessageID     string    `json:"message_id"`
	FromEmail     string    `json:"from_email"`
	Domain        string    `json:"domain"`
	Subject       string    `json:"subject"`
	ReceivedAt    time.Time `json:"received_at"`
	HasAttachment bool      `json:"has_attachment"`
	Category      string    `json:"category"`
	Intent        string    `json:"intent"`
	MatchedRule   string    `json:"matched_rule"`
}

// ClassifiedMessagesFilter constrains QueryClassifiedMessages results.
// Empty string fields disable the corresponding filter. Limit 0 disables limit.
type ClassifiedMessagesFilter struct {
	Category string
	Intent   string
	Limit    int
	Since    time.Time
}

// ClassificationSeed is a single rule stored in the classification_seeds table.
// MailboxID is empty for global seeds (apply to all mailboxes).
type ClassificationSeed struct {
	ID           int64
	MailboxID    string // empty = global
	PatternType  string // "domain", "sender_email", "sender_prefix", "subject_term"
	PatternValue string
	Category     string
	Source       string // "seed", "operator", "ai"
	Priority     int    // lower = evaluated first; default 100
	CreatedAt    time.Time
}

// Classification is a single row in the message_classifications table.
type Classification struct {
	MessageID    string
	MailboxID    string
	Category     string
	Intent       string
	MatchedRule  string
	Source       string
	ClassifiedAt time.Time
}

// InferenceEvidence is the persisted structured evidence payload for an AI
// inference suggestion.
type InferenceEvidence struct {
	SubjectPhrases []string `json:"subject_phrases"`
	SnippetPhrases []string `json:"snippet_phrases"`
	SenderSignal   string   `json:"sender_signal"`
	DomainSignal   string   `json:"domain_signal"`
	LabelSignals   []string `json:"label_signals"`
}

// InferenceSuggestion is one persisted mailbox-scoped AI inference candidate
// staged for operator review.
type InferenceSuggestion struct {
	MailboxID      string
	MessageID      string
	PatternType    string
	PatternValue   string
	Category       string
	Confidence     float64
	ConfidenceBand string
	Evidence       InferenceEvidence
	ReviewRequired bool
	CreatedAt      time.Time
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

// QuerySenderStatsByMailbox returns persisted sender aggregate rows for one
// mailbox from the discovery sender_stats table, filtered by minCount and
// ordered by count descending then sender ascending.
func (s *Store) QuerySenderStatsByMailbox(ctx context.Context, mailboxID string, minCount int) ([]SenderCount, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT from_email, from_name, domain, message_count
		 FROM sender_stats
		 WHERE mailbox_id = ? AND from_email != '' AND message_count >= ?
		 ORDER BY message_count DESC, from_email ASC`,
		mailboxID, minCount,
	)
	if err != nil {
		return nil, fmt.Errorf("query sender stats by mailbox: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []SenderCount
	for rows.Next() {
		var sc SenderCount
		var fromName sql.NullString
		var domain sql.NullString
		if err := rows.Scan(&sc.Email, &fromName, &domain, &sc.Count); err != nil {
			return nil, fmt.Errorf("scan sender stats row: %w", err)
		}
		if fromName.Valid {
			sc.Name = fromName.String
		}
		if domain.Valid {
			sc.Domain = domain.String
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

// QueryDomainStatsByMailbox returns persisted domain aggregate rows for one
// mailbox from the discovery domain_stats table, filtered by minCount and
// ordered by count descending then domain ascending.
func (s *Store) QueryDomainStatsByMailbox(ctx context.Context, mailboxID string, minCount int) ([]DomainCount, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT domain, message_count
		 FROM domain_stats
		 WHERE mailbox_id = ? AND domain != '' AND message_count >= ?
		 ORDER BY message_count DESC, domain ASC`,
		mailboxID, minCount,
	)
	if err != nil {
		return nil, fmt.Errorf("query domain stats by mailbox: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []DomainCount
	for rows.Next() {
		var dc DomainCount
		if err := rows.Scan(&dc.Domain, &dc.Count); err != nil {
			return nil, fmt.Errorf("scan domain stats row: %w", err)
		}
		out = append(out, dc)
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

// ListMessageMetaByMailbox returns all stored message metadata for mailboxID.
// Results are ordered by received_at ASC, then provider_id ASC.
func (s *Store) ListMessageMetaByMailbox(ctx context.Context, mailboxID string) ([]models.MessageMeta, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, provider, mailbox_id, thread_id, from_email, from_name, domain, subject, snippet, received_at, labels, has_attachment, attachment_types, provider_id
		 FROM messages
		 WHERE mailbox_id = ?
		 ORDER BY received_at ASC, provider_id ASC`,
		mailboxID,
	)
	if err != nil {
		return nil, fmt.Errorf("list message meta by mailbox: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []models.MessageMeta
	for rows.Next() {
		var msg models.MessageMeta
		var threadID sql.NullString
		var fromEmail sql.NullString
		var fromName sql.NullString
		var domain sql.NullString
		var subject sql.NullString
		var snippet sql.NullString
		var receivedAt string
		var labelsJSON string
		var hasAttachment bool
		var attachmentTypesJSON string

		if err := rows.Scan(
			&msg.ID,
			&msg.Provider,
			&msg.MailboxID,
			&threadID,
			&fromEmail,
			&fromName,
			&domain,
			&subject,
			&snippet,
			&receivedAt,
			&labelsJSON,
			&hasAttachment,
			&attachmentTypesJSON,
			&msg.ProviderID,
		); err != nil {
			return nil, fmt.Errorf("scan message meta row: %w", err)
		}

		if threadID.Valid {
			msg.ThreadID = threadID.String
		}
		if fromEmail.Valid {
			msg.FromEmail = fromEmail.String
		}
		if fromName.Valid {
			msg.FromName = fromName.String
		}
		if domain.Valid {
			msg.Domain = domain.String
		}
		if subject.Valid {
			msg.Subject = subject.String
		}
		if snippet.Valid {
			msg.Snippet = snippet.String
		}

		msg.ReceivedAt, err = parseStoredTimestamp(receivedAt)
		if err != nil {
			return nil, fmt.Errorf("parse message received_at %q: %w", receivedAt, err)
		}
		if err := json.Unmarshal([]byte(labelsJSON), &msg.Labels); err != nil {
			return nil, fmt.Errorf("unmarshal message labels: %w", err)
		}
		msg.HasAttachment = hasAttachment
		if err := json.Unmarshal([]byte(attachmentTypesJSON), &msg.AttachmentTypes); err != nil {
			return nil, fmt.Errorf("unmarshal attachment types: %w", err)
		}

		out = append(out, msg)
	}
	return out, rows.Err()
}

// QueryClassifiedMessages returns per-message classification rows for mailboxID,
// optionally filtered by category and intent. Results are ordered by received_at desc.
func (s *Store) QueryClassifiedMessages(ctx context.Context, mailboxID string, filter ClassifiedMessagesFilter) ([]ClassifiedMessage, error) {
	query := `
		SELECT m.provider_id, m.from_email, m.domain, m.subject, m.received_at, m.has_attachment,
		       mc.category, mc.intent, mc.matched_rule
		FROM messages m
		JOIN message_classifications mc ON m.id = mc.message_id AND m.mailbox_id = mc.mailbox_id
		WHERE mc.mailbox_id = ?
		  AND (? = '' OR mc.category = ?)
		  AND (? = '' OR mc.intent = ?)`
	args := []any{mailboxID, filter.Category, filter.Category, filter.Intent, filter.Intent}
	if !filter.Since.IsZero() {
		query += `
		  AND m.received_at >= ?`
		args = append(args, filter.Since.UTC().Format(time.RFC3339))
	}
	query += `
		ORDER BY m.received_at DESC, m.provider_id ASC`
	if filter.Limit > 0 {
		query += " LIMIT ?"
		args = append(args, filter.Limit)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query classified messages: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []ClassifiedMessage
	for rows.Next() {
		var row ClassifiedMessage
		var fromEmail sql.NullString
		var domain sql.NullString
		var subject sql.NullString
		var receivedAt string
		var hasAttachment bool
		var matchedRule sql.NullString

		if err := rows.Scan(
			&row.MessageID,
			&fromEmail,
			&domain,
			&subject,
			&receivedAt,
			&hasAttachment,
			&row.Category,
			&row.Intent,
			&matchedRule,
		); err != nil {
			return nil, fmt.Errorf("scan classified message row: %w", err)
		}
		if fromEmail.Valid {
			row.FromEmail = fromEmail.String
		}
		if domain.Valid {
			row.Domain = domain.String
		}
		if subject.Valid {
			row.Subject = subject.String
		}
		if matchedRule.Valid {
			row.MatchedRule = matchedRule.String
		}
		row.HasAttachment = hasAttachment
		row.ReceivedAt, err = parseStoredTimestamp(receivedAt)
		if err != nil {
			return nil, fmt.Errorf("parse classified message received_at %q: %w", receivedAt, err)
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// UpsertMessage inserts msg into the messages table. On conflict, only
// attachment metadata is updated so re-syncing can backfill attachment fields
// without overwriting existing sender or subject metadata.
func (s *Store) UpsertMessage(ctx context.Context, msg models.MessageMeta) error {
	labels := msg.Labels
	if labels == nil {
		labels = []string{}
	}
	labelsJSON, err := json.Marshal(labels)
	if err != nil {
		return fmt.Errorf("marshal labels: %w", err)
	}
	attachmentTypes := msg.AttachmentTypes
	if attachmentTypes == nil {
		attachmentTypes = []string{}
	}
	attachmentTypesJSON, err := json.Marshal(attachmentTypes)
	if err != nil {
		return fmt.Errorf("marshal attachment types: %w", err)
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO messages
		 (id, mailbox_id, provider, provider_id, thread_id, from_email, from_name, domain, subject, snippet, received_at, labels, has_attachment, attachment_types)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(provider_id, mailbox_id) DO UPDATE SET
		 has_attachment = excluded.has_attachment,
		 attachment_types = excluded.attachment_types`,
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
		msg.HasAttachment,
		string(attachmentTypesJSON),
	)
	if err != nil {
		return fmt.Errorf("upsert message: %w", err)
	}
	return nil
}

// UpsertSenderStat inserts or updates one sender_stats row for mailboxID.
func (s *Store) UpsertSenderStat(ctx context.Context, mailboxID, fromEmail, fromName, domain string, messageCount int) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO sender_stats (mailbox_id, from_email, from_name, domain, message_count)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(mailbox_id, from_email) DO UPDATE SET
		 from_name = excluded.from_name,
		 domain = excluded.domain,
		 message_count = excluded.message_count`,
		mailboxID,
		fromEmail,
		nullableString(fromName),
		nullableString(domain),
		messageCount,
	)
	if err != nil {
		return fmt.Errorf("upsert sender stat: %w", err)
	}
	return nil
}

// UpsertDomainStat inserts or updates one domain_stats row for mailboxID.
func (s *Store) UpsertDomainStat(ctx context.Context, mailboxID, domain string, messageCount int) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO domain_stats (mailbox_id, domain, message_count)
		 VALUES (?, ?, ?)
		 ON CONFLICT(mailbox_id, domain) DO UPDATE SET
		 message_count = excluded.message_count`,
		mailboxID,
		domain,
		messageCount,
	)
	if err != nil {
		return fmt.Errorf("upsert domain stat: %w", err)
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

// InsertSeed inserts a classification seed. Duplicate (mailbox_id, pattern_type,
// pattern_value) tuples are silently ignored (INSERT OR IGNORE).
func (s *Store) InsertSeed(ctx context.Context, seed ClassificationSeed) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO classification_seeds
		 (mailbox_id, pattern_type, pattern_value, category, source, priority, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		nullableString(seed.MailboxID),
		seed.PatternType,
		seed.PatternValue,
		seed.Category,
		seed.Source,
		seed.Priority,
		time.Now().UTC().Format(time.RFC3339),
	)
	if err != nil {
		return fmt.Errorf("insert seed: %w", err)
	}
	return nil
}

// UpsertSeed inserts a classification seed or updates the existing row with
// matching mailbox scope, pattern type, and pattern value while preserving the
// original created_at timestamp.
func (s *Store) UpsertSeed(ctx context.Context, seed ClassificationSeed) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO classification_seeds
		 (mailbox_id, pattern_type, pattern_value, category, source, priority, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(COALESCE(mailbox_id, ''), pattern_type, pattern_value) DO UPDATE SET
		 category = excluded.category,
		 source = excluded.source,
		 priority = excluded.priority`,
		nullableString(seed.MailboxID),
		seed.PatternType,
		seed.PatternValue,
		seed.Category,
		seed.Source,
		seed.Priority,
		time.Now().UTC().Format(time.RFC3339),
	)
	if err != nil {
		return fmt.Errorf("upsert seed: %w", err)
	}
	return nil
}

// ListSeeds returns all seeds that apply to the given mailboxID: global seeds
// (mailbox_id IS NULL) plus mailbox-specific seeds (mailbox_id = mailboxID).
// When mailboxID is empty, only global seeds are returned.
// Results are ordered by priority ASC, then id ASC.
func (s *Store) ListSeeds(ctx context.Context, mailboxID string) ([]ClassificationSeed, error) {
	var rows *sql.Rows
	var err error
	if mailboxID != "" {
		rows, err = s.db.QueryContext(ctx,
			`SELECT id, mailbox_id, pattern_type, pattern_value, category, source, priority, created_at
			 FROM classification_seeds
			 WHERE mailbox_id IS NULL OR mailbox_id = ?
			 ORDER BY priority ASC, id ASC`,
			mailboxID,
		)
	} else {
		rows, err = s.db.QueryContext(ctx,
			`SELECT id, mailbox_id, pattern_type, pattern_value, category, source, priority, created_at
			 FROM classification_seeds
			 WHERE mailbox_id IS NULL
			 ORDER BY priority ASC, id ASC`,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("list seeds: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []ClassificationSeed
	for rows.Next() {
		seed, err := scanSeed(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *seed)
	}
	return out, rows.Err()
}

// DeleteSeed removes the seed with the given ID.
func (s *Store) DeleteSeed(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM classification_seeds WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete seed: %w", err)
	}
	return nil
}

// SaveInferenceCandidate inserts or updates one mailbox-scoped AI inference
// suggestion keyed by mailbox and message.
func (s *Store) SaveInferenceCandidate(ctx context.Context, suggestion InferenceSuggestion) error {
	evidenceJSON, err := json.Marshal(suggestion.Evidence)
	if err != nil {
		return fmt.Errorf("marshal inference evidence: %w", err)
	}

	_, err = s.db.ExecContext(ctx,
		`INSERT INTO ai_inference_suggestions
		 (mailbox_id, message_id, pattern_type, pattern_value, category, confidence, confidence_band, evidence_json, review_required, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(mailbox_id, message_id) DO UPDATE SET
		 pattern_type = excluded.pattern_type,
		 pattern_value = excluded.pattern_value,
		 category = excluded.category,
		 confidence = excluded.confidence,
		 confidence_band = excluded.confidence_band,
		 evidence_json = excluded.evidence_json,
		 review_required = excluded.review_required`,
		suggestion.MailboxID,
		suggestion.MessageID,
		suggestion.PatternType,
		suggestion.PatternValue,
		suggestion.Category,
		suggestion.Confidence,
		suggestion.ConfidenceBand,
		string(evidenceJSON),
		boolToInt(suggestion.ReviewRequired),
		time.Now().UTC().Format(time.RFC3339),
	)
	if err != nil {
		return fmt.Errorf("save inference candidate: %w", err)
	}
	return nil
}

// ListInferenceSuggestions returns persisted mailbox-scoped AI inference
// suggestions ordered by confidence descending then message ID ascending.
func (s *Store) ListInferenceSuggestions(ctx context.Context, mailboxID string) ([]InferenceSuggestion, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT mailbox_id, message_id, pattern_type, pattern_value, category, confidence, confidence_band, evidence_json, review_required, created_at
		 FROM ai_inference_suggestions
		 WHERE mailbox_id = ?
		 ORDER BY confidence DESC, message_id ASC`,
		mailboxID,
	)
	if err != nil {
		return nil, fmt.Errorf("list inference suggestions: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []InferenceSuggestion
	for rows.Next() {
		suggestion, err := scanInferenceSuggestion(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *suggestion)
	}
	return out, rows.Err()
}

// DeleteInferenceSuggestion removes one persisted AI inference suggestion for a
// mailbox/message pair.
func (s *Store) DeleteInferenceSuggestion(ctx context.Context, mailboxID, messageID string) error {
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM ai_inference_suggestions WHERE mailbox_id = ? AND message_id = ?`,
		mailboxID,
		messageID,
	)
	if err != nil {
		return fmt.Errorf("delete inference suggestion: %w", err)
	}
	return nil
}

// SaveClassification inserts or replaces a message classification.
func (s *Store) SaveClassification(ctx context.Context, c Classification) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO message_classifications
		 (message_id, mailbox_id, category, intent, matched_rule, source, classified_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		c.MessageID,
		c.MailboxID,
		c.Category,
		c.Intent,
		c.MatchedRule,
		c.Source,
		c.ClassifiedAt.UTC().Format(time.RFC3339),
	)
	if err != nil {
		return fmt.Errorf("save classification: %w", err)
	}
	return nil
}

// QueryClassificationsByMailbox returns mailbox-scoped classification counts by
// category and intent ordered by count descending, then category and intent ascending.
func (s *Store) QueryClassificationsByMailbox(ctx context.Context, mailboxID string) ([]ClassificationCount, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT category, intent, COUNT(*)
		 FROM message_classifications
		 WHERE mailbox_id = ?
		 GROUP BY category, intent
		 ORDER BY COUNT(*) DESC, category ASC, intent ASC`,
		mailboxID,
	)
	if err != nil {
		return nil, fmt.Errorf("query classifications by mailbox: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []ClassificationCount
	for rows.Next() {
		var count ClassificationCount
		if err := rows.Scan(&count.Category, &count.Intent, &count.Count); err != nil {
			return nil, fmt.Errorf("scan classification count row: %w", err)
		}
		out = append(out, count)
	}
	return out, rows.Err()
}

// BulkSaveClassifications saves multiple classifications in a single transaction.
func (s *Store) BulkSaveClassifications(ctx context.Context, classifications []Classification) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.PrepareContext(ctx,
		`INSERT OR REPLACE INTO message_classifications
		 (message_id, mailbox_id, category, intent, matched_rule, source, classified_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
	)
	if err != nil {
		return fmt.Errorf("prepare bulk save: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	for _, c := range classifications {
		if _, err := stmt.ExecContext(ctx,
			c.MessageID,
			c.MailboxID,
			c.Category,
			c.Intent,
			c.MatchedRule,
			c.Source,
			c.ClassifiedAt.UTC().Format(time.RFC3339),
		); err != nil {
			return fmt.Errorf("bulk save classification: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit bulk save: %w", err)
	}
	return nil
}

// GetClassification returns the classification for the given message and mailbox.
// Returns (nil, nil) when no classification exists.
func (s *Store) GetClassification(ctx context.Context, messageID, mailboxID string) (*Classification, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT message_id, mailbox_id, category, intent, matched_rule, source, classified_at
		 FROM message_classifications
		 WHERE message_id = ? AND mailbox_id = ?`,
		messageID, mailboxID,
	)
	return scanClassification(row)
}

// scanSeed reads one classification_seeds row from s.
func scanSeed(s scanner) (*ClassificationSeed, error) {
	var seed ClassificationSeed
	var mailboxID sql.NullString
	var createdAt string

	err := s.Scan(
		&seed.ID, &mailboxID, &seed.PatternType, &seed.PatternValue,
		&seed.Category, &seed.Source, &seed.Priority, &createdAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("scan seed: %w", err)
	}
	if mailboxID.Valid {
		seed.MailboxID = mailboxID.String
	}
	seed.CreatedAt, err = time.Parse(time.RFC3339, createdAt)
	if err != nil {
		return nil, fmt.Errorf("parse seed created_at %q: %w", createdAt, err)
	}
	return &seed, nil
}

func scanInferenceSuggestion(s scanner) (*InferenceSuggestion, error) {
	var suggestion InferenceSuggestion
	var evidenceJSON string
	var reviewRequired int
	var createdAt string

	if err := s.Scan(
		&suggestion.MailboxID,
		&suggestion.MessageID,
		&suggestion.PatternType,
		&suggestion.PatternValue,
		&suggestion.Category,
		&suggestion.Confidence,
		&suggestion.ConfidenceBand,
		&evidenceJSON,
		&reviewRequired,
		&createdAt,
	); err != nil {
		return nil, fmt.Errorf("scan inference suggestion: %w", err)
	}
	if err := json.Unmarshal([]byte(evidenceJSON), &suggestion.Evidence); err != nil {
		return nil, fmt.Errorf("unmarshal inference evidence: %w", err)
	}
	parsedCreatedAt, err := time.Parse(time.RFC3339, createdAt)
	if err != nil {
		return nil, fmt.Errorf("parse inference suggestion created_at %q: %w", createdAt, err)
	}
	suggestion.CreatedAt = parsedCreatedAt
	suggestion.ReviewRequired = reviewRequired != 0
	return &suggestion, nil
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func parseStoredTimestamp(value string) (time.Time, error) {
	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return parsed, nil
	}
	if parsed, err := time.Parse("2006-01-02 15:04:05", value); err == nil {
		return parsed, nil
	}
	return time.Time{}, fmt.Errorf("unsupported timestamp format %q", value)
}

// scanClassification reads one message_classifications row from s.
// Returns (nil, nil) on sql.ErrNoRows.
func scanClassification(s scanner) (*Classification, error) {
	var c Classification
	var classifiedAt string

	err := s.Scan(
		&c.MessageID, &c.MailboxID, &c.Category, &c.Intent, &c.MatchedRule, &c.Source, &classifiedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("scan classification: %w", err)
	}
	c.ClassifiedAt, err = time.Parse(time.RFC3339, classifiedAt)
	if err != nil {
		return nil, fmt.Errorf("parse classification classified_at %q: %w", classifiedAt, err)
	}
	return &c, nil
}
