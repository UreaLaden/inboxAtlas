package storage

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/UreaLaden/inboxatlas/pkg/models"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	st, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open :memory: store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func createTestMailbox(t *testing.T, st *Store, id string) {
	t.Helper()
	if err := st.CreateMailbox(context.Background(), models.Mailbox{ID: id, Provider: "gmail"}); err != nil {
		t.Fatalf("CreateMailbox(%q): %v", id, err)
	}
}

func TestOpen(t *testing.T) {
	st := newTestStore(t)
	if st == nil {
		t.Fatal("expected non-nil store")
	}
}

func TestOpen_EnablesForeignKeys(t *testing.T) {
	st := newTestStore(t)

	var enabled int
	if err := st.db.QueryRow(`PRAGMA foreign_keys`).Scan(&enabled); err != nil {
		t.Fatalf("PRAGMA foreign_keys: %v", err)
	}
	if enabled != 1 {
		t.Fatalf("foreign key enforcement: got %d, want 1", enabled)
	}
}

func TestOpen_InvalidPath(t *testing.T) {
	_, err := Open("/dev/null/inboxatlas.db")
	if err == nil {
		t.Fatal("expected error for invalid database path")
	}
}

func TestOpen_MigratesAttachmentColumns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	legacySchema := `
CREATE TABLE mailboxes (
	id TEXT PRIMARY KEY,
	alias TEXT UNIQUE,
	provider TEXT NOT NULL,
	created_at TEXT NOT NULL,
	last_synced_at TEXT
);
CREATE TABLE messages (
	id TEXT PRIMARY KEY,
	mailbox_id TEXT NOT NULL REFERENCES mailboxes(id),
	provider TEXT NOT NULL,
	provider_id TEXT NOT NULL,
	thread_id TEXT,
	from_email TEXT,
	from_name TEXT,
	domain TEXT,
	subject TEXT,
	snippet TEXT,
	received_at TEXT,
	labels TEXT,
	UNIQUE (provider_id, mailbox_id)
);
CREATE TABLE message_classifications (
	message_id TEXT NOT NULL REFERENCES messages(id),
	mailbox_id TEXT NOT NULL REFERENCES mailboxes(id),
	category TEXT NOT NULL,
	matched_rule TEXT,
	source TEXT NOT NULL,
	classified_at TEXT NOT NULL,
	PRIMARY KEY (message_id, mailbox_id)
);`
	if _, err := db.Exec(legacySchema); err != nil {
		t.Fatalf("Exec legacy schema: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close legacy db: %v", err)
	}

	st, err := Open(path)
	if err != nil {
		t.Fatalf("Open migrated store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	columns, err := st.messageColumns()
	if err != nil {
		t.Fatalf("messageColumns: %v", err)
	}
	if _, ok := columns["has_attachment"]; !ok {
		t.Fatal("expected has_attachment column after migration")
	}
	if _, ok := columns["attachment_types"]; !ok {
		t.Fatal("expected attachment_types column after migration")
	}

	classificationColumns, err := st.tableColumns("message_classifications")
	if err != nil {
		t.Fatalf("tableColumns(message_classifications): %v", err)
	}
	if _, ok := classificationColumns["intent"]; !ok {
		t.Fatal("expected intent column after migration")
	}
}

func TestMigrations_AreIdempotentOnCurrentSchema(t *testing.T) {
	st := newTestStore(t)

	if err := st.migrateAttachmentColumns(); err != nil {
		t.Fatalf("migrateAttachmentColumns: %v", err)
	}
	if err := st.migrateClassificationIntentColumn(); err != nil {
		t.Fatalf("migrateClassificationIntentColumn: %v", err)
	}
	if err := st.migrate(); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	messageColumns, err := st.tableColumns("messages")
	if err != nil {
		t.Fatalf("tableColumns(messages): %v", err)
	}
	if _, ok := messageColumns["has_attachment"]; !ok {
		t.Fatal("expected has_attachment column on current schema")
	}
	if _, ok := messageColumns["attachment_types"]; !ok {
		t.Fatal("expected attachment_types column on current schema")
	}

	classificationColumns, err := st.tableColumns("message_classifications")
	if err != nil {
		t.Fatalf("tableColumns(message_classifications): %v", err)
	}
	if _, ok := classificationColumns["intent"]; !ok {
		t.Fatal("expected intent column on current schema")
	}
}

func TestOpen_MigratesLegacyClassificationRowsWithEmptyIntent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy-classifications.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	legacySchema := `
CREATE TABLE mailboxes (
	id TEXT PRIMARY KEY,
	alias TEXT UNIQUE,
	provider TEXT NOT NULL,
	created_at TEXT NOT NULL,
	last_synced_at TEXT
);
CREATE TABLE messages (
	id TEXT PRIMARY KEY,
	mailbox_id TEXT NOT NULL REFERENCES mailboxes(id),
	provider TEXT NOT NULL,
	provider_id TEXT NOT NULL,
	thread_id TEXT,
	from_email TEXT,
	from_name TEXT,
	domain TEXT,
	subject TEXT,
	snippet TEXT,
	received_at TEXT,
	labels TEXT,
	UNIQUE (provider_id, mailbox_id)
);
CREATE TABLE message_classifications (
	message_id TEXT NOT NULL REFERENCES messages(id),
	mailbox_id TEXT NOT NULL REFERENCES mailboxes(id),
	category TEXT NOT NULL,
	matched_rule TEXT,
	source TEXT NOT NULL,
	classified_at TEXT NOT NULL,
	PRIMARY KEY (message_id, mailbox_id)
);`
	if _, err := db.Exec(legacySchema); err != nil {
		t.Fatalf("Exec legacy schema: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO mailboxes (id, provider, created_at) VALUES (?, ?, ?)`,
		"user@example.com", "gmail", time.Now().UTC().Format(time.RFC3339),
	); err != nil {
		t.Fatalf("insert mailbox: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO messages (id, mailbox_id, provider, provider_id) VALUES (?, ?, ?, ?)`,
		"msg1", "user@example.com", "gmail", "msg1",
	); err != nil {
		t.Fatalf("insert message: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO message_classifications (message_id, mailbox_id, category, matched_rule, source, classified_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		"msg1", "user@example.com", "vendor", "domain:x.com", "seed", time.Now().UTC().Format(time.RFC3339),
	); err != nil {
		t.Fatalf("insert classification: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close legacy db: %v", err)
	}

	st, err := Open(path)
	if err != nil {
		t.Fatalf("Open migrated store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	got, err := st.GetClassification(context.Background(), "msg1", "user@example.com")
	if err != nil {
		t.Fatalf("GetClassification: %v", err)
	}
	if got == nil {
		t.Fatal("expected migrated classification row")
		return
	}
	if got.Intent != "" {
		t.Fatalf("Intent: got %q, want empty", got.Intent)
	}
}

func TestCreateAndGetMailbox_ByEmail(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	mb := models.Mailbox{ID: "User@Company.com", Alias: "work", Provider: "gmail"}
	if err := st.CreateMailbox(ctx, mb); err != nil {
		t.Fatalf("CreateMailbox: %v", err)
	}

	got, err := st.GetMailbox(ctx, "user@company.com")
	if err != nil {
		t.Fatalf("GetMailbox: %v", err)
	}
	if got == nil {
		t.Fatal("expected mailbox, got nil")
		return
	}
	if got.ID != "user@company.com" {
		t.Errorf("ID: got %q, want %q", got.ID, "user@company.com")
	}
	if got.Alias != "work" {
		t.Errorf("Alias: got %q, want %q", got.Alias, "work")
	}
	if got.Provider != "gmail" {
		t.Errorf("Provider: got %q, want %q", got.Provider, "gmail")
	}
	if got.CreatedAt.IsZero() {
		t.Error("CreatedAt must not be zero")
	}
	if got.LastSyncedAt != nil {
		t.Error("LastSyncedAt should be nil")
	}
}

func TestCreateAndGetMailbox_ByAlias(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	mb := models.Mailbox{ID: "user@company.com", Alias: "work", Provider: "gmail"}
	if err := st.CreateMailbox(ctx, mb); err != nil {
		t.Fatalf("CreateMailbox: %v", err)
	}

	got, err := st.GetMailbox(ctx, "work")
	if err != nil {
		t.Fatalf("GetMailbox by alias: %v", err)
	}
	if got == nil {
		t.Fatal("expected mailbox, got nil")
		return
	}
	if got.ID != "user@company.com" {
		t.Errorf("ID: got %q, want %q", got.ID, "user@company.com")
	}
}

func TestGetMailbox_NotFound(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	got, err := st.GetMailbox(ctx, "nobody@example.com")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil, got %+v", got)
	}
}

func TestCreateMailbox_DuplicateID(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	mb := models.Mailbox{ID: "user@company.com", Provider: "gmail"}
	if err := st.CreateMailbox(ctx, mb); err != nil {
		t.Fatalf("first CreateMailbox: %v", err)
	}
	if err := st.CreateMailbox(ctx, mb); err == nil {
		t.Error("expected error on duplicate ID, got nil")
	}
}

func TestCreateMailbox_DuplicateAlias(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	mb1 := models.Mailbox{ID: "a@company.com", Alias: "work", Provider: "gmail"}
	mb2 := models.Mailbox{ID: "b@company.com", Alias: "work", Provider: "gmail"}

	if err := st.CreateMailbox(ctx, mb1); err != nil {
		t.Fatalf("first CreateMailbox: %v", err)
	}
	if err := st.CreateMailbox(ctx, mb2); err == nil {
		t.Error("expected error on duplicate alias, got nil")
	}
}

func TestCreateMailbox_InvalidAliasContainsAt(t *testing.T) {
	st := newTestStore(t)

	err := st.CreateMailbox(context.Background(), models.Mailbox{
		ID:       "user@company.com",
		Alias:    "work@alias",
		Provider: "gmail",
	})
	if err == nil {
		t.Fatal("expected invalid alias error, got nil")
	}
	if !strings.Contains(err.Error(), "invalid alias") {
		t.Fatalf("expected invalid alias error, got %v", err)
	}
}

func TestCreateMailbox_NoAlias(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	mb := models.Mailbox{ID: "user@company.com", Provider: "gmail"}
	if err := st.CreateMailbox(ctx, mb); err != nil {
		t.Fatalf("CreateMailbox without alias: %v", err)
	}

	got, err := st.GetMailbox(ctx, "user@company.com")
	if err != nil {
		t.Fatalf("GetMailbox: %v", err)
	}
	if got.Alias != "" {
		t.Errorf("expected empty alias, got %q", got.Alias)
	}
}

func TestListMailboxes_Empty(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	mailboxes, err := st.ListMailboxes(ctx)
	if err != nil {
		t.Fatalf("ListMailboxes: %v", err)
	}
	if len(mailboxes) != 0 {
		t.Errorf("expected 0 mailboxes, got %d", len(mailboxes))
	}
}

func TestListMailboxes_Order(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	for _, id := range []string{"c@x.com", "a@x.com", "b@x.com"} {
		if err := st.CreateMailbox(ctx, models.Mailbox{ID: id, Provider: "gmail"}); err != nil {
			t.Fatalf("CreateMailbox %s: %v", id, err)
		}
	}

	mailboxes, err := st.ListMailboxes(ctx)
	if err != nil {
		t.Fatalf("ListMailboxes: %v", err)
	}
	if len(mailboxes) != 3 {
		t.Fatalf("expected 3 mailboxes, got %d", len(mailboxes))
	}
	// Order by created_at ASC — insertion order here
	if mailboxes[0].ID != "c@x.com" {
		t.Errorf("first mailbox: got %q, want %q", mailboxes[0].ID, "c@x.com")
	}
}

func TestDeleteMailbox_ByEmail(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	if err := st.CreateMailbox(ctx, models.Mailbox{ID: "user@company.com", Provider: "gmail"}); err != nil {
		t.Fatal(err)
	}
	if err := st.DeleteMailbox(ctx, "user@company.com"); err != nil {
		t.Fatalf("DeleteMailbox: %v", err)
	}

	got, err := st.GetMailbox(ctx, "user@company.com")
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Error("expected nil after delete")
	}
}

func TestDeleteMailbox_ByAlias(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	if err := st.CreateMailbox(ctx, models.Mailbox{ID: "user@company.com", Alias: "work", Provider: "gmail"}); err != nil {
		t.Fatal(err)
	}
	if err := st.DeleteMailbox(ctx, "work"); err != nil {
		t.Fatalf("DeleteMailbox by alias: %v", err)
	}

	got, err := st.GetMailbox(ctx, "user@company.com")
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Error("expected nil after delete by alias")
	}
}

func TestDeleteMailbox_PurgesMailboxScopedStateAndPreservesGlobalSeeds(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	createTestMailbox(t, st, "user1@example.com")
	createTestMailbox(t, st, "user2@example.com")
	if err := st.UpdateMailboxAlias(ctx, "user1@example.com", "work"); err != nil {
		t.Fatalf("UpdateMailboxAlias user1: %v", err)
	}

	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	seedMessage(t, st, "m1", "user1@example.com", "a@x.com", "", "x.com", "Hello", now)
	seedMessage(t, st, "m2", "user2@example.com", "b@y.com", "", "y.com", "Other", now)

	if err := st.SaveCheckpoint(ctx, SyncCheckpoint{
		MailboxID:      "user1@example.com",
		Provider:       "gmail",
		PageCursor:     "cursor-1",
		MessagesSynced: 1,
		Status:         "complete",
		StartedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("SaveCheckpoint user1: %v", err)
	}
	if err := st.SaveCheckpoint(ctx, SyncCheckpoint{
		MailboxID:      "user2@example.com",
		Provider:       "gmail",
		PageCursor:     "cursor-2",
		MessagesSynced: 1,
		Status:         "complete",
		StartedAt:      now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("SaveCheckpoint user2: %v", err)
	}

	if _, err := st.db.ExecContext(ctx,
		`INSERT INTO sender_stats (mailbox_id, from_email, from_name, domain, message_count) VALUES (?, ?, ?, ?, ?)`,
		"user1@example.com", "a@x.com", "Alice", "x.com", 1,
	); err != nil {
		t.Fatalf("insert sender_stats user1: %v", err)
	}
	if _, err := st.db.ExecContext(ctx,
		`INSERT INTO sender_stats (mailbox_id, from_email, from_name, domain, message_count) VALUES (?, ?, ?, ?, ?)`,
		"user2@example.com", "b@y.com", "Bob", "y.com", 1,
	); err != nil {
		t.Fatalf("insert sender_stats user2: %v", err)
	}
	if _, err := st.db.ExecContext(ctx,
		`INSERT INTO domain_stats (mailbox_id, domain, message_count) VALUES (?, ?, ?)`,
		"user1@example.com", "x.com", 1,
	); err != nil {
		t.Fatalf("insert domain_stats user1: %v", err)
	}
	if _, err := st.db.ExecContext(ctx,
		`INSERT INTO domain_stats (mailbox_id, domain, message_count) VALUES (?, ?, ?)`,
		"user2@example.com", "y.com", 1,
	); err != nil {
		t.Fatalf("insert domain_stats user2: %v", err)
	}
	if _, err := st.db.ExecContext(ctx,
		`INSERT INTO subject_term_stats (mailbox_id, term, message_count) VALUES (?, ?, ?)`,
		"user1@example.com", "hello", 1,
	); err != nil {
		t.Fatalf("insert subject_term_stats user1: %v", err)
	}
	if _, err := st.db.ExecContext(ctx,
		`INSERT INTO subject_term_stats (mailbox_id, term, message_count) VALUES (?, ?, ?)`,
		"user2@example.com", "other", 1,
	); err != nil {
		t.Fatalf("insert subject_term_stats user2: %v", err)
	}

	if err := st.InsertSeed(ctx, globalSeed("domain", "global.com", "vendor")); err != nil {
		t.Fatalf("InsertSeed global: %v", err)
	}
	if err := st.InsertSeed(ctx, ClassificationSeed{
		MailboxID:    "user1@example.com",
		PatternType:  "domain",
		PatternValue: "x.com",
		Category:     "vendor",
		Source:       "seed",
		Priority:     100,
	}); err != nil {
		t.Fatalf("InsertSeed user1: %v", err)
	}
	if err := st.InsertSeed(ctx, ClassificationSeed{
		MailboxID:    "user2@example.com",
		PatternType:  "domain",
		PatternValue: "y.com",
		Category:     "vendor",
		Source:       "seed",
		Priority:     100,
	}); err != nil {
		t.Fatalf("InsertSeed user2: %v", err)
	}

	if err := st.SaveClassification(ctx, Classification{
		MessageID:    "m1",
		MailboxID:    "user1@example.com",
		Category:     "vendor",
		MatchedRule:  "domain:x.com",
		Source:       "seed",
		ClassifiedAt: now,
	}); err != nil {
		t.Fatalf("SaveClassification user1: %v", err)
	}
	if err := st.SaveClassification(ctx, Classification{
		MessageID:    "m2",
		MailboxID:    "user2@example.com",
		Category:     "vendor",
		MatchedRule:  "domain:y.com",
		Source:       "seed",
		ClassifiedAt: now,
	}); err != nil {
		t.Fatalf("SaveClassification user2: %v", err)
	}

	if err := st.DeleteMailbox(ctx, "work"); err != nil {
		t.Fatalf("DeleteMailbox: %v", err)
	}

	got, err := st.GetMailbox(ctx, "user1@example.com")
	if err != nil {
		t.Fatalf("GetMailbox user1: %v", err)
	}
	if got != nil {
		t.Fatal("expected user1 mailbox to be removed")
	}

	other, err := st.GetMailbox(ctx, "user2@example.com")
	if err != nil {
		t.Fatalf("GetMailbox user2: %v", err)
	}
	if other == nil {
		t.Fatal("expected user2 mailbox to remain")
	}

	for _, tc := range []struct {
		name  string
		query string
		arg   string
		want  int
	}{
		{name: "user1 messages", query: `SELECT COUNT(*) FROM messages WHERE mailbox_id = ?`, arg: "user1@example.com", want: 0},
		{name: "user2 messages", query: `SELECT COUNT(*) FROM messages WHERE mailbox_id = ?`, arg: "user2@example.com", want: 1},
		{name: "user1 checkpoints", query: `SELECT COUNT(*) FROM sync_checkpoint WHERE mailbox_id = ?`, arg: "user1@example.com", want: 0},
		{name: "user2 checkpoints", query: `SELECT COUNT(*) FROM sync_checkpoint WHERE mailbox_id = ?`, arg: "user2@example.com", want: 1},
		{name: "user1 sender_stats", query: `SELECT COUNT(*) FROM sender_stats WHERE mailbox_id = ?`, arg: "user1@example.com", want: 0},
		{name: "user2 sender_stats", query: `SELECT COUNT(*) FROM sender_stats WHERE mailbox_id = ?`, arg: "user2@example.com", want: 1},
		{name: "user1 domain_stats", query: `SELECT COUNT(*) FROM domain_stats WHERE mailbox_id = ?`, arg: "user1@example.com", want: 0},
		{name: "user2 domain_stats", query: `SELECT COUNT(*) FROM domain_stats WHERE mailbox_id = ?`, arg: "user2@example.com", want: 1},
		{name: "user1 subject_term_stats", query: `SELECT COUNT(*) FROM subject_term_stats WHERE mailbox_id = ?`, arg: "user1@example.com", want: 0},
		{name: "user2 subject_term_stats", query: `SELECT COUNT(*) FROM subject_term_stats WHERE mailbox_id = ?`, arg: "user2@example.com", want: 1},
		{name: "user1 mailbox seeds", query: `SELECT COUNT(*) FROM classification_seeds WHERE mailbox_id = ?`, arg: "user1@example.com", want: 0},
		{name: "user2 mailbox seeds", query: `SELECT COUNT(*) FROM classification_seeds WHERE mailbox_id = ?`, arg: "user2@example.com", want: 1},
		{name: "user1 classifications", query: `SELECT COUNT(*) FROM message_classifications WHERE mailbox_id = ?`, arg: "user1@example.com", want: 0},
		{name: "user2 classifications", query: `SELECT COUNT(*) FROM message_classifications WHERE mailbox_id = ?`, arg: "user2@example.com", want: 1},
	} {
		var count int
		if err := st.db.QueryRowContext(ctx, tc.query, tc.arg).Scan(&count); err != nil {
			t.Fatalf("%s count: %v", tc.name, err)
		}
		if count != tc.want {
			t.Fatalf("%s: got %d, want %d", tc.name, count, tc.want)
		}
	}

	var globalSeedCount int
	if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM classification_seeds WHERE mailbox_id IS NULL`).Scan(&globalSeedCount); err != nil {
		t.Fatalf("global seed count: %v", err)
	}
	if globalSeedCount != 1 {
		t.Fatalf("global seeds: got %d, want 1", globalSeedCount)
	}
}

func TestDeleteMailbox_FinalDeleteRowsAffectedCheck(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	if err := st.CreateMailbox(ctx, models.Mailbox{ID: "user@company.com", Provider: "gmail"}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx, `
		CREATE TRIGGER ignore_mailbox_delete
		BEFORE DELETE ON mailboxes
		BEGIN
			DELETE FROM mailboxes WHERE id = OLD.id;
			SELECT RAISE(IGNORE);
		END;
	`); err != nil {
		t.Fatalf("create trigger: %v", err)
	}

	err := st.DeleteMailbox(ctx, "user@company.com")
	if err == nil {
		t.Fatal("expected final delete rows-affected error, got nil")
	}
	if !strings.Contains(err.Error(), "disappeared before final delete") {
		t.Fatalf("expected final delete validation error, got %v", err)
	}
}

func TestDeleteMailbox_NotFound(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	err := st.DeleteMailbox(ctx, "nobody@example.com")
	if err == nil {
		t.Error("expected error when deleting non-existent mailbox")
	}
}

func TestUpdateLastSynced(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	if err := st.CreateMailbox(ctx, models.Mailbox{ID: "user@company.com", Provider: "gmail"}); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC().Truncate(time.Second)
	if err := st.UpdateLastSynced(ctx, "user@company.com", now); err != nil {
		t.Fatalf("UpdateLastSynced: %v", err)
	}

	got, err := st.GetMailbox(ctx, "user@company.com")
	if err != nil {
		t.Fatal(err)
	}
	if got.LastSyncedAt == nil {
		t.Fatal("LastSyncedAt should not be nil")
	}
	if !got.LastSyncedAt.Equal(now) {
		t.Errorf("LastSyncedAt: got %v, want %v", got.LastSyncedAt, now)
	}
}

func TestUpdateMailboxAlias_InvalidAliasContainsAt(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	if err := st.CreateMailbox(ctx, models.Mailbox{ID: "user@company.com", Provider: "gmail"}); err != nil {
		t.Fatal(err)
	}

	err := st.UpdateMailboxAlias(ctx, "user@company.com", "work@alias")
	if err == nil {
		t.Fatal("expected invalid alias error, got nil")
	}
	if !strings.Contains(err.Error(), "invalid alias") {
		t.Fatalf("expected invalid alias error, got %v", err)
	}
}

func TestUpdateLastSynced_NotFound(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	err := st.UpdateLastSynced(ctx, "nobody@example.com", time.Now())
	if err == nil {
		t.Error("expected error when updating non-existent mailbox")
	}
}

func TestResolveMailbox_ByEmail(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	if err := st.CreateMailbox(ctx, models.Mailbox{ID: "user@company.com", Alias: "work", Provider: "gmail"}); err != nil {
		t.Fatal(err)
	}

	mb, err := ResolveMailbox(ctx, st, "user@company.com")
	if err != nil {
		t.Fatalf("ResolveMailbox: %v", err)
	}
	if mb.ID != "user@company.com" {
		t.Errorf("ID: got %q, want %q", mb.ID, "user@company.com")
	}
}

func TestResolveMailbox_ByAlias(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	if err := st.CreateMailbox(ctx, models.Mailbox{ID: "user@company.com", Alias: "work", Provider: "gmail"}); err != nil {
		t.Fatal(err)
	}

	mb, err := ResolveMailbox(ctx, st, "work")
	if err != nil {
		t.Fatalf("ResolveMailbox by alias: %v", err)
	}
	if mb.ID != "user@company.com" {
		t.Errorf("ID: got %q, want %q", mb.ID, "user@company.com")
	}
}

func TestResolveMailbox_NotFound(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	_, err := ResolveMailbox(ctx, st, "nobody")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "inboxatlas mailbox list") {
		t.Errorf("error should mention 'inboxatlas mailbox list', got: %v", err)
	}
}

func TestGetMailbox_ClosedStore(t *testing.T) {
	st := newTestStore(t)
	_ = st.Close()
	got, err := st.GetMailbox(context.Background(), "user@example.com")
	if err == nil {
		t.Error("expected error from closed store")
	}
	if got != nil {
		t.Error("expected nil result")
	}
}

func TestListMailboxes_ClosedStore(t *testing.T) {
	st := newTestStore(t)
	_ = st.Close()
	_, err := st.ListMailboxes(context.Background())
	if err == nil {
		t.Error("expected error from closed store")
	}
}

func TestListMailboxes_ScanError(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	if _, err := st.db.ExecContext(ctx,
		`INSERT INTO mailboxes (id, alias, provider, created_at) VALUES (?, ?, ?, ?)`,
		"user@example.com", nil, "gmail", "not-valid-rfc3339",
	); err != nil {
		t.Fatal(err)
	}
	_, err := st.ListMailboxes(ctx)
	if err == nil {
		t.Error("expected error due to invalid created_at timestamp")
	}
}

func TestResolveMailbox_GetMailboxError(t *testing.T) {
	st := newTestStore(t)
	_ = st.Close()
	_, err := ResolveMailbox(context.Background(), st, "user@example.com")
	if err == nil {
		t.Fatal("expected error from closed store")
	}
}

func TestUpdateLastSynced_ClosedStore(t *testing.T) {
	st := newTestStore(t)
	_ = st.Close()
	err := st.UpdateLastSynced(context.Background(), "user@example.com", time.Now())
	if err == nil {
		t.Error("expected error from closed store")
	}
}

func TestDeleteMailbox_ClosedStore(t *testing.T) {
	st := newTestStore(t)
	_ = st.Close()
	err := st.DeleteMailbox(context.Background(), "user@example.com")
	if err == nil {
		t.Error("expected error from closed store")
	}
}

func TestScanMailbox_InvalidCreatedAt(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	if _, err := st.db.ExecContext(ctx,
		`INSERT INTO mailboxes (id, alias, provider, created_at) VALUES (?, ?, ?, ?)`,
		"user@example.com", nil, "gmail", "not-valid-rfc3339",
	); err != nil {
		t.Fatal(err)
	}
	_, err := st.GetMailbox(ctx, "user@example.com")
	if err == nil {
		t.Error("expected parse error for invalid created_at")
	}
	if !strings.Contains(err.Error(), "parse created_at") {
		t.Errorf("expected 'parse created_at' in error, got: %v", err)
	}
}

func TestScanMailbox_InvalidLastSyncedAt(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	if _, err := st.db.ExecContext(ctx,
		`INSERT INTO mailboxes (id, alias, provider, created_at, last_synced_at) VALUES (?, ?, ?, ?, ?)`,
		"user@example.com", nil, "gmail", time.Now().UTC().Format(time.RFC3339), "not-valid-rfc3339",
	); err != nil {
		t.Fatal(err)
	}
	_, err := st.GetMailbox(ctx, "user@example.com")
	if err == nil {
		t.Error("expected parse error for invalid last_synced_at")
	}
	if !strings.Contains(err.Error(), "parse last_synced_at") {
		t.Errorf("expected 'parse last_synced_at' in error, got: %v", err)
	}
}

// --- Schema smoke test ---

func TestSchema_AllTablesPresent(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	want := []string{"mailboxes", "messages", "sync_checkpoint", "sender_stats", "domain_stats", "subject_term_stats", "classification_seeds", "message_classifications"}
	for _, table := range want {
		var name string
		err := st.db.QueryRowContext(ctx,
			`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, table,
		).Scan(&name)
		if err != nil {
			t.Errorf("table %q not found in schema: %v", table, err)
		}
	}
}

func TestSchema_SyncCheckpointHasMailboxForeignKey(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	rows, err := st.db.QueryContext(ctx, `PRAGMA foreign_key_list(sync_checkpoint)`)
	if err != nil {
		t.Fatalf("foreign_key_list(sync_checkpoint): %v", err)
	}
	defer func() { _ = rows.Close() }()

	var found bool
	for rows.Next() {
		var (
			id       int
			seq      int
			table    string
			from     string
			to       string
			onUpdate string
			onDelete string
			match    string
		)
		if err := rows.Scan(&id, &seq, &table, &from, &to, &onUpdate, &onDelete, &match); err != nil {
			t.Fatalf("scan foreign key: %v", err)
		}
		if table == "mailboxes" && from == "mailbox_id" && to == "id" {
			found = true
			if onDelete != "CASCADE" {
				t.Errorf("on delete = %q, want %q", onDelete, "CASCADE")
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("foreign_key_list rows: %v", err)
	}
	if !found {
		t.Fatal("expected sync_checkpoint foreign key on mailbox_id -> mailboxes(id)")
	}
}

// --- UpsertMessage ---

func TestUpsertMessage_Insert(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	createTestMailbox(t, st, "user@example.com")

	msg := models.MessageMeta{
		ProviderID: "gm1",
		MailboxID:  "user@example.com",
		Provider:   "gmail",
		ThreadID:   "thread1",
		FromEmail:  "alice@corp.io",
		FromName:   "Alice",
		Domain:     "corp.io",
		Subject:    "Hello",
		Snippet:    "Hi there",
		ReceivedAt: time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
		Labels:     []string{"INBOX"},
	}
	if err := st.UpsertMessage(ctx, msg); err != nil {
		t.Fatalf("UpsertMessage: %v", err)
	}

	var count int
	if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM messages`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("expected 1 message, got %d", count)
	}
}

func TestUpsertMessage_Idempotent(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	createTestMailbox(t, st, "user@example.com")

	msg := models.MessageMeta{
		ProviderID: "gm2",
		MailboxID:  "user@example.com",
		Provider:   "gmail",
		ReceivedAt: time.Now(),
	}
	if err := st.UpsertMessage(ctx, msg); err != nil {
		t.Fatalf("first UpsertMessage: %v", err)
	}
	if err := st.UpsertMessage(ctx, msg); err != nil {
		t.Fatalf("second UpsertMessage (duplicate): %v", err)
	}

	var count int
	if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM messages`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("expected 1 row after duplicate insert, got %d (not idempotent)", count)
	}
}

func TestUpsertMessage_NilLabels(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	createTestMailbox(t, st, "user@example.com")

	msg := models.MessageMeta{
		ProviderID: "gm3",
		MailboxID:  "user@example.com",
		Provider:   "gmail",
		Labels:     nil,
	}
	if err := st.UpsertMessage(ctx, msg); err != nil {
		t.Fatalf("UpsertMessage with nil Labels: %v", err)
	}

	var labels string
	if err := st.db.QueryRowContext(ctx, `SELECT labels FROM messages WHERE id = 'gm3'`).Scan(&labels); err != nil {
		t.Fatal(err)
	}
	if labels != "[]" {
		t.Errorf("labels column: got %q, want %q", labels, "[]")
	}
}

func TestUpsertMessage_UpdatesAttachmentFieldsOnConflict(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	createTestMailbox(t, st, "user@example.com")

	original := models.MessageMeta{
		ProviderID: "gm-attach",
		MailboxID:  "user@example.com",
		Provider:   "gmail",
		Subject:    "Original subject",
		ReceivedAt: time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
	}
	if err := st.UpsertMessage(ctx, original); err != nil {
		t.Fatalf("first UpsertMessage: %v", err)
	}

	updated := original
	updated.Subject = "Updated subject should not overwrite existing row"
	updated.HasAttachment = true
	updated.AttachmentTypes = []string{"application/pdf"}
	if err := st.UpsertMessage(ctx, updated); err != nil {
		t.Fatalf("second UpsertMessage: %v", err)
	}

	var (
		subject             string
		hasAttachment       bool
		attachmentTypesJSON string
	)
	if err := st.db.QueryRowContext(ctx,
		`SELECT subject, has_attachment, attachment_types FROM messages WHERE provider_id = ? AND mailbox_id = ?`,
		"gm-attach", "user@example.com",
	).Scan(&subject, &hasAttachment, &attachmentTypesJSON); err != nil {
		t.Fatalf("QueryRow: %v", err)
	}
	if subject != "Original subject" {
		t.Fatalf("subject overwritten: got %q", subject)
	}
	if !hasAttachment {
		t.Fatal("has_attachment: got false, want true")
	}
	if attachmentTypesJSON != `["application/pdf"]` {
		t.Fatalf("attachment_types: got %q", attachmentTypesJSON)
	}
}

// --- GetCheckpoint / SaveCheckpoint / DeleteCheckpoint ---

func TestGetCheckpoint_NotFound(t *testing.T) {
	st := newTestStore(t)
	createTestMailbox(t, st, "user@example.com")
	cp, err := st.GetCheckpoint(context.Background(), "user@example.com", "gmail")
	if err != nil {
		t.Fatalf("GetCheckpoint: %v", err)
	}
	if cp != nil {
		t.Errorf("expected nil for missing checkpoint, got %+v", cp)
	}
}

func TestSaveAndGetCheckpoint(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	createTestMailbox(t, st, "user@example.com")

	now := time.Now().UTC().Truncate(time.Second)
	cp := SyncCheckpoint{
		MailboxID:      "user@example.com",
		Provider:       "gmail",
		PageCursor:     "token123",
		MessagesSynced: 42,
		Status:         "running",
		StartedAt:      now,
		UpdatedAt:      now,
	}
	if err := st.SaveCheckpoint(ctx, cp); err != nil {
		t.Fatalf("SaveCheckpoint: %v", err)
	}

	got, err := st.GetCheckpoint(ctx, "user@example.com", "gmail")
	if err != nil {
		t.Fatalf("GetCheckpoint: %v", err)
	}
	if got == nil {
		t.Fatal("expected checkpoint, got nil")
		return
	}
	if got.PageCursor != "token123" {
		t.Errorf("PageCursor = %q, want %q", got.PageCursor, "token123")
	}
	if got.MessagesSynced != 42 {
		t.Errorf("MessagesSynced = %d, want 42", got.MessagesSynced)
	}
	if got.Status != "running" {
		t.Errorf("Status = %q, want %q", got.Status, "running")
	}
	if !got.StartedAt.Equal(now) {
		t.Errorf("StartedAt = %v, want %v", got.StartedAt, now)
	}
}

func TestSaveCheckpoint_ReplaceSemantics(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	createTestMailbox(t, st, "user@example.com")

	now := time.Now().UTC().Truncate(time.Second)
	cp := SyncCheckpoint{
		MailboxID: "user@example.com",
		Provider:  "gmail",
		Status:    "running",
		StartedAt: now,
		UpdatedAt: now,
	}
	if err := st.SaveCheckpoint(ctx, cp); err != nil {
		t.Fatalf("first SaveCheckpoint: %v", err)
	}
	cp.Status = "completed"
	if err := st.SaveCheckpoint(ctx, cp); err != nil {
		t.Fatalf("second SaveCheckpoint: %v", err)
	}

	got, err := st.GetCheckpoint(ctx, "user@example.com", "gmail")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "completed" {
		t.Errorf("Status after replace = %q, want %q", got.Status, "completed")
	}
}

func TestDeleteCheckpoint_Existing(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	createTestMailbox(t, st, "user@example.com")

	now := time.Now().UTC()
	cp := SyncCheckpoint{MailboxID: "user@example.com", Provider: "gmail", Status: "running", StartedAt: now, UpdatedAt: now}
	if err := st.SaveCheckpoint(ctx, cp); err != nil {
		t.Fatal(err)
	}
	if err := st.DeleteCheckpoint(ctx, "user@example.com", "gmail"); err != nil {
		t.Fatalf("DeleteCheckpoint: %v", err)
	}

	got, _ := st.GetCheckpoint(ctx, "user@example.com", "gmail")
	if got != nil {
		t.Error("expected nil after delete")
	}
}

func TestDeleteCheckpoint_NotFound(t *testing.T) {
	st := newTestStore(t)
	createTestMailbox(t, st, "user@example.com")
	err := st.DeleteCheckpoint(context.Background(), "nobody@example.com", "gmail")
	if err == nil {
		t.Error("expected error when deleting non-existent checkpoint")
	}
}

func TestSaveCheckpoint_ClosedStore(t *testing.T) {
	st := newTestStore(t)
	_ = st.Close()

	now := time.Now().UTC()
	err := st.SaveCheckpoint(context.Background(), SyncCheckpoint{
		MailboxID: "user@example.com",
		Provider:  "gmail",
		Status:    "running",
		StartedAt: now,
		UpdatedAt: now,
	})
	if err == nil {
		t.Fatal("expected error from closed store")
	}
}

func TestDeleteCheckpoint_ClosedStore(t *testing.T) {
	st := newTestStore(t)
	_ = st.Close()

	err := st.DeleteCheckpoint(context.Background(), "user@example.com", "gmail")
	if err == nil {
		t.Fatal("expected error from closed store")
	}
}

func TestGetCheckpoint_InvalidUpdatedAt(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	createTestMailbox(t, st, "user@example.com")

	if _, err := st.db.ExecContext(ctx,
		`INSERT INTO sync_checkpoint (mailbox_id, provider, page_cursor, messages_synced, status, started_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		"user@example.com", "gmail", "", 1, "running", time.Now().UTC().Format(time.RFC3339), "not-valid-rfc3339",
	); err != nil {
		t.Fatal(err)
	}

	_, err := st.GetCheckpoint(ctx, "user@example.com", "gmail")
	if err == nil {
		t.Fatal("expected parse error for invalid updated_at")
	}
	if !strings.Contains(err.Error(), "parse checkpoint updated_at") {
		t.Fatalf("expected parse checkpoint updated_at error, got %v", err)
	}
}

func TestEmailCanonicalization(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	// Insert with mixed case
	if err := st.CreateMailbox(ctx, models.Mailbox{ID: "User@Company.COM", Provider: "gmail"}); err != nil {
		t.Fatal(err)
	}

	// Should find with lowercase
	got, err := st.GetMailbox(ctx, "user@company.com")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("expected mailbox after canonicalization, got nil")
		return
	}
	if got.ID != "user@company.com" {
		t.Errorf("ID should be lowercase, got %q", got.ID)
	}
}

// --- Query helpers ---

// seedMessage creates a minimal message with the given fields for query tests.
func seedMessage(t *testing.T, st *Store, providerID, mailboxID, fromEmail, fromName, domain, subject string, receivedAt time.Time) {
	t.Helper()
	msg := models.MessageMeta{
		ProviderID: providerID,
		MailboxID:  mailboxID,
		Provider:   "gmail",
		FromEmail:  fromEmail,
		FromName:   fromName,
		Domain:     domain,
		Subject:    subject,
		ReceivedAt: receivedAt,
	}
	if err := st.UpsertMessage(context.Background(), msg); err != nil {
		t.Fatalf("seedMessage(%q): %v", providerID, err)
	}
}

// --- QueryMessagesByDomain ---

func TestQueryMessagesByDomain_ScopedOrderedDesc(t *testing.T) {
	st := newTestStore(t)
	createTestMailbox(t, st, "user@example.com")

	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	seedMessage(t, st, "m1", "user@example.com", "a@foo.com", "", "foo.com", "", now)
	seedMessage(t, st, "m2", "user@example.com", "b@foo.com", "", "foo.com", "", now)
	seedMessage(t, st, "m3", "user@example.com", "c@bar.com", "", "bar.com", "", now)

	rows, err := st.QueryMessagesByDomain(context.Background(), "user@example.com", 10)
	if err != nil {
		t.Fatalf("QueryMessagesByDomain: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0].Domain != "foo.com" || rows[0].Count != 2 {
		t.Errorf("first row: got %+v, want {foo.com 2}", rows[0])
	}
	if rows[1].Domain != "bar.com" || rows[1].Count != 1 {
		t.Errorf("second row: got %+v, want {bar.com 1}", rows[1])
	}
}

func TestQueryMessagesByDomain_EmptyMailboxID_AllMailboxes(t *testing.T) {
	st := newTestStore(t)
	createTestMailbox(t, st, "user1@example.com")
	createTestMailbox(t, st, "user2@example.com")

	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	seedMessage(t, st, "m1", "user1@example.com", "a@foo.com", "", "foo.com", "", now)
	seedMessage(t, st, "m2", "user2@example.com", "b@foo.com", "", "foo.com", "", now)
	seedMessage(t, st, "m3", "user2@example.com", "c@bar.com", "", "bar.com", "", now)

	rows, err := st.QueryMessagesByDomain(context.Background(), "", 10)
	if err != nil {
		t.Fatalf("QueryMessagesByDomain (all): %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0].Domain != "foo.com" || rows[0].Count != 2 {
		t.Errorf("first row: got %+v", rows[0])
	}
}

func TestQueryMessagesByDomain_EmptyDomainExcluded(t *testing.T) {
	st := newTestStore(t)
	createTestMailbox(t, st, "user@example.com")

	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	seedMessage(t, st, "m1", "user@example.com", "a@foo.com", "", "foo.com", "", now)
	seedMessage(t, st, "m2", "user@example.com", "b@nodom.com", "", "", "", now) // empty domain

	rows, err := st.QueryMessagesByDomain(context.Background(), "user@example.com", 10)
	if err != nil {
		t.Fatalf("QueryMessagesByDomain: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row (empty domain excluded), got %d", len(rows))
	}
}

func TestQueryMessagesByDomain_NoMessages(t *testing.T) {
	st := newTestStore(t)
	createTestMailbox(t, st, "user@example.com")

	rows, err := st.QueryMessagesByDomain(context.Background(), "user@example.com", 10)
	if err != nil {
		t.Fatalf("QueryMessagesByDomain: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("expected 0 rows, got %d", len(rows))
	}
}

func TestQueryDomainStatsByMailbox_MinCountAndOrdering(t *testing.T) {
	st := newTestStore(t)
	createTestMailbox(t, st, "user@example.com")

	for _, stat := range []struct {
		domain string
		count  int
	}{
		{"alpha.example", 7},
		{"zeta.example", 7},
		{"beta.example", 4},
	} {
		if err := st.UpsertDomainStat(context.Background(), "user@example.com", stat.domain, stat.count); err != nil {
			t.Fatalf("UpsertDomainStat(%s): %v", stat.domain, err)
		}
	}

	rows, err := st.QueryDomainStatsByMailbox(context.Background(), "user@example.com", 5)
	if err != nil {
		t.Fatalf("QueryDomainStatsByMailbox: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0] != (DomainCount{Domain: "alpha.example", Count: 7}) {
		t.Fatalf("first row: got %+v", rows[0])
	}
	if rows[1] != (DomainCount{Domain: "zeta.example", Count: 7}) {
		t.Fatalf("second row: got %+v", rows[1])
	}
}

// --- QueryMessagesBySender ---

func TestQueryMessagesBySender_ScopedOrderedDesc(t *testing.T) {
	st := newTestStore(t)
	createTestMailbox(t, st, "user@example.com")

	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	seedMessage(t, st, "m1", "user@example.com", "alice@foo.com", "Alice", "foo.com", "", now)
	seedMessage(t, st, "m2", "user@example.com", "alice@foo.com", "Alice", "foo.com", "", now.Add(time.Hour))
	seedMessage(t, st, "m3", "user@example.com", "bob@bar.com", "Bob", "bar.com", "", now)

	rows, err := st.QueryMessagesBySender(context.Background(), "user@example.com", 10)
	if err != nil {
		t.Fatalf("QueryMessagesBySender: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0].Email != "alice@foo.com" || rows[0].Count != 2 {
		t.Errorf("first row: got %+v", rows[0])
	}
	if rows[0].Name != "Alice" {
		t.Errorf("expected Name=Alice, got %q", rows[0].Name)
	}
}

func TestQueryMessagesBySender_EmptyMailboxID_AllMailboxes(t *testing.T) {
	st := newTestStore(t)
	createTestMailbox(t, st, "user1@example.com")
	createTestMailbox(t, st, "user2@example.com")

	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	seedMessage(t, st, "m1", "user1@example.com", "alice@foo.com", "Alice", "foo.com", "", now)
	seedMessage(t, st, "m2", "user2@example.com", "alice@foo.com", "Alice", "foo.com", "", now.Add(time.Hour))

	rows, err := st.QueryMessagesBySender(context.Background(), "", 10)
	if err != nil {
		t.Fatalf("QueryMessagesBySender (all): %v", err)
	}
	if len(rows) != 1 || rows[0].Count != 2 {
		t.Errorf("expected 1 row with count=2, got %+v", rows)
	}
}

func TestQueryMessagesBySender_EmptyEmailExcluded(t *testing.T) {
	st := newTestStore(t)
	createTestMailbox(t, st, "user@example.com")

	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	seedMessage(t, st, "m1", "user@example.com", "alice@foo.com", "Alice", "foo.com", "", now)
	seedMessage(t, st, "m2", "user@example.com", "", "", "", "", now) // empty email

	rows, err := st.QueryMessagesBySender(context.Background(), "user@example.com", 10)
	if err != nil {
		t.Fatalf("QueryMessagesBySender: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row (empty email excluded), got %d", len(rows))
	}
}

func TestQueryMessagesBySender_NoMessages(t *testing.T) {
	st := newTestStore(t)
	createTestMailbox(t, st, "user@example.com")

	rows, err := st.QueryMessagesBySender(context.Background(), "user@example.com", 10)
	if err != nil {
		t.Fatalf("QueryMessagesBySender: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("expected 0 rows, got %d", len(rows))
	}
}

func TestQuerySenderStatsByMailbox_MinCountAndOrdering(t *testing.T) {
	st := newTestStore(t)
	createTestMailbox(t, st, "user@example.com")

	for _, stat := range []struct {
		email string
		count int
	}{
		{"alpha@example.com", 7},
		{"zeta@example.com", 7},
		{"beta@example.com", 4},
	} {
		if err := st.UpsertSenderStat(context.Background(), "user@example.com", stat.email, "", "example.com", stat.count); err != nil {
			t.Fatalf("UpsertSenderStat(%s): %v", stat.email, err)
		}
	}

	rows, err := st.QuerySenderStatsByMailbox(context.Background(), "user@example.com", 5)
	if err != nil {
		t.Fatalf("QuerySenderStatsByMailbox: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0].Email != "alpha@example.com" || rows[0].Count != 7 {
		t.Fatalf("first row: got %+v", rows[0])
	}
	if rows[1].Email != "zeta@example.com" || rows[1].Count != 7 {
		t.Fatalf("second row: got %+v", rows[1])
	}
}

func TestUpsertSenderStat_UpdatesExistingRow(t *testing.T) {
	st := newTestStore(t)
	createTestMailbox(t, st, "user@example.com")

	if err := st.UpsertSenderStat(context.Background(), "user@example.com", "owner@example.com", "Owner", "example.com", 2); err != nil {
		t.Fatalf("first UpsertSenderStat: %v", err)
	}
	if err := st.UpsertSenderStat(context.Background(), "user@example.com", "owner@example.com", "Owner Updated", "mail.example.com", 5); err != nil {
		t.Fatalf("second UpsertSenderStat: %v", err)
	}

	rows, err := st.QuerySenderStatsByMailbox(context.Background(), "user@example.com", 1)
	if err != nil {
		t.Fatalf("QuerySenderStatsByMailbox: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0].Name != "Owner Updated" || rows[0].Domain != "mail.example.com" || rows[0].Count != 5 {
		t.Fatalf("unexpected updated sender stat: %+v", rows[0])
	}
}

func TestUpsertDomainStat_UpdatesExistingRow(t *testing.T) {
	st := newTestStore(t)
	createTestMailbox(t, st, "user@example.com")

	if err := st.UpsertDomainStat(context.Background(), "user@example.com", "example.com", 2); err != nil {
		t.Fatalf("first UpsertDomainStat: %v", err)
	}
	if err := st.UpsertDomainStat(context.Background(), "user@example.com", "example.com", 5); err != nil {
		t.Fatalf("second UpsertDomainStat: %v", err)
	}

	rows, err := st.QueryDomainStatsByMailbox(context.Background(), "user@example.com", 1)
	if err != nil {
		t.Fatalf("QueryDomainStatsByMailbox: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0] != (DomainCount{Domain: "example.com", Count: 5}) {
		t.Fatalf("unexpected updated domain stat: %+v", rows[0])
	}
}

// --- QueryMessagesByVolume ---

func TestQueryMessagesByVolume_ScopedOrderedAsc(t *testing.T) {
	st := newTestStore(t)
	createTestMailbox(t, st, "user@example.com")

	seedMessage(t, st, "m1", "user@example.com", "a@x.com", "", "x.com", "", time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC))
	seedMessage(t, st, "m2", "user@example.com", "b@x.com", "", "x.com", "", time.Date(2025, 1, 20, 0, 0, 0, 0, time.UTC))
	seedMessage(t, st, "m3", "user@example.com", "c@x.com", "", "x.com", "", time.Date(2025, 2, 5, 0, 0, 0, 0, time.UTC))

	rows, err := st.QueryMessagesByVolume(context.Background(), "user@example.com")
	if err != nil {
		t.Fatalf("QueryMessagesByVolume: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0].Period != "2025-01" || rows[0].Count != 2 {
		t.Errorf("first row: got %+v", rows[0])
	}
	if rows[1].Period != "2025-02" || rows[1].Count != 1 {
		t.Errorf("second row: got %+v", rows[1])
	}
}

func TestQueryMessagesByVolume_EmptyMailboxID_AllMailboxes(t *testing.T) {
	st := newTestStore(t)
	createTestMailbox(t, st, "user1@example.com")
	createTestMailbox(t, st, "user2@example.com")

	jan := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	seedMessage(t, st, "m1", "user1@example.com", "a@x.com", "", "x.com", "", jan)
	seedMessage(t, st, "m2", "user2@example.com", "b@x.com", "", "x.com", "", jan.Add(24*time.Hour))

	rows, err := st.QueryMessagesByVolume(context.Background(), "")
	if err != nil {
		t.Fatalf("QueryMessagesByVolume (all): %v", err)
	}
	if len(rows) != 1 || rows[0].Count != 2 {
		t.Errorf("expected 1 row with count=2, got %+v", rows)
	}
}

func TestQueryMessagesByVolume_NoMessages(t *testing.T) {
	st := newTestStore(t)
	createTestMailbox(t, st, "user@example.com")

	rows, err := st.QueryMessagesByVolume(context.Background(), "user@example.com")
	if err != nil {
		t.Fatalf("QueryMessagesByVolume: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("expected 0 rows, got %d", len(rows))
	}
}

// --- QuerySubjects ---

func TestQuerySubjects_ReturnNonEmpty(t *testing.T) {
	st := newTestStore(t)
	createTestMailbox(t, st, "user@example.com")

	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	seedMessage(t, st, "m1", "user@example.com", "a@x.com", "", "x.com", "Hello World", now)
	seedMessage(t, st, "m2", "user@example.com", "b@x.com", "", "x.com", "", now.Add(time.Hour)) // empty subject

	subjects, err := st.QuerySubjects(context.Background(), "user@example.com")
	if err != nil {
		t.Fatalf("QuerySubjects: %v", err)
	}
	if len(subjects) != 1 || subjects[0] != "Hello World" {
		t.Errorf("expected [Hello World], got %v", subjects)
	}
}

func TestQuerySubjects_EmptyMailboxID_AllMailboxes(t *testing.T) {
	st := newTestStore(t)
	createTestMailbox(t, st, "user1@example.com")
	createTestMailbox(t, st, "user2@example.com")

	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	seedMessage(t, st, "m1", "user1@example.com", "a@x.com", "", "x.com", "Foo", now)
	seedMessage(t, st, "m2", "user2@example.com", "b@x.com", "", "x.com", "Bar", now.Add(time.Hour))

	subjects, err := st.QuerySubjects(context.Background(), "")
	if err != nil {
		t.Fatalf("QuerySubjects (all): %v", err)
	}
	if len(subjects) != 2 {
		t.Errorf("expected 2 subjects, got %v", subjects)
	}
}

func TestQuerySubjects_NoMessages(t *testing.T) {
	st := newTestStore(t)
	createTestMailbox(t, st, "user@example.com")

	subjects, err := st.QuerySubjects(context.Background(), "user@example.com")
	if err != nil {
		t.Fatalf("QuerySubjects: %v", err)
	}
	if len(subjects) != 0 {
		t.Errorf("expected 0 subjects, got %v", subjects)
	}
}

func TestListMessageMetaByMailbox_ScopedOrdered(t *testing.T) {
	st := newTestStore(t)
	createTestMailbox(t, st, "user1@example.com")
	createTestMailbox(t, st, "user2@example.com")

	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	seedMessage(t, st, "m2", "user1@example.com", "b@example.com", "B", "example.com", "Second", now.Add(time.Hour))
	seedMessage(t, st, "m1", "user1@example.com", "a@example.com", "A", "example.com", "First", now)
	seedMessage(t, st, "m3", "user2@example.com", "c@example.com", "C", "example.com", "Other", now)

	messages, err := st.ListMessageMetaByMailbox(context.Background(), "user1@example.com")
	if err != nil {
		t.Fatalf("ListMessageMetaByMailbox: %v", err)
	}
	if len(messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(messages))
	}
	if messages[0].ProviderID != "m1" || messages[1].ProviderID != "m2" {
		t.Fatalf("unexpected message order: %+v", messages)
	}
	if messages[0].MailboxID != "user1@example.com" || messages[1].MailboxID != "user1@example.com" {
		t.Fatalf("expected mailbox scoping, got %+v", messages)
	}
}

func TestListMessageMetaByMailbox_IncludesAttachmentMetadata(t *testing.T) {
	st := newTestStore(t)
	createTestMailbox(t, st, "user@example.com")

	if err := st.UpsertMessage(context.Background(), models.MessageMeta{
		ProviderID:      "m1",
		MailboxID:       "user@example.com",
		Provider:        "gmail",
		ReceivedAt:      time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		HasAttachment:   true,
		AttachmentTypes: []string{"application/pdf", "image/png"},
	}); err != nil {
		t.Fatalf("UpsertMessage: %v", err)
	}

	messages, err := st.ListMessageMetaByMailbox(context.Background(), "user@example.com")
	if err != nil {
		t.Fatalf("ListMessageMetaByMailbox: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(messages))
	}
	if !messages[0].HasAttachment {
		t.Fatal("HasAttachment: got false, want true")
	}
	if len(messages[0].AttachmentTypes) != 2 || messages[0].AttachmentTypes[0] != "application/pdf" || messages[0].AttachmentTypes[1] != "image/png" {
		t.Fatalf("AttachmentTypes: got %v", messages[0].AttachmentTypes)
	}
}

func TestListMessageMetaByMailbox_NoMessages(t *testing.T) {
	st := newTestStore(t)
	createTestMailbox(t, st, "user@example.com")

	messages, err := st.ListMessageMetaByMailbox(context.Background(), "user@example.com")
	if err != nil {
		t.Fatalf("ListMessageMetaByMailbox: %v", err)
	}
	if len(messages) != 0 {
		t.Fatalf("expected 0 messages, got %d", len(messages))
	}
}

// --- ClassificationSeed CRUD ---

func globalSeed(patternType, patternValue, category string) ClassificationSeed {
	return ClassificationSeed{
		PatternType:  patternType,
		PatternValue: patternValue,
		Category:     category,
		Source:       "seed",
		Priority:     100,
	}
}

func TestInsertAndListSeeds_GlobalOnly(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	seed1 := globalSeed("domain", "example.com", "vendor")
	seed2 := globalSeed("sender_prefix", "noreply", "system-generated")

	if err := st.InsertSeed(ctx, seed1); err != nil {
		t.Fatalf("InsertSeed 1: %v", err)
	}
	if err := st.InsertSeed(ctx, seed2); err != nil {
		t.Fatalf("InsertSeed 2: %v", err)
	}

	// ListSeeds("") returns only global seeds
	seeds, err := st.ListSeeds(ctx, "")
	if err != nil {
		t.Fatalf("ListSeeds: %v", err)
	}
	if len(seeds) != 2 {
		t.Fatalf("expected 2 seeds, got %d", len(seeds))
	}
	if seeds[0].MailboxID != "" {
		t.Errorf("expected empty MailboxID for global seed, got %q", seeds[0].MailboxID)
	}
	if seeds[0].CreatedAt.IsZero() {
		t.Error("CreatedAt must be set")
	}
}

func TestInsertAndListSeeds_GlobalReturnedForMailbox(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	createTestMailbox(t, st, "user@example.com")

	if err := st.InsertSeed(ctx, globalSeed("domain", "example.com", "vendor")); err != nil {
		t.Fatal(err)
	}

	// Global seed should appear when listing for a specific mailbox
	seeds, err := st.ListSeeds(ctx, "user@example.com")
	if err != nil {
		t.Fatalf("ListSeeds: %v", err)
	}
	if len(seeds) != 1 {
		t.Fatalf("expected 1 seed, got %d", len(seeds))
	}
}

func TestListSeedsMailboxScoping(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	createTestMailbox(t, st, "mbA@example.com")
	createTestMailbox(t, st, "mbB@example.com")

	// Insert a global seed
	if err := st.InsertSeed(ctx, globalSeed("domain", "global.com", "vendor")); err != nil {
		t.Fatal(err)
	}
	// Insert a mailbox-A-specific seed
	if err := st.InsertSeed(ctx, ClassificationSeed{
		MailboxID:    "mbA@example.com",
		PatternType:  "domain",
		PatternValue: "mba-only.com",
		Category:     "client",
		Source:       "operator",
		Priority:     50,
	}); err != nil {
		t.Fatal(err)
	}

	seedsA, err := st.ListSeeds(ctx, "mbA@example.com")
	if err != nil {
		t.Fatalf("ListSeeds(mbA): %v", err)
	}
	if len(seedsA) != 2 {
		t.Fatalf("mbA: expected 2 seeds (global + scoped), got %d", len(seedsA))
	}

	seedsB, err := st.ListSeeds(ctx, "mbB@example.com")
	if err != nil {
		t.Fatalf("ListSeeds(mbB): %v", err)
	}
	if len(seedsB) != 1 {
		t.Fatalf("mbB: expected 1 seed (global only), got %d", len(seedsB))
	}
	if seedsB[0].PatternValue != "global.com" {
		t.Errorf("mbB seed: expected global.com, got %q", seedsB[0].PatternValue)
	}
}

func TestListSeeds_OrderedByPriorityThenID(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	// Insert seeds in reverse priority order
	high := ClassificationSeed{PatternType: "domain", PatternValue: "high.com", Category: "vendor", Source: "seed", Priority: 50}
	low := ClassificationSeed{PatternType: "domain", PatternValue: "low.com", Category: "vendor", Source: "seed", Priority: 150}
	med := ClassificationSeed{PatternType: "domain", PatternValue: "med.com", Category: "vendor", Source: "seed", Priority: 100}

	for _, s := range []ClassificationSeed{low, high, med} {
		if err := st.InsertSeed(ctx, s); err != nil {
			t.Fatal(err)
		}
	}

	seeds, err := st.ListSeeds(ctx, "")
	if err != nil {
		t.Fatalf("ListSeeds: %v", err)
	}
	if len(seeds) != 3 {
		t.Fatalf("expected 3 seeds, got %d", len(seeds))
	}
	if seeds[0].Priority != 50 {
		t.Errorf("first seed priority: got %d, want 50", seeds[0].Priority)
	}
	if seeds[1].Priority != 100 {
		t.Errorf("second seed priority: got %d, want 100", seeds[1].Priority)
	}
	if seeds[2].Priority != 150 {
		t.Errorf("third seed priority: got %d, want 150", seeds[2].Priority)
	}
}

func TestSeedUniqueConstraint(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	seed := globalSeed("domain", "example.com", "vendor")
	if err := st.InsertSeed(ctx, seed); err != nil {
		t.Fatalf("InsertSeed first: %v", err)
	}
	// Duplicate insert should be silently ignored (INSERT OR IGNORE)
	if err := st.InsertSeed(ctx, seed); err != nil {
		t.Fatalf("InsertSeed duplicate: expected no error, got %v", err)
	}

	seeds, err := st.ListSeeds(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(seeds) != 1 {
		t.Errorf("expected 1 seed after duplicate insert, got %d", len(seeds))
	}
}

func TestUpsertSeed_UpdatesExistingRowAndPreservesCreatedAt(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	if err := st.InsertSeed(ctx, globalSeed("domain", "example.com", "vendor")); err != nil {
		t.Fatalf("InsertSeed: %v", err)
	}

	before, err := st.ListSeeds(ctx, "")
	if err != nil {
		t.Fatalf("ListSeeds before upsert: %v", err)
	}
	if len(before) != 1 {
		t.Fatalf("expected 1 seed before upsert, got %d", len(before))
	}

	createdAt := before[0].CreatedAt
	id := before[0].ID
	if err := st.UpsertSeed(ctx, ClassificationSeed{
		PatternType:  "domain",
		PatternValue: "example.com",
		Category:     "client",
		Source:       "operator",
		Priority:     25,
	}); err != nil {
		t.Fatalf("UpsertSeed: %v", err)
	}

	after, err := st.ListSeeds(ctx, "")
	if err != nil {
		t.Fatalf("ListSeeds after upsert: %v", err)
	}
	if len(after) != 1 {
		t.Fatalf("expected 1 seed after upsert, got %d", len(after))
	}
	if after[0].ID != id {
		t.Fatalf("seed ID changed after upsert: got %d, want %d", after[0].ID, id)
	}
	if !after[0].CreatedAt.Equal(createdAt) {
		t.Fatalf("CreatedAt changed after upsert: got %v, want %v", after[0].CreatedAt, createdAt)
	}
	if after[0].Category != "client" {
		t.Fatalf("Category after upsert: got %q, want %q", after[0].Category, "client")
	}
	if after[0].Source != "operator" {
		t.Fatalf("Source after upsert: got %q, want %q", after[0].Source, "operator")
	}
	if after[0].Priority != 25 {
		t.Fatalf("Priority after upsert: got %d, want %d", after[0].Priority, 25)
	}
}

func TestDeleteSeed(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	if err := st.InsertSeed(ctx, globalSeed("domain", "example.com", "vendor")); err != nil {
		t.Fatal(err)
	}
	seeds, err := st.ListSeeds(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(seeds) != 1 {
		t.Fatalf("expected 1 seed before delete")
	}

	if err := st.DeleteSeed(ctx, seeds[0].ID); err != nil {
		t.Fatalf("DeleteSeed: %v", err)
	}

	after, err := st.ListSeeds(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != 0 {
		t.Errorf("expected 0 seeds after delete, got %d", len(after))
	}
}

func TestInsertSeed_ClosedStore(t *testing.T) {
	st := newTestStore(t)
	_ = st.Close()

	err := st.InsertSeed(context.Background(), globalSeed("domain", "example.com", "vendor"))
	if err == nil {
		t.Fatal("expected error from closed store")
	}
}

func TestUpsertSeed_ClosedStore(t *testing.T) {
	st := newTestStore(t)
	_ = st.Close()

	err := st.UpsertSeed(context.Background(), globalSeed("domain", "example.com", "vendor"))
	if err == nil {
		t.Fatal("expected error from closed store")
	}
}

func TestListSeeds_ClosedStore(t *testing.T) {
	st := newTestStore(t)
	_ = st.Close()

	_, err := st.ListSeeds(context.Background(), "")
	if err == nil {
		t.Fatal("expected error from closed store")
	}
}

func TestDeleteSeed_ClosedStore(t *testing.T) {
	st := newTestStore(t)
	_ = st.Close()

	err := st.DeleteSeed(context.Background(), 1)
	if err == nil {
		t.Fatal("expected error from closed store")
	}
}

func TestListSeeds_InvalidCreatedAt(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()

	if _, err := st.db.ExecContext(ctx,
		`INSERT INTO classification_seeds (mailbox_id, pattern_type, pattern_value, category, source, priority, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		nil, "domain", "example.com", "vendor", "seed", 100, "not-valid-rfc3339",
	); err != nil {
		t.Fatal(err)
	}

	_, err := st.ListSeeds(ctx, "")
	if err == nil {
		t.Fatal("expected parse error for invalid seed created_at")
	}
	if !strings.Contains(err.Error(), "parse seed created_at") {
		t.Fatalf("expected parse seed created_at error, got %v", err)
	}
}

// --- Classification save/get ---

func TestSaveAndGetClassification(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	createTestMailbox(t, st, "user@example.com")
	seedMessage(t, st, "msg1", "user@example.com", "a@x.com", "", "x.com", "", time.Now())

	now := time.Now().UTC().Truncate(time.Second)
	c := Classification{
		MessageID:    "msg1",
		MailboxID:    "user@example.com",
		Category:     "vendor",
		Intent:       "invoice",
		MatchedRule:  "domain:x.com",
		Source:       "seed",
		ClassifiedAt: now,
	}
	if err := st.SaveClassification(ctx, c); err != nil {
		t.Fatalf("SaveClassification: %v", err)
	}

	got, err := st.GetClassification(ctx, "msg1", "user@example.com")
	if err != nil {
		t.Fatalf("GetClassification: %v", err)
	}
	if got == nil {
		t.Fatal("expected classification, got nil")
		return
	}
	if got.Category != "vendor" {
		t.Errorf("Category: got %q, want %q", got.Category, "vendor")
	}
	if got.MatchedRule != "domain:x.com" {
		t.Errorf("MatchedRule: got %q, want %q", got.MatchedRule, "domain:x.com")
	}
	if got.Intent != "invoice" {
		t.Errorf("Intent: got %q, want %q", got.Intent, "invoice")
	}
	if !got.ClassifiedAt.Equal(now) {
		t.Errorf("ClassifiedAt: got %v, want %v", got.ClassifiedAt, now)
	}
}

func TestSaveClassification_ReplaceSemantics(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	createTestMailbox(t, st, "user@example.com")
	seedMessage(t, st, "msg1", "user@example.com", "a@x.com", "", "x.com", "", time.Now())

	now := time.Now().UTC().Truncate(time.Second)
	c := Classification{MessageID: "msg1", MailboxID: "user@example.com", Category: "vendor", Source: "seed", ClassifiedAt: now}
	if err := st.SaveClassification(ctx, c); err != nil {
		t.Fatal(err)
	}
	// Re-save with different category (INSERT OR REPLACE)
	c.Category = "client"
	if err := st.SaveClassification(ctx, c); err != nil {
		t.Fatal(err)
	}

	got, err := st.GetClassification(ctx, "msg1", "user@example.com")
	if err != nil {
		t.Fatal(err)
	}
	if got.Category != "client" {
		t.Errorf("Category after replace: got %q, want %q", got.Category, "client")
	}
}

func TestGetClassification_NotFound(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	createTestMailbox(t, st, "user@example.com")

	got, err := st.GetClassification(ctx, "nosuch", "user@example.com")
	if err != nil {
		t.Fatalf("GetClassification: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil for missing classification, got %+v", got)
	}
}

func TestSaveClassification_ClosedStore(t *testing.T) {
	st := newTestStore(t)
	_ = st.Close()

	err := st.SaveClassification(context.Background(), Classification{
		MessageID:    "msg1",
		MailboxID:    "user@example.com",
		Category:     "vendor",
		Source:       "seed",
		ClassifiedAt: time.Now(),
	})
	if err == nil {
		t.Fatal("expected error from closed store")
	}
}

func TestGetClassification_ClosedStore(t *testing.T) {
	st := newTestStore(t)
	_ = st.Close()

	_, err := st.GetClassification(context.Background(), "msg1", "user@example.com")
	if err == nil {
		t.Fatal("expected error from closed store")
	}
}

func TestGetClassification_InvalidClassifiedAt(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	createTestMailbox(t, st, "user@example.com")
	seedMessage(t, st, "msg1", "user@example.com", "a@x.com", "", "x.com", "", time.Now())

	if _, err := st.db.ExecContext(ctx,
		`INSERT INTO message_classifications (message_id, mailbox_id, category, intent, matched_rule, source, classified_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		"msg1", "user@example.com", "vendor", "", "domain:x.com", "seed", "not-valid-rfc3339",
	); err != nil {
		t.Fatal(err)
	}

	_, err := st.GetClassification(ctx, "msg1", "user@example.com")
	if err == nil {
		t.Fatal("expected parse error for invalid classified_at")
	}
	if !strings.Contains(err.Error(), "parse classification classified_at") {
		t.Fatalf("expected parse classification classified_at error, got %v", err)
	}
}

func TestBulkSaveClassifications(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	createTestMailbox(t, st, "user@example.com")
	now := time.Now()

	// Seed multiple messages
	for _, id := range []string{"m1", "m2", "m3"} {
		seedMessage(t, st, id, "user@example.com", "a@x.com", "", "x.com", "", now)
	}

	classifications := []Classification{
		{MessageID: "m1", MailboxID: "user@example.com", Category: "vendor", Intent: "invoice", Source: "seed", ClassifiedAt: now},
		{MessageID: "m2", MailboxID: "user@example.com", Category: "client", Source: "seed", ClassifiedAt: now},
		{MessageID: "m3", MailboxID: "user@example.com", Category: "social", Source: "seed", ClassifiedAt: now},
	}
	if err := st.BulkSaveClassifications(ctx, classifications); err != nil {
		t.Fatalf("BulkSaveClassifications: %v", err)
	}

	for _, c := range classifications {
		got, err := st.GetClassification(ctx, c.MessageID, c.MailboxID)
		if err != nil {
			t.Fatalf("GetClassification(%q): %v", c.MessageID, err)
		}
		if got == nil {
			t.Fatalf("expected classification for %q, got nil", c.MessageID)
			return
		}
		if got.Category != c.Category {
			t.Errorf("%q: Category: got %q, want %q", c.MessageID, got.Category, c.Category)
		}
		if got.Intent != c.Intent {
			t.Errorf("%q: Intent: got %q, want %q", c.MessageID, got.Intent, c.Intent)
		}
	}
}

func TestBulkSaveClassifications_AtomicRollbackOnError(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	createTestMailbox(t, st, "user@example.com")
	now := time.Now()
	seedMessage(t, st, "m1", "user@example.com", "a@x.com", "", "x.com", "", now)

	classifications := []Classification{
		{MessageID: "m1", MailboxID: "user@example.com", Category: "vendor", Source: "seed", ClassifiedAt: now},
		{MessageID: "nonexistent", MailboxID: "user@example.com", Category: "client", Source: "seed", ClassifiedAt: now},
	}

	err := st.BulkSaveClassifications(ctx, classifications)
	if err == nil {
		t.Fatal("expected foreign key failure from invalid message_id")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "foreign key") {
		t.Fatalf("expected foreign key error, got %v", err)
	}

	got, err := st.GetClassification(ctx, "m1", "user@example.com")
	if err != nil {
		t.Fatalf("GetClassification after rollback: %v", err)
	}
	if got != nil {
		t.Fatal("expected rollback: valid row should not be persisted after FK failure")
	}
}

func TestBulkSaveClassifications_ClosedStore(t *testing.T) {
	st := newTestStore(t)
	_ = st.Close()

	err := st.BulkSaveClassifications(context.Background(), []Classification{
		{MessageID: "m1", MailboxID: "user@example.com", Category: "vendor", Source: "seed", ClassifiedAt: time.Now()},
	})
	if err == nil {
		t.Fatal("expected error from closed store")
	}
}

func TestQueryClassificationsByMailbox(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	createTestMailbox(t, st, "user@example.com")
	createTestMailbox(t, st, "other@example.com")

	now := time.Now().UTC()
	for _, id := range []string{"m1", "m2", "m3", "m4", "m5"} {
		seedMessage(t, st, id, "user@example.com", "a@x.com", "", "x.com", "", now)
	}
	seedMessage(t, st, "m6", "other@example.com", "b@y.com", "", "y.com", "", now)

	for _, c := range []Classification{
		{MessageID: "m1", MailboxID: "user@example.com", Category: "unknown", Source: "seed", ClassifiedAt: now},
		{MessageID: "m2", MailboxID: "user@example.com", Category: "vendor", Intent: "invoice", Source: "seed", ClassifiedAt: now},
		{MessageID: "m3", MailboxID: "user@example.com", Category: "vendor", Source: "seed", ClassifiedAt: now},
		{MessageID: "m4", MailboxID: "user@example.com", Category: "client", Source: "seed", ClassifiedAt: now},
		{MessageID: "m5", MailboxID: "user@example.com", Category: "client", Source: "seed", ClassifiedAt: now},
		{MessageID: "m6", MailboxID: "other@example.com", Category: "social", Source: "seed", ClassifiedAt: now},
	} {
		if err := st.SaveClassification(ctx, c); err != nil {
			t.Fatalf("SaveClassification(%s): %v", c.MessageID, err)
		}
	}

	counts, err := st.QueryClassificationsByMailbox(ctx, "user@example.com")
	if err != nil {
		t.Fatalf("QueryClassificationsByMailbox: %v", err)
	}
	if len(counts) != 4 {
		t.Fatalf("expected 4 category rows, got %d", len(counts))
	}
	if counts[0] != (ClassificationCount{Category: "client", Count: 2}) {
		t.Fatalf("first row: got %+v", counts[0])
	}
	if counts[1] != (ClassificationCount{Category: "unknown", Count: 1}) {
		t.Fatalf("second row: got %+v", counts[1])
	}
	if counts[2] != (ClassificationCount{Category: "vendor", Count: 1}) {
		t.Fatalf("third row: got %+v", counts[2])
	}
	if counts[3] != (ClassificationCount{Category: "vendor", Intent: "invoice", Count: 1}) {
		t.Fatalf("fourth row: got %+v", counts[3])
	}
}

func TestQueryClassificationsByMailbox_Empty(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	createTestMailbox(t, st, "user@example.com")

	counts, err := st.QueryClassificationsByMailbox(ctx, "user@example.com")
	if err != nil {
		t.Fatalf("QueryClassificationsByMailbox: %v", err)
	}
	if len(counts) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(counts))
	}
}

func TestSaveAndListInferenceSuggestions(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	createTestMailbox(t, st, "user@example.com")
	seedMessage(t, st, "msg1", "user@example.com", "a@x.com", "", "x.com", "Invoice", time.Now())

	if err := st.SaveInferenceCandidate(ctx, InferenceSuggestion{
		MailboxID:      "user@example.com",
		MessageID:      "msg1",
		PatternType:    "domain",
		PatternValue:   "x.com",
		Category:       "client",
		Confidence:     0.81,
		ConfidenceBand: "high",
		Evidence: InferenceEvidence{
			SubjectPhrases: []string{"invoice"},
		},
		ReviewRequired: false,
	}); err != nil {
		t.Fatalf("SaveInferenceCandidate: %v", err)
	}

	got, err := st.ListInferenceSuggestions(ctx, "user@example.com")
	if err != nil {
		t.Fatalf("ListInferenceSuggestions: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 suggestion, got %d", len(got))
	}
	if got[0].PatternValue != "x.com" || got[0].Category != "client" || got[0].CreatedAt.IsZero() {
		t.Fatalf("unexpected suggestion: %+v", got[0])
	}
}

func TestSaveInferenceCandidate_UpsertByMailboxAndMessage(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	createTestMailbox(t, st, "user@example.com")
	seedMessage(t, st, "msg1", "user@example.com", "a@x.com", "", "x.com", "Invoice", time.Now())

	first := InferenceSuggestion{
		MailboxID:      "user@example.com",
		MessageID:      "msg1",
		PatternType:    "domain",
		PatternValue:   "x.com",
		Category:       "client",
		Confidence:     0.81,
		ConfidenceBand: "high",
		ReviewRequired: false,
	}
	if err := st.SaveInferenceCandidate(ctx, first); err != nil {
		t.Fatalf("SaveInferenceCandidate first: %v", err)
	}
	before, err := st.ListInferenceSuggestions(ctx, "user@example.com")
	if err != nil {
		t.Fatalf("ListInferenceSuggestions before: %v", err)
	}

	if err := st.SaveInferenceCandidate(ctx, InferenceSuggestion{
		MailboxID:      "user@example.com",
		MessageID:      "msg1",
		PatternType:    "sender_email",
		PatternValue:   "a@x.com",
		Category:       "vendor",
		Confidence:     0.61,
		ConfidenceBand: "medium",
		ReviewRequired: true,
	}); err != nil {
		t.Fatalf("SaveInferenceCandidate second: %v", err)
	}
	after, err := st.ListInferenceSuggestions(ctx, "user@example.com")
	if err != nil {
		t.Fatalf("ListInferenceSuggestions after: %v", err)
	}
	if len(after) != 1 {
		t.Fatalf("expected 1 suggestion after upsert, got %d", len(after))
	}
	if !after[0].CreatedAt.Equal(before[0].CreatedAt) {
		t.Fatalf("CreatedAt changed after upsert: before=%v after=%v", before[0].CreatedAt, after[0].CreatedAt)
	}
	if after[0].PatternType != "sender_email" || after[0].Category != "vendor" || !after[0].ReviewRequired {
		t.Fatalf("unexpected updated suggestion: %+v", after[0])
	}
}

func TestDeleteInferenceSuggestion(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	createTestMailbox(t, st, "user@example.com")
	seedMessage(t, st, "msg1", "user@example.com", "a@x.com", "", "x.com", "Invoice", time.Now())

	if err := st.SaveInferenceCandidate(ctx, InferenceSuggestion{
		MailboxID:      "user@example.com",
		MessageID:      "msg1",
		PatternType:    "domain",
		PatternValue:   "x.com",
		Category:       "client",
		Confidence:     0.81,
		ConfidenceBand: "high",
	}); err != nil {
		t.Fatalf("SaveInferenceCandidate: %v", err)
	}
	if err := st.DeleteInferenceSuggestion(ctx, "user@example.com", "msg1"); err != nil {
		t.Fatalf("DeleteInferenceSuggestion: %v", err)
	}
	got, err := st.ListInferenceSuggestions(ctx, "user@example.com")
	if err != nil {
		t.Fatalf("ListInferenceSuggestions: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected no inference suggestions, got %d", len(got))
	}
}

func TestListInferenceSuggestions_Empty(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	createTestMailbox(t, st, "user@example.com")

	got, err := st.ListInferenceSuggestions(ctx, "user@example.com")
	if err != nil {
		t.Fatalf("ListInferenceSuggestions: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected no inference suggestions, got %d", len(got))
	}
}

func TestInferenceSuggestionMethods_ClosedStore(t *testing.T) {
	st := newTestStore(t)
	_ = st.Close()

	if err := st.SaveInferenceCandidate(context.Background(), InferenceSuggestion{
		MailboxID:      "user@example.com",
		MessageID:      "msg1",
		PatternType:    "domain",
		PatternValue:   "x.com",
		Category:       "client",
		Confidence:     0.81,
		ConfidenceBand: "high",
	}); err == nil {
		t.Fatal("expected SaveInferenceCandidate error from closed store")
	}
	if _, err := st.ListInferenceSuggestions(context.Background(), "user@example.com"); err == nil {
		t.Fatal("expected ListInferenceSuggestions error from closed store")
	}
	if err := st.DeleteInferenceSuggestion(context.Background(), "user@example.com", "msg1"); err == nil {
		t.Fatal("expected DeleteInferenceSuggestion error from closed store")
	}
}

func TestListInferenceSuggestions_InvalidCreatedAt(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	createTestMailbox(t, st, "user@example.com")
	seedMessage(t, st, "msg1", "user@example.com", "a@x.com", "", "x.com", "Invoice", time.Now())

	if _, err := st.db.ExecContext(ctx,
		`INSERT INTO ai_inference_suggestions (mailbox_id, message_id, pattern_type, pattern_value, category, confidence, confidence_band, evidence_json, review_required, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"user@example.com", "msg1", "domain", "x.com", "client", 0.81, "high", `{}`, 0, "not-valid-rfc3339",
	); err != nil {
		t.Fatal(err)
	}

	if _, err := st.ListInferenceSuggestions(ctx, "user@example.com"); err == nil {
		t.Fatal("expected parse error for invalid inference suggestion created_at")
	}
}
