package storage

import (
	"context"
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

func TestOpen(t *testing.T) {
	st := newTestStore(t)
	if st == nil {
		t.Fatal("expected non-nil store")
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
	}
	if got.ID != "user@company.com" {
		t.Errorf("ID should be lowercase, got %q", got.ID)
	}
}
