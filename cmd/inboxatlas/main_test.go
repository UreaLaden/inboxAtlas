package main

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/UreaLaden/inboxatlas/internal/config"
	"github.com/UreaLaden/inboxatlas/internal/storage"
	"github.com/UreaLaden/inboxatlas/pkg/models"
)

// openMemStore opens an in-memory store and registers a cleanup.
func openMemStore(t *testing.T) *storage.Store {
	t.Helper()
	st, err := storage.Open(":memory:")
	if err != nil {
		t.Fatalf("open :memory: store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// --- initLogger ---

func TestInitLogger(t *testing.T) {
	for _, level := range []string{"debug", "info", "warn", "error", "unknown"} {
		initLogger(level) // must not panic
	}
}

// --- buildRoot ---

func TestBuildRoot_Use(t *testing.T) {
	root := buildRoot(config.Default())
	if root.Use != "inboxatlas" {
		t.Errorf("Use: got %q, want %q", root.Use, "inboxatlas")
	}
}

func TestBuildRoot_HasSubcommands(t *testing.T) {
	root := buildRoot(config.Default())
	names := make(map[string]bool)
	for _, cmd := range root.Commands() {
		names[cmd.Name()] = true
	}
	for _, want := range []string{"version", "config", "mailbox"} {
		if !names[want] {
			t.Errorf("expected subcommand %q to be registered", want)
		}
	}
}

// --- buildVersionCmd ---

func TestBuildVersionCmd(t *testing.T) {
	cmd := buildVersionCmd()
	if cmd.Use != "version" {
		t.Errorf("Use: got %q, want %q", cmd.Use, "version")
	}
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !strings.Contains(buf.String(), "0.1.0") {
		t.Errorf("expected version in output, got %q", buf.String())
	}
}

// --- buildConfigShowCmd ---

func TestBuildConfigShowCmd(t *testing.T) {
	cfg := config.Default()
	cmd := buildConfigShowCmd(cfg)

	var buf bytes.Buffer
	cmd.SetOut(&buf)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	out := buf.String()
	for _, want := range []string{"storage_path", "log_level", "token_dir", "default_provider", "credentials_path"} {
		if !strings.Contains(out, want) {
			t.Errorf("expected %q in config show output", want)
		}
	}
}

// --- runMailboxList ---

func TestRunMailboxList_Empty(t *testing.T) {
	st := openMemStore(t)
	var buf bytes.Buffer
	if err := runMailboxList(&buf, st); err != nil {
		t.Fatalf("runMailboxList: %v", err)
	}
	if !strings.Contains(buf.String(), "No mailboxes registered") {
		t.Errorf("expected empty message, got: %q", buf.String())
	}
}

func TestRunMailboxList_WithMailboxes(t *testing.T) {
	st := openMemStore(t)
	ctx := context.Background()

	mbs := []models.Mailbox{
		{ID: "work@company.com", Alias: "work", Provider: "gmail"},
		{ID: "personal@example.com", Alias: "personal", Provider: "gmail"},
	}
	for _, mb := range mbs {
		if err := st.CreateMailbox(ctx, mb); err != nil {
			t.Fatalf("CreateMailbox: %v", err)
		}
	}

	var buf bytes.Buffer
	if err := runMailboxList(&buf, st); err != nil {
		t.Fatalf("runMailboxList: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "work@company.com") {
		t.Error("expected work@company.com in output")
	}
	if !strings.Contains(out, "personal") {
		t.Error("expected alias 'personal' in output")
	}
	if !strings.Contains(out, "never") {
		t.Error("expected 'never' for unsynced mailbox")
	}
}

func TestRunMailboxList_WithLastSynced(t *testing.T) {
	st := openMemStore(t)
	ctx := context.Background()

	if err := st.CreateMailbox(ctx, models.Mailbox{ID: "user@company.com", Provider: "gmail"}); err != nil {
		t.Fatal(err)
	}
	// Use a fixed time so the output is deterministic.
	// storage.UpdateLastSynced stores RFC3339; we just verify it's not "never".
	if err := st.UpdateLastSynced(ctx, "user@company.com", fixedTime()); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := runMailboxList(&buf, st); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(buf.String(), "never") {
		t.Error("expected a real timestamp, not 'never'")
	}
}

// --- runMailboxRemove ---

func TestRunMailboxRemove_Force(t *testing.T) {
	st := openMemStore(t)
	ctx := context.Background()
	if err := st.CreateMailbox(ctx, models.Mailbox{ID: "user@company.com", Alias: "work", Provider: "gmail"}); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := runMailboxRemove(&buf, nil, st, "work", true); err != nil {
		t.Fatalf("runMailboxRemove --force: %v", err)
	}
	if !strings.Contains(buf.String(), "Mailbox removed.") {
		t.Errorf("expected removed message, got %q", buf.String())
	}

	mb, _ := st.GetMailbox(ctx, "user@company.com")
	if mb != nil {
		t.Error("mailbox should be deleted")
	}
}

func TestRunMailboxRemove_Confirm_Yes(t *testing.T) {
	st := openMemStore(t)
	ctx := context.Background()
	if err := st.CreateMailbox(ctx, models.Mailbox{ID: "user@company.com", Alias: "work", Provider: "gmail"}); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := runMailboxRemove(&buf, strings.NewReader("y\n"), st, "work", false); err != nil {
		t.Fatalf("runMailboxRemove confirm y: %v", err)
	}
	if !strings.Contains(buf.String(), "Mailbox removed.") {
		t.Errorf("expected removed message, got %q", buf.String())
	}
}

func TestRunMailboxRemove_Confirm_No(t *testing.T) {
	st := openMemStore(t)
	ctx := context.Background()
	if err := st.CreateMailbox(ctx, models.Mailbox{ID: "user@company.com", Alias: "work", Provider: "gmail"}); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := runMailboxRemove(&buf, strings.NewReader("n\n"), st, "work", false); err != nil {
		t.Fatalf("runMailboxRemove confirm n: %v", err)
	}
	if !strings.Contains(buf.String(), "Cancelled.") {
		t.Errorf("expected cancelled message, got %q", buf.String())
	}

	mb, _ := st.GetMailbox(ctx, "user@company.com")
	if mb == nil {
		t.Error("mailbox should NOT be deleted after cancellation")
	}
}

func TestRunMailboxRemove_NotFound(t *testing.T) {
	st := openMemStore(t)
	var buf bytes.Buffer
	err := runMailboxRemove(&buf, nil, st, "nobody", true)
	if err == nil {
		t.Error("expected error for non-existent mailbox")
	}
}

func TestRunMailboxRemove_ByEmail(t *testing.T) {
	st := openMemStore(t)
	ctx := context.Background()
	if err := st.CreateMailbox(ctx, models.Mailbox{ID: "user@company.com", Provider: "gmail"}); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := runMailboxRemove(&buf, nil, st, "user@company.com", true); err != nil {
		t.Fatalf("runMailboxRemove by email: %v", err)
	}
	if !strings.Contains(buf.String(), "Mailbox removed.") {
		t.Errorf("expected removed message, got %q", buf.String())
	}
}

// --- buildMailboxListCmd (RunE integration) ---

func TestBuildMailboxListCmd_RunE_OpenError(t *testing.T) {
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(t.TempDir(), "nonexistent", "test.db")

	cmd := buildMailboxListCmd(cfg)
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	if err := cmd.Execute(); err == nil {
		t.Error("expected error for bad storage path")
	}
}

func TestBuildMailboxRemoveCmd_RunE_OpenError(t *testing.T) {
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(t.TempDir(), "nonexistent", "test.db")

	cmd := buildMailboxRemoveCmd(cfg)
	cmd.SetArgs([]string{"--account", "user@example.com", "--force"})
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	if err := cmd.Execute(); err == nil {
		t.Error("expected error for bad storage path")
	}
}

func TestRunMailboxList_StoreClosed(t *testing.T) {
	st := openMemStore(t)
	_ = st.Close()
	var buf bytes.Buffer
	if err := runMailboxList(&buf, st); err == nil {
		t.Error("expected error with closed store")
	}
}

func TestBuildMailboxListCmd_RunE_Empty(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")

	cmd := buildMailboxListCmd(cfg)
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !strings.Contains(buf.String(), "No mailboxes registered") {
		t.Errorf("expected empty message, got: %q", buf.String())
	}
}

func TestBuildMailboxRemoveCmd_RunE_Force(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	st, err := storage.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.CreateMailbox(context.Background(), models.Mailbox{ID: "user@example.com", Provider: "gmail"}); err != nil {
		t.Fatalf("CreateMailbox: %v", err)
	}
	_ = st.Close()

	cfg := config.Default()
	cfg.StoragePath = dbPath

	cmd := buildMailboxRemoveCmd(cfg)
	cmd.SetArgs([]string{"--account", "user@example.com", "--force"})
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !strings.Contains(buf.String(), "Mailbox removed.") {
		t.Errorf("expected removed message, got: %q", buf.String())
	}
}

// fixedTime returns a deterministic time for use in tests.
func fixedTime() time.Time {
	return time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)
}
