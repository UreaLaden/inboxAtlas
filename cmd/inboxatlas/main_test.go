package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/oauth2"

	"github.com/UreaLaden/inboxatlas/internal/auth"
	"github.com/UreaLaden/inboxatlas/internal/config"
	"github.com/UreaLaden/inboxatlas/internal/ingestion"
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
	for _, want := range []string{"version", "config", "mailbox", "auth", "sync"} {
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

// --- buildAuthCmd ---

func TestBuildAuthCmd_HasGmailSubcommand(t *testing.T) {
	cmd := buildAuthCmd(config.Default())
	names := make(map[string]bool)
	for _, sub := range cmd.Commands() {
		names[sub.Name()] = true
	}
	if !names["gmail"] {
		t.Error("expected 'gmail' subcommand under 'auth'")
	}
}

// --- buildAuthGmailCmd ---

func TestBuildAuthGmailCmd_MissingAccount(t *testing.T) {
	cmd := buildAuthGmailCmd(config.Default())
	cmd.SetArgs([]string{}) // no --account flag
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	if err := cmd.Execute(); err == nil {
		t.Error("expected error when --account is missing")
	}
}

func TestBuildAuthGmailCmd_DelegatedFlag(t *testing.T) {
	cmd := buildAuthGmailCmd(config.Default())
	if cmd.Flags().Lookup("delegated") == nil {
		t.Fatal("expected --delegated flag")
	}
}

// --- runAuthGmail ---

func TestRunAuthGmail_MissingCredentials(t *testing.T) {
	cfg := config.Default()
	cfg.CredentialsPath = filepath.Join(t.TempDir(), "nonexistent.json")
	var buf bytes.Buffer
	err := runAuthGmail(context.Background(), &buf, cfg, "user@example.com", "")
	if err == nil {
		t.Fatal("expected error for missing credentials file")
	}
	if !strings.Contains(err.Error(), "credentials file not found") {
		t.Errorf("expected 'credentials file not found' in error, got: %v", err)
	}
}

// --- runAuthGmailWithFlow ---

func mockFlow(token *oauth2.Token, err error) func(context.Context, *oauth2.Config, io.Writer) (*oauth2.Token, error) {
	return func(_ context.Context, _ *oauth2.Config, _ io.Writer) (*oauth2.Token, error) {
		return token, err
	}
}

func TestRunAuthGmailWithFlow_Success(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.TokenDir = filepath.Join(dir, "tokens")
	cfg.StoragePath = filepath.Join(dir, "test.db")

	tok := &oauth2.Token{AccessToken: "test-access", RefreshToken: "test-refresh"}
	var buf bytes.Buffer
	err := runAuthGmailWithFlow(context.Background(), &buf, cfg, "user@example.com", "work", &oauth2.Config{}, mockFlow(tok, nil))
	if err != nil {
		t.Fatalf("runAuthGmailWithFlow: %v", err)
	}
	if !strings.Contains(buf.String(), "user@example.com") {
		t.Errorf("expected email in output, got: %q", buf.String())
	}
	if !strings.Contains(buf.String(), "registered") {
		t.Errorf("expected 'registered' in output, got: %q", buf.String())
	}
}

func TestRunAuthGmailWithFlow_AlreadyRegistered(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.TokenDir = filepath.Join(dir, "tokens")
	cfg.StoragePath = filepath.Join(dir, "test.db")

	// Pre-register the mailbox.
	st, err := storage.Open(cfg.StoragePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.CreateMailbox(context.Background(), models.Mailbox{ID: "user@example.com", Provider: "gmail"}); err != nil {
		t.Fatal(err)
	}
	_ = st.Close()

	tok := &oauth2.Token{AccessToken: "test-access"}
	var buf bytes.Buffer
	err = runAuthGmailWithFlow(context.Background(), &buf, cfg, "user@example.com", "", &oauth2.Config{}, mockFlow(tok, nil))
	if err != nil {
		t.Fatalf("expected no error for already-registered mailbox: %v", err)
	}
}

func TestRunAuthGmailWithFlow_FlowError(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.TokenDir = filepath.Join(dir, "tokens")
	cfg.StoragePath = filepath.Join(dir, "test.db")

	var buf bytes.Buffer
	err := runAuthGmailWithFlow(context.Background(), &buf, cfg, "user@example.com", "", &oauth2.Config{}, mockFlow(nil, fmt.Errorf("flow failed")))
	if err == nil {
		t.Error("expected error when flow fails")
	}
}

func TestRunAuthGmailWithFlow_SaveTokenError(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")
	cfg.TokenStorage = "file" // use file storage so blocked dir causes an error
	// Block token directory: create a file where SaveToken would create a dir.
	tokenFile := filepath.Join(dir, "tokens")
	if err := os.WriteFile(tokenFile, []byte("block"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg.TokenDir = tokenFile // file, not a directory — MkdirAll will fail

	tok := &oauth2.Token{AccessToken: "test"}
	var buf bytes.Buffer
	err := runAuthGmailWithFlow(context.Background(), &buf, cfg, "user@example.com", "", &oauth2.Config{}, mockFlow(tok, nil))
	if err == nil {
		t.Error("expected error when token cannot be saved")
	}
}

func TestRunAuthGmailWithFlow_StorageOpenError(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.TokenDir = filepath.Join(dir, "tokens")
	cfg.StoragePath = filepath.Join(dir, "nonexistent", "test.db")

	tok := &oauth2.Token{AccessToken: "test-access"}
	var buf bytes.Buffer
	err := runAuthGmailWithFlow(context.Background(), &buf, cfg, "user@example.com", "", &oauth2.Config{}, mockFlow(tok, nil))
	if err == nil {
		t.Error("expected error for bad storage path")
	}
}

func TestRunAuthGmail_MalformedCredentials(t *testing.T) {
	dir := t.TempDir()
	credPath := filepath.Join(dir, "credentials.json")
	if err := os.WriteFile(credPath, []byte("not-json"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.CredentialsPath = credPath
	var buf bytes.Buffer
	err := runAuthGmail(context.Background(), &buf, cfg, "user@example.com", "")
	if err == nil {
		t.Fatal("expected error for malformed credentials")
	}
}

func TestRunAuthGmailDelegated_Success(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")
	cfg.CredentialsPath = filepath.Join(dir, "service-account.json")
	if err := os.WriteFile(cfg.CredentialsPath, []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}

	orig := validateGmailDelegation
	validateGmailDelegation = func(context.Context, string, string) error { return nil }
	t.Cleanup(func() { validateGmailDelegation = orig })

	var buf bytes.Buffer
	if err := runAuthGmailDelegated(context.Background(), &buf, cfg, "User@Example.com", "work"); err != nil {
		t.Fatalf("runAuthGmailDelegated: %v", err)
	}
	if !strings.Contains(buf.String(), "Delegation validated successfully") {
		t.Fatalf("unexpected output: %q", buf.String())
	}
}

func TestRunAuthGmailDelegated_ValidationError(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")
	cfg.CredentialsPath = filepath.Join(dir, "service-account.json")
	if err := os.WriteFile(cfg.CredentialsPath, []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}

	orig := validateGmailDelegation
	validateGmailDelegation = func(context.Context, string, string) error { return fmt.Errorf("delegation failed") }
	t.Cleanup(func() { validateGmailDelegation = orig })

	err := runAuthGmailDelegated(context.Background(), io.Discard, cfg, "user@example.com", "")
	if err == nil {
		t.Fatal("expected delegated validation error")
	}
}

func TestRunAuthGmailWithFlow_UsesProvidedContext(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.TokenDir = filepath.Join(dir, "tokens")
	cfg.StoragePath = filepath.Join(dir, "test.db")

	type contextKey string
	wantCtx := context.WithValue(context.Background(), contextKey("marker"), "ctx-marker")
	tok := &oauth2.Token{AccessToken: "test-access"}

	var gotCtx context.Context
	err := runAuthGmailWithFlow(wantCtx, io.Discard, cfg, "user@example.com", "", &oauth2.Config{}, func(ctx context.Context, _ *oauth2.Config, _ io.Writer) (*oauth2.Token, error) {
		gotCtx = ctx
		return tok, nil
	})
	if err != nil {
		t.Fatalf("runAuthGmailWithFlow: %v", err)
	}
	if gotCtx != wantCtx {
		t.Error("expected flow to receive the provided context")
	}
}

func TestRunAuthGmailWithFlow_CanonicalizesAccount(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.TokenDir = filepath.Join(dir, "tokens")
	cfg.StoragePath = filepath.Join(dir, "test.db")
	cfg.TokenStorage = "file" // use file storage so token path can be verified on disk

	tok := &oauth2.Token{AccessToken: "test-access", RefreshToken: "test-refresh"}
	var buf bytes.Buffer
	err := runAuthGmailWithFlow(context.Background(), &buf, cfg, "User@Example.com", "work", &oauth2.Config{}, mockFlow(tok, nil))
	if err != nil {
		t.Fatalf("runAuthGmailWithFlow: %v", err)
	}
	if !strings.Contains(buf.String(), "user@example.com") {
		t.Errorf("expected canonical email in output, got %q", buf.String())
	}

	st, err := storage.Open(cfg.StoragePath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = st.Close() }()

	mb, err := st.GetMailbox(context.Background(), "user@example.com")
	if err != nil {
		t.Fatalf("GetMailbox: %v", err)
	}
	if mb == nil {
		t.Fatal("expected canonical mailbox record to be stored")
	}

	if _, err := os.Stat(auth.TokenPath(cfg.TokenDir, "gmail", "user@example.com")); err != nil {
		t.Fatalf("expected token file for canonical email: %v", err)
	}
}

// --- buildConfigShowCmd — new fields ---

func TestBuildConfigShowCmd_IncludesNewFields(t *testing.T) {
	cfg := config.Default()
	cmd := buildConfigShowCmd(cfg)

	var buf bytes.Buffer
	cmd.SetOut(&buf)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	out := buf.String()
	for _, want := range []string{"token_storage", "sync_delay_ms"} {
		if !strings.Contains(out, want) {
			t.Errorf("expected %q in config show output", want)
		}
	}
}

// --- buildSyncCmd ---

func TestBuildSyncCmd_Subcommands(t *testing.T) {
	cmd := buildSyncCmd(config.Default())
	names := make(map[string]bool)
	for _, sub := range cmd.Commands() {
		names[sub.Name()] = true
	}
	for _, want := range []string{"gmail", "status"} {
		if !names[want] {
			t.Errorf("expected subcommand %q under sync", want)
		}
	}
}

func TestBuildSyncGmailCmd_RequiresAccount(t *testing.T) {
	cmd := buildSyncGmailCmd(config.Default())
	cmd.SetArgs([]string{}) // no --account
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	if err := cmd.Execute(); err == nil {
		t.Error("expected error when --account is missing")
	}
}

func TestBuildSyncStatusCmd_RequiresAccount(t *testing.T) {
	cmd := buildSyncStatusCmd(config.Default())
	cmd.SetArgs([]string{}) // no --account
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	if err := cmd.Execute(); err == nil {
		t.Error("expected error when --account is missing")
	}
}

// --- runSyncStatus ---

func TestRunSyncStatus_NoCheckpoint(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	st, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.CreateMailbox(context.Background(), models.Mailbox{ID: "user@example.com", Provider: "gmail"}); err != nil {
		t.Fatal(err)
	}
	_ = st.Close()

	cfg := config.Default()
	cfg.StoragePath = dbPath

	var buf bytes.Buffer
	if err := runSyncStatus(&buf, cfg, "user@example.com"); err != nil {
		t.Fatalf("runSyncStatus: %v", err)
	}
	if !strings.Contains(buf.String(), "No sync checkpoint found") {
		t.Errorf("expected 'No sync checkpoint found', got: %q", buf.String())
	}
}

func TestRunSyncStatus_WithCheckpoint(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	st, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := st.CreateMailbox(ctx, models.Mailbox{ID: "user@example.com", Provider: "gmail"}); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)
	_ = st.SaveCheckpoint(ctx, storage.SyncCheckpoint{
		MailboxID:      "user@example.com",
		Provider:       "gmail",
		MessagesSynced: 42,
		Status:         "completed",
		StartedAt:      now,
		UpdatedAt:      now,
	})
	_ = st.Close()

	cfg := config.Default()
	cfg.StoragePath = dbPath

	var buf bytes.Buffer
	if err := runSyncStatus(&buf, cfg, "user@example.com"); err != nil {
		t.Fatalf("runSyncStatus: %v", err)
	}
	out := buf.String()
	for _, want := range []string{"user@example.com", "gmail", "completed", "42"} {
		if !strings.Contains(out, want) {
			t.Errorf("expected %q in output:\n%s", want, out)
		}
	}
}

func TestRunSyncStatus_UnknownMailbox(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")

	var buf bytes.Buffer
	err := runSyncStatus(&buf, cfg, "nobody@example.com")
	if err == nil {
		t.Error("expected error for unregistered mailbox")
	}
}

// --- runSyncGmail error paths ---

func TestRunSyncGmail_MissingCredentials(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	st, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.CreateMailbox(context.Background(), models.Mailbox{ID: "user@example.com", Provider: "gmail"}); err != nil {
		t.Fatal(err)
	}
	_ = st.Close()

	cfg := config.Default()
	cfg.StoragePath = dbPath
	cfg.CredentialsPath = filepath.Join(dir, "nonexistent.json")

	var buf bytes.Buffer
	err = runSyncGmail(context.Background(), &buf, cfg, "user@example.com")
	if err == nil {
		t.Fatal("expected error for missing credentials")
	}
	if !strings.Contains(err.Error(), "credentials file not found") {
		t.Errorf("expected 'credentials file not found', got: %v", err)
	}
}

func TestRunSyncGmail_UnknownMailbox(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")

	var buf bytes.Buffer
	err := runSyncGmail(context.Background(), &buf, cfg, "nobody@example.com")
	if err == nil {
		t.Error("expected error for unregistered mailbox")
	}
}

func TestRunSyncGmail_MalformedCredentials(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	st, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.CreateMailbox(context.Background(), models.Mailbox{ID: "user@example.com", Provider: "gmail"}); err != nil {
		t.Fatal(err)
	}
	_ = st.Close()

	credPath := filepath.Join(dir, "credentials.json")
	if err := os.WriteFile(credPath, []byte("not-json"), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg := config.Default()
	cfg.StoragePath = dbPath
	cfg.CredentialsPath = credPath

	var buf bytes.Buffer
	err = runSyncGmail(context.Background(), &buf, cfg, "user@example.com")
	if err == nil {
		t.Fatal("expected error for malformed credentials")
	}
}

func TestRunSyncGmail_UsesResolvedTokenSource(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	st, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.CreateMailbox(context.Background(), models.Mailbox{ID: "user@example.com", Provider: "gmail"}); err != nil {
		t.Fatal(err)
	}
	_ = st.Close()

	cfg := config.Default()
	cfg.StoragePath = dbPath
	cfg.CredentialsPath = filepath.Join(dir, "credentials.json")
	if err := os.WriteFile(cfg.CredentialsPath, []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}

	origResolve := resolveGmailTokenSource
	origProvider := newGmailProvider
	origRunIngestion := runIngestion
	t.Cleanup(func() {
		resolveGmailTokenSource = origResolve
		newGmailProvider = origProvider
		runIngestion = origRunIngestion
	})

	resolveCalled := false
	providerCreated := false
	ingestionCalled := false

	resolveGmailTokenSource = func(_ *config.Config, mailboxID string) (func(context.Context) (oauth2.TokenSource, error), error) {
		resolveCalled = mailboxID == "user@example.com"
		return func(context.Context) (oauth2.TokenSource, error) {
			return oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "tok"}), nil
		}, nil
	}
	newGmailProvider = func(email string, tokenSourceFactory func(context.Context) (oauth2.TokenSource, error)) models.MailProvider {
		providerCreated = email == "user@example.com" && tokenSourceFactory != nil
		return stubMailProvider{}
	}
	runIngestion = func(_ context.Context, opts ingestion.Options) error {
		ingestionCalled = opts.MailboxID == "user@example.com" && opts.MailProvider != nil
		return nil
	}

	if err := runSyncGmail(context.Background(), io.Discard, cfg, "user@example.com"); err != nil {
		t.Fatalf("runSyncGmail: %v", err)
	}
	if !resolveCalled || !providerCreated || !ingestionCalled {
		t.Fatalf("expected resolve/provider/ingestion path, got resolve=%v provider=%v ingestion=%v", resolveCalled, providerCreated, ingestionCalled)
	}
}

func TestBuildSyncGmailCmd_RunE_UnknownMailbox(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")

	cmd := buildSyncGmailCmd(cfg)
	cmd.SetArgs([]string{"--account", "nobody@example.com"})
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	if err := cmd.Execute(); err == nil {
		t.Error("expected error for unknown mailbox in RunE")
	}
}

func TestBuildSyncStatusCmd_RunE_UnknownMailbox(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")

	cmd := buildSyncStatusCmd(cfg)
	cmd.SetArgs([]string{"--account", "nobody@example.com"})
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	if err := cmd.Execute(); err == nil {
		t.Error("expected error for unknown mailbox in RunE")
	}
}

// fixedTime returns a deterministic time for use in tests.
func fixedTime() time.Time {
	return time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)
}

type stubMailProvider struct{}

func (stubMailProvider) Authenticate(context.Context) error { return nil }

func (stubMailProvider) ListMessages(context.Context, string) ([]string, string, error) {
	return nil, "", nil
}

func (stubMailProvider) GetMessageMeta(context.Context, string) (*models.MessageMeta, error) {
	return &models.MessageMeta{}, nil
}
