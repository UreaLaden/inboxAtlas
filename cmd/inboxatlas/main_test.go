package main

import (
	"bytes"
	"context"
	"errors"
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
	"github.com/UreaLaden/inboxatlas/internal/engine"
	exportpkg "github.com/UreaLaden/inboxatlas/internal/export"
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

type alwaysErrorWriter struct{}

func (alwaysErrorWriter) Write([]byte) (int, error) {
	return 0, errors.New("write failed")
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
		if want == "" {
			continue
		}
		if !names[want] {
			t.Errorf("expected subcommand %q to be registered", want)
		}
	}
	for _, want := range []string{"report", "classify"} {
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
	if !strings.Contains(buf.String(), "Mailbox removed. InboxAtlas local data purged.") {
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
	if !strings.Contains(buf.String(), "Permanently purge InboxAtlas local data for mailbox 'user@company.com'? [y/N] ") {
		t.Errorf("expected purge confirmation prompt, got %q", buf.String())
	}
	if !strings.Contains(buf.String(), "Mailbox removed. InboxAtlas local data purged.") {
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
	if !strings.Contains(buf.String(), "Mailbox removed. InboxAtlas local data purged.") {
		t.Errorf("expected removed message, got %q", buf.String())
	}
}

func TestRunMailboxRemove_ForcePurgesLocalData(t *testing.T) {
	st := openMemStore(t)
	ctx := context.Background()
	if err := st.CreateMailbox(ctx, models.Mailbox{ID: "user@company.com", Alias: "work", Provider: "gmail"}); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	if err := st.UpsertMessage(ctx, models.MessageMeta{
		ProviderID: "msg1",
		MailboxID:  "user@company.com",
		Provider:   "gmail",
		FromEmail:  "a@x.com",
		Domain:     "x.com",
		Subject:    "Hello",
		ReceivedAt: now,
	}); err != nil {
		t.Fatalf("UpsertMessage: %v", err)
	}

	var buf bytes.Buffer
	if err := runMailboxRemove(&buf, nil, st, "work", true); err != nil {
		t.Fatalf("runMailboxRemove --force: %v", err)
	}

	mb, err := st.GetMailbox(ctx, "user@company.com")
	if err != nil {
		t.Fatalf("GetMailbox: %v", err)
	}
	if mb != nil {
		t.Fatal("expected mailbox to be removed")
	}

	messages, err := st.ListMessageMetaByMailbox(ctx, "user@company.com")
	if err != nil {
		t.Fatalf("ListMessageMetaByMailbox: %v", err)
	}
	if len(messages) != 0 {
		t.Fatalf("expected local messages to be purged, got %d", len(messages))
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
	if !strings.Contains(buf.String(), "Mailbox removed. InboxAtlas local data purged.") {
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
	err = runSyncGmail(context.Background(), &buf, cfg, "user@example.com", 0)
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
	err := runSyncGmail(context.Background(), &buf, cfg, "nobody@example.com", 0)
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
	err = runSyncGmail(context.Background(), &buf, cfg, "user@example.com", 0)
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

	if err := runSyncGmail(context.Background(), io.Discard, cfg, "user@example.com", 0); err != nil {
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

// --- buildAuthCmd (with status subcommand) ---

func TestBuildAuthCmd_HasStatusSubcommand(t *testing.T) {
	cmd := buildAuthCmd(config.Default())
	names := make(map[string]bool)
	for _, sub := range cmd.Commands() {
		names[sub.Name()] = true
	}
	if !names["status"] {
		t.Error("expected 'status' subcommand under 'auth'")
	}
}

// --- runAuthStatus ---

func TestRunAuthStatus_NoMailboxes(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")

	var buf bytes.Buffer
	if err := runAuthStatus(&buf, cfg); err != nil {
		t.Fatalf("runAuthStatus: %v", err)
	}
	if !strings.Contains(buf.String(), "No mailboxes registered") {
		t.Errorf("expected empty message, got: %q", buf.String())
	}
}

func TestRunAuthStatus_OAuthAuthenticated(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")
	cfg.TokenDir = filepath.Join(dir, "tokens")
	cfg.TokenStorage = "file"

	// Write a minimal installed-app credentials file.
	credPath := filepath.Join(dir, "credentials.json")
	if err := os.WriteFile(credPath, []byte(`{"installed":{"client_id":"x","client_secret":"y","redirect_uris":["urn:ietf:wg:oauth:2.0:oob"]}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg.CredentialsPath = credPath

	// Register mailbox and save a token.
	st, err := storage.Open(cfg.StoragePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.CreateMailbox(context.Background(), models.Mailbox{ID: "user@example.com", Provider: "gmail"}); err != nil {
		t.Fatal(err)
	}
	_ = st.Close()

	ts := auth.NewTokenStorage(&cfg)
	if err := ts.Save("gmail", "user@example.com", &oauth2.Token{AccessToken: "tok"}); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := runAuthStatus(&buf, cfg); err != nil {
		t.Fatalf("runAuthStatus: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "user@example.com") {
		t.Errorf("expected email in output: %q", out)
	}
	if !strings.Contains(out, "oauth") {
		t.Errorf("expected auth mode 'oauth' in output: %q", out)
	}
	if !strings.Contains(out, "authenticated") {
		t.Errorf("expected status 'authenticated' in output: %q", out)
	}
}

func TestRunAuthStatus_OAuthNotAuthenticated(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")
	cfg.TokenDir = filepath.Join(dir, "tokens")
	cfg.TokenStorage = "file"

	credPath := filepath.Join(dir, "credentials.json")
	if err := os.WriteFile(credPath, []byte(`{"installed":{"client_id":"x","client_secret":"y","redirect_uris":[]}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg.CredentialsPath = credPath

	st, err := storage.Open(cfg.StoragePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.CreateMailbox(context.Background(), models.Mailbox{ID: "user@example.com", Provider: "gmail"}); err != nil {
		t.Fatal(err)
	}
	_ = st.Close()

	// No token saved — should report not authenticated.
	var buf bytes.Buffer
	if err := runAuthStatus(&buf, cfg); err != nil {
		t.Fatalf("runAuthStatus: %v", err)
	}
	if !strings.Contains(buf.String(), "not authenticated") {
		t.Errorf("expected 'not authenticated', got: %q", buf.String())
	}
}

func TestRunAuthStatus_DelegatedConfigured(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")

	credPath := filepath.Join(dir, "sa.json")
	if err := os.WriteFile(credPath, []byte(`{"type":"service_account","client_email":"sa@proj.iam.gserviceaccount.com","private_key":"k","token_uri":"https://oauth2.googleapis.com/token"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg.CredentialsPath = credPath

	st, err := storage.Open(cfg.StoragePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.CreateMailbox(context.Background(), models.Mailbox{ID: "admin@corp.com", Provider: "gmail"}); err != nil {
		t.Fatal(err)
	}
	_ = st.Close()

	var buf bytes.Buffer
	if err := runAuthStatus(&buf, cfg); err != nil {
		t.Fatalf("runAuthStatus: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "delegated") {
		t.Errorf("expected 'delegated' mode in output: %q", out)
	}
	if !strings.Contains(out, "authenticated (delegated)") {
		t.Errorf("expected 'authenticated (delegated)' status in output: %q", out)
	}
}

func TestRunAuthStatus_NoCredentialsFile(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")
	cfg.CredentialsPath = filepath.Join(dir, "nonexistent.json")

	st, err := storage.Open(cfg.StoragePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.CreateMailbox(context.Background(), models.Mailbox{ID: "user@example.com", Provider: "gmail"}); err != nil {
		t.Fatal(err)
	}
	_ = st.Close()

	var buf bytes.Buffer
	if err := runAuthStatus(&buf, cfg); err != nil {
		t.Fatalf("runAuthStatus: %v", err)
	}
	if !strings.Contains(buf.String(), "no credentials file") {
		t.Errorf("expected 'no credentials file', got: %q", buf.String())
	}
}

func TestRunAuthStatus_StorageOpenError(t *testing.T) {
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(t.TempDir(), "nonexistent", "test.db")

	var buf bytes.Buffer
	if err := runAuthStatus(&buf, cfg); err == nil {
		t.Error("expected error for bad storage path")
	}
}

func TestRunAuthStatus_InvalidCredentialsFile(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")

	credPath := filepath.Join(dir, "credentials.json")
	// Write a JSON file that is neither installed-app nor service-account.
	if err := os.WriteFile(credPath, []byte(`{"other_key":"value"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg.CredentialsPath = credPath

	st, err := storage.Open(cfg.StoragePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.CreateMailbox(context.Background(), models.Mailbox{ID: "user@example.com", Provider: "gmail"}); err != nil {
		t.Fatal(err)
	}
	_ = st.Close()

	var buf bytes.Buffer
	if err := runAuthStatus(&buf, cfg); err != nil {
		t.Fatalf("runAuthStatus: %v", err)
	}
	if !strings.Contains(buf.String(), "invalid credentials file") {
		t.Errorf("expected 'invalid credentials file', got: %q", buf.String())
	}
}

func TestRunAuthStatus_UnsupportedProvider(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")

	st, err := storage.Open(cfg.StoragePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.CreateMailbox(context.Background(), models.Mailbox{ID: "user@outlook.com", Provider: "outlook"}); err != nil {
		t.Fatal(err)
	}
	_ = st.Close()

	var buf bytes.Buffer
	if err := runAuthStatus(&buf, cfg); err != nil {
		t.Fatalf("runAuthStatus: %v", err)
	}
	if !strings.Contains(buf.String(), "unsupported provider") {
		t.Errorf("expected 'unsupported provider', got: %q", buf.String())
	}
}

func TestBuildAuthStatusCmd_RunE_Empty(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")

	cmd := buildAuthStatusCmd(cfg)
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !strings.Contains(buf.String(), "No mailboxes registered") {
		t.Errorf("expected empty message, got: %q", buf.String())
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

// --- seedReportData seeds a mailbox and messages for report tests ---

func seedReportData(t *testing.T, dbPath string) {
	t.Helper()
	st, err := storage.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = st.Close() }()
	ctx := context.Background()
	if err := st.CreateMailbox(ctx, models.Mailbox{ID: "user@example.com", Provider: "gmail"}); err != nil {
		t.Fatalf("CreateMailbox: %v", err)
	}
	msgs := []models.MessageMeta{
		{ProviderID: "r1", MailboxID: "user@example.com", Provider: "gmail", FromEmail: "alice@foo.com", FromName: "Alice", Domain: "foo.com", Subject: "Meeting update", ReceivedAt: time.Date(2025, 1, 5, 0, 0, 0, 0, time.UTC)},
		{ProviderID: "r2", MailboxID: "user@example.com", Provider: "gmail", FromEmail: "alice@foo.com", FromName: "Alice", Domain: "foo.com", Subject: "Meeting notes", ReceivedAt: time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)},
		{ProviderID: "r3", MailboxID: "user@example.com", Provider: "gmail", FromEmail: "bob@bar.com", FromName: "Bob", Domain: "bar.com", Subject: "Weekly report", ReceivedAt: time.Date(2025, 2, 3, 0, 0, 0, 0, time.UTC)},
	}
	for _, m := range msgs {
		if err := st.UpsertMessage(ctx, m); err != nil {
			t.Fatalf("UpsertMessage: %v", err)
		}
	}
}

func seedClassifyData(t *testing.T, dbPath string) {
	t.Helper()
	st, err := storage.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = st.Close() }()
	ctx := context.Background()
	if err := st.CreateMailbox(ctx, models.Mailbox{ID: "user@example.com", Provider: "gmail"}); err != nil {
		t.Fatalf("CreateMailbox: %v", err)
	}
	if err := st.UpsertMessage(ctx, models.MessageMeta{
		ProviderID: "c1",
		MailboxID:  "user@example.com",
		Provider:   "gmail",
		FromEmail:  "groupupdates@facebookmail.com",
		Domain:     "facebookmail.com",
		ReceivedAt: time.Date(2025, 2, 4, 0, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatalf("UpsertMessage: %v", err)
	}
}

func seedClassifyResultsData(t *testing.T, dbPath string) {
	t.Helper()
	st, err := storage.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = st.Close() }()

	ctx := context.Background()
	if err := st.CreateMailbox(ctx, models.Mailbox{ID: "user@example.com", Provider: "gmail"}); err != nil {
		t.Fatalf("CreateMailbox: %v", err)
	}

	now := time.Date(2025, 2, 4, 0, 0, 0, 0, time.UTC)
	for _, msg := range []models.MessageMeta{
		{ProviderID: "r1", MailboxID: "user@example.com", Provider: "gmail", ReceivedAt: now},
		{ProviderID: "r2", MailboxID: "user@example.com", Provider: "gmail", ReceivedAt: now},
		{ProviderID: "r3", MailboxID: "user@example.com", Provider: "gmail", ReceivedAt: now},
	} {
		if err := st.UpsertMessage(ctx, msg); err != nil {
			t.Fatalf("UpsertMessage(%s): %v", msg.ProviderID, err)
		}
	}

	for _, c := range []storage.Classification{
		{MessageID: "r1", MailboxID: "user@example.com", Category: "vendor", Intent: "invoice", Source: "seed", ClassifiedAt: now},
		{MessageID: "r2", MailboxID: "user@example.com", Category: "vendor", Source: "seed", ClassifiedAt: now},
		{MessageID: "r3", MailboxID: "user@example.com", Category: "unknown", Source: "seed", ClassifiedAt: now},
	} {
		if err := st.SaveClassification(ctx, c); err != nil {
			t.Fatalf("SaveClassification(%s): %v", c.MessageID, err)
		}
	}
}

func seedClassifyResultsNoIntentData(t *testing.T, dbPath string) {
	t.Helper()
	st, err := storage.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = st.Close() }()

	ctx := context.Background()
	if err := st.CreateMailbox(ctx, models.Mailbox{ID: "user@example.com", Provider: "gmail"}); err != nil {
		t.Fatalf("CreateMailbox: %v", err)
	}

	now := time.Date(2025, 2, 4, 0, 0, 0, 0, time.UTC)
	for _, msg := range []models.MessageMeta{
		{ProviderID: "r1", MailboxID: "user@example.com", Provider: "gmail", ReceivedAt: now},
		{ProviderID: "r2", MailboxID: "user@example.com", Provider: "gmail", ReceivedAt: now},
	} {
		if err := st.UpsertMessage(ctx, msg); err != nil {
			t.Fatalf("UpsertMessage(%s): %v", msg.ProviderID, err)
		}
	}

	for _, c := range []storage.Classification{
		{MessageID: "r1", MailboxID: "user@example.com", Category: "vendor", Source: "seed", ClassifiedAt: now},
		{MessageID: "r2", MailboxID: "user@example.com", Category: "unknown", Source: "seed", ClassifiedAt: now},
	} {
		if err := st.SaveClassification(ctx, c); err != nil {
			t.Fatalf("SaveClassification(%s): %v", c.MessageID, err)
		}
	}
}

func seedClassifiedMessagesData(t *testing.T, dbPath string) {
	t.Helper()
	st, err := storage.Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = st.Close() }()

	ctx := context.Background()
	if err := st.CreateMailbox(ctx, models.Mailbox{ID: "user@example.com", Provider: "gmail"}); err != nil {
		t.Fatalf("CreateMailbox: %v", err)
	}

	now := time.Date(2026, 4, 2, 10, 0, 0, 0, time.UTC)
	for _, msg := range []models.MessageMeta{
		{ProviderID: "gmail-1", MailboxID: "user@example.com", Provider: "gmail", FromEmail: "acct1@client.example", Domain: "client.example", Subject: "Invoice for April services with more text", HasAttachment: true, ReceivedAt: now},
		{ProviderID: "gmail-2", MailboxID: "user@example.com", Provider: "gmail", FromEmail: "acct2@client.example", Domain: "client.example", Subject: "Project update", ReceivedAt: now.Add(1 * time.Hour)},
	} {
		if err := st.UpsertMessage(ctx, msg); err != nil {
			t.Fatalf("UpsertMessage(%s): %v", msg.ProviderID, err)
		}
	}

	for _, c := range []storage.Classification{
		{MessageID: "gmail-1", MailboxID: "user@example.com", Category: "client", Intent: "invoice", MatchedRule: "subject_term:invoice", Source: "seed", ClassifiedAt: now},
		{MessageID: "gmail-2", MailboxID: "user@example.com", Category: "client", MatchedRule: "domain:client.example", Source: "seed", ClassifiedAt: now},
	} {
		if err := st.SaveClassification(ctx, c); err != nil {
			t.Fatalf("SaveClassification(%s): %v", c.MessageID, err)
		}
	}
}

// --- buildReportCmd ---

func TestBuildReportCmd_HasSubcommands(t *testing.T) {
	cmd := buildReportCmd(config.Default())
	names := make(map[string]bool)
	for _, sub := range cmd.Commands() {
		names[sub.Name()] = true
	}
	for _, want := range []string{"export", "domains", "senders", "subjects", "volume"} {
		if !names[want] {
			t.Errorf("expected subcommand %q under report", want)
		}
	}
}

func TestBuildRoot_HasReportCommand(t *testing.T) {
	root := buildRoot(config.Default())
	names := make(map[string]bool)
	for _, cmd := range root.Commands() {
		names[cmd.Name()] = true
	}
	if !names["report"] {
		t.Error("expected 'report' subcommand on root")
	}
	if !names["classify"] {
		t.Error("expected 'classify' subcommand on root")
	}
}

func TestBuildClassifyCmd_HasSubcommands(t *testing.T) {
	cmd := buildClassifyCmd(config.Default())
	names := make(map[string]bool)
	for _, sub := range cmd.Commands() {
		names[sub.Name()] = true
	}
	for _, want := range []string{"run", "results", "suggestions", "infer", "promote", "seeds", "categories", "pattern-types", "label-analysis"} {
		if !names[want] {
			t.Errorf("expected subcommand %q under classify", want)
		}
	}
}

func TestBuildClassifyInferCmd_HasSubcommands(t *testing.T) {
	cmd := buildClassifyInferCmd(config.Default())
	names := make(map[string]bool)
	for _, sub := range cmd.Commands() {
		names[sub.Name()] = true
	}
	if !names["suggestions"] {
		t.Fatal("expected suggestions subcommand under classify infer")
	}
}

func TestBuildClassifyInferSuggestionsCmd_RequiresAccount(t *testing.T) {
	cmd := buildClassifyInferSuggestionsCmd(config.Default())
	cmd.SetArgs(nil)
	if err := cmd.Execute(); err == nil {
		t.Fatal("expected missing account flag error")
	}
}

func TestBuildClassifySeedsCmd_HasSubcommands(t *testing.T) {
	cmd := buildClassifySeedsCmd(config.Default())
	names := make(map[string]bool)
	for _, sub := range cmd.Commands() {
		names[sub.Name()] = true
	}
	for _, want := range []string{"list", "delete"} {
		if !names[want] {
			t.Errorf("expected subcommand %q under classify seeds", want)
		}
	}
}

func TestRunClassifyRun_EmptyMailbox(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")

	st, err := storage.Open(cfg.StoragePath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.CreateMailbox(context.Background(), models.Mailbox{ID: "user@example.com", Provider: "gmail"}); err != nil {
		t.Fatalf("CreateMailbox: %v", err)
	}
	_ = st.Close()

	err = runClassifyRun(context.Background(), io.Discard, cfg, "user@example.com")
	if err == nil {
		t.Fatal("expected empty mailbox error")
	}
}

func TestRunClassifyRun_Success(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")
	seedClassifyData(t, cfg.StoragePath)

	var buf bytes.Buffer
	if err := runClassifyRun(context.Background(), &buf, cfg, "user@example.com"); err != nil {
		t.Fatalf("runClassifyRun: %v", err)
	}
	output := buf.String()
	if !strings.Contains(output, "Classified 1 messages for user@example.com.") {
		t.Fatalf("unexpected output: %q", buf.String())
	}
	if !strings.Contains(output, "CATEGORY") || !strings.Contains(output, "social") || !strings.Contains(output, "Unknown: 0.0%") {
		t.Fatalf("missing breakdown output: %q", output)
	}

	st, err := storage.Open(cfg.StoragePath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = st.Close() }()

	got, err := st.GetClassification(context.Background(), "c1", "user@example.com")
	if err != nil {
		t.Fatalf("GetClassification: %v", err)
	}
	if got == nil || got.Category != "social" {
		t.Fatalf("expected social classification, got %+v", got)
	}
}

func TestRunClassifyRun_WithUnknownBreakdown(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")

	st, err := storage.Open(cfg.StoragePath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.CreateMailbox(context.Background(), models.Mailbox{ID: "user@example.com", Provider: "gmail"}); err != nil {
		t.Fatalf("CreateMailbox: %v", err)
	}
	now := time.Now().UTC()
	for _, msg := range []models.MessageMeta{
		{
			ProviderID: "c1",
			MailboxID:  "user@example.com",
			Provider:   "gmail",
			FromEmail:  "groupupdates@facebookmail.com",
			Domain:     "facebookmail.com",
			ReceivedAt: now,
		},
		{
			ProviderID: "c2",
			MailboxID:  "user@example.com",
			Provider:   "gmail",
			FromEmail:  "unknown@example.com",
			Domain:     "example.com",
			ReceivedAt: now.Add(time.Minute),
		},
	} {
		if err := st.UpsertMessage(context.Background(), msg); err != nil {
			t.Fatalf("UpsertMessage(%s): %v", msg.ProviderID, err)
		}
	}
	_ = st.Close()

	var buf bytes.Buffer
	if err := runClassifyRun(context.Background(), &buf, cfg, "user@example.com"); err != nil {
		t.Fatalf("runClassifyRun: %v", err)
	}
	output := buf.String()
	if !strings.Contains(output, "social") || !strings.Contains(output, "unknown") || !strings.Contains(output, "Unknown: 50.0%") {
		t.Fatalf("unexpected breakdown output: %q", output)
	}
}

func TestRunClassifyRun_NoBreakdownRows(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")

	st, err := storage.Open(cfg.StoragePath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.CreateMailbox(context.Background(), models.Mailbox{ID: "user@example.com", Provider: "gmail"}); err != nil {
		t.Fatalf("CreateMailbox: %v", err)
	}
	now := time.Now().UTC()
	if err := st.UpsertMessage(context.Background(), models.MessageMeta{
		ProviderID: "c1",
		MailboxID:  "user@example.com",
		Provider:   "gmail",
		FromEmail:  "groupupdates@facebookmail.com",
		Domain:     "facebookmail.com",
		ReceivedAt: now,
	}); err != nil {
		t.Fatalf("UpsertMessage: %v", err)
	}
	originalRun := runClassify
	runClassify = func(ctx context.Context, cfg config.Config, account string) (engine.ClassifyRunSummary, error) {
		return engine.ClassifyRunSummary{
			MailboxID:         "user@example.com",
			MessagesProcessed: 1,
		}, nil
	}
	t.Cleanup(func() { runClassify = originalRun })
	_ = st.Close()

	var buf bytes.Buffer
	if err := runClassifyRun(context.Background(), &buf, cfg, "user@example.com"); err != nil {
		t.Fatalf("runClassifyRun: %v", err)
	}
	output := buf.String()
	if strings.Contains(output, "CATEGORY") || strings.Contains(output, "Unknown:") {
		t.Fatalf("expected count-only output, got %q", output)
	}
}

func TestRunClassifySuggestions_Table(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")

	st, err := storage.Open(cfg.StoragePath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.CreateMailbox(context.Background(), models.Mailbox{ID: "user@example.com", Provider: "gmail"}); err != nil {
		t.Fatalf("CreateMailbox: %v", err)
	}
	if err := st.UpsertDomainStat(context.Background(), "user@example.com", "healthymd.com", 6); err != nil {
		t.Fatalf("UpsertDomainStat: %v", err)
	}
	_ = st.Close()

	var buf bytes.Buffer
	if err := runClassifySuggestions(context.Background(), &buf, cfg, "user@example.com", "table"); err != nil {
		t.Fatalf("runClassifySuggestions: %v", err)
	}
	if !strings.Contains(buf.String(), "PATTERN TYPE") || !strings.Contains(buf.String(), "healthymd.com") {
		t.Fatalf("unexpected table output: %q", buf.String())
	}
}

func TestRunClassifySuggestions_JSON(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")

	st, err := storage.Open(cfg.StoragePath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.CreateMailbox(context.Background(), models.Mailbox{ID: "user@example.com", Provider: "gmail"}); err != nil {
		t.Fatalf("CreateMailbox: %v", err)
	}
	if err := st.UpsertDomainStat(context.Background(), "user@example.com", "healthymd.com", 6); err != nil {
		t.Fatalf("UpsertDomainStat: %v", err)
	}
	_ = st.Close()

	var buf bytes.Buffer
	if err := runClassifySuggestions(context.Background(), &buf, cfg, "user@example.com", "json"); err != nil {
		t.Fatalf("runClassifySuggestions: %v", err)
	}
	if !strings.Contains(buf.String(), "\"pattern_value\": \"healthymd.com\"") {
		t.Fatalf("unexpected json output: %q", buf.String())
	}
}

func TestRunClassifyInfer(t *testing.T) {
	originalRunInference := runInference
	runInference = func(ctx context.Context, cfg config.Config, account, command string, args []string) (engine.InferenceRunSummary, error) {
		return engine.InferenceRunSummary{
			MailboxID: "user@example.com",
			Submitted: 3,
			Persisted: 2,
			High:      1,
			Medium:    1,
			Low:       1,
			Rejected:  0,
		}, nil
	}
	t.Cleanup(func() { runInference = originalRunInference })

	var buf bytes.Buffer
	if err := runClassifyInfer(context.Background(), &buf, config.Default(), "user@example.com", "provider", nil); err != nil {
		t.Fatalf("runClassifyInfer: %v", err)
	}
	output := buf.String()
	if !strings.Contains(output, "Inference submitted 3 unknown messages") || !strings.Contains(output, "Persisted 2 suggestions") {
		t.Fatalf("unexpected output: %q", output)
	}
}

func TestRunClassifyInfer_RequiresProviderCommand(t *testing.T) {
	t.Setenv("INBOXATLAS_INFERENCE_PROVIDER_CMD", "")
	err := runClassifyInfer(context.Background(), io.Discard, config.Default(), "user@example.com", "", nil)
	if err == nil {
		t.Fatal("expected missing provider command error")
	}
}

func TestRunClassifyInfer_UsesEnvProviderCommand(t *testing.T) {
	originalRunInference := runInference
	runInference = func(ctx context.Context, cfg config.Config, account, command string, args []string) (engine.InferenceRunSummary, error) {
		if command != "provider-from-env" {
			t.Fatalf("command: got %q", command)
		}
		return engine.InferenceRunSummary{MailboxID: "user@example.com"}, nil
	}
	t.Cleanup(func() { runInference = originalRunInference })

	t.Setenv("INBOXATLAS_INFERENCE_PROVIDER_CMD", "provider-from-env")
	if err := runClassifyInfer(context.Background(), io.Discard, config.Default(), "user@example.com", "", nil); err != nil {
		t.Fatalf("runClassifyInfer: %v", err)
	}
}

func TestRunClassifyInferSuggestions_TableAndJSON(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")

	st, err := storage.Open(cfg.StoragePath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.CreateMailbox(context.Background(), models.Mailbox{ID: "user@example.com", Provider: "gmail"}); err != nil {
		t.Fatalf("CreateMailbox: %v", err)
	}
	if err := st.UpsertMessage(context.Background(), models.MessageMeta{
		ProviderID: "m1",
		MailboxID:  "user@example.com",
		Provider:   "gmail",
		FromEmail:  "ai@healthymd.com",
		Domain:     "healthymd.com",
		ReceivedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("UpsertMessage: %v", err)
	}
	if err := st.SaveInferenceCandidate(context.Background(), storage.InferenceSuggestion{
		MailboxID:      "user@example.com",
		MessageID:      "m1",
		PatternType:    "domain",
		PatternValue:   "healthymd.com",
		Category:       "client",
		Confidence:     0.81,
		ConfidenceBand: "high",
		ReviewRequired: false,
	}); err != nil {
		t.Fatalf("SaveInferenceCandidate: %v", err)
	}
	_ = st.Close()

	var table bytes.Buffer
	if err := runClassifyInferSuggestions(context.Background(), &table, cfg, "user@example.com", "table"); err != nil {
		t.Fatalf("runClassifyInferSuggestions table: %v", err)
	}
	if !strings.Contains(table.String(), "MESSAGE ID") || !strings.Contains(table.String(), "healthymd.com") {
		t.Fatalf("unexpected table output: %q", table.String())
	}

	var jsonBuf bytes.Buffer
	if err := runClassifyInferSuggestions(context.Background(), &jsonBuf, cfg, "user@example.com", "json"); err != nil {
		t.Fatalf("runClassifyInferSuggestions json: %v", err)
	}
	if !strings.Contains(jsonBuf.String(), "\"message_id\": \"m1\"") || !strings.Contains(jsonBuf.String(), "\"pattern_value\": \"healthymd.com\"") {
		t.Fatalf("unexpected json output: %q", jsonBuf.String())
	}
}

func TestRunClassifyInferSuggestions_EmptyAndInvalidFormat(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")

	st, err := storage.Open(cfg.StoragePath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.CreateMailbox(context.Background(), models.Mailbox{ID: "user@example.com", Provider: "gmail"}); err != nil {
		t.Fatalf("CreateMailbox: %v", err)
	}
	_ = st.Close()

	var buf bytes.Buffer
	if err := runClassifyInferSuggestions(context.Background(), &buf, cfg, "user@example.com", "table"); err != nil {
		t.Fatalf("runClassifyInferSuggestions empty: %v", err)
	}
	if !strings.Contains(buf.String(), "No AI inference suggestions") {
		t.Fatalf("unexpected empty output: %q", buf.String())
	}

	if err := runClassifyInferSuggestions(context.Background(), io.Discard, cfg, "user@example.com", "yaml"); err == nil {
		t.Fatal("expected invalid format error")
	}
}

func TestRunClassifyResults_Table(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")
	seedClassifyResultsData(t, cfg.StoragePath)

	var buf bytes.Buffer
	if err := runClassifyResults(context.Background(), &buf, cfg, "user@example.com", "table"); err != nil {
		t.Fatalf("runClassifyResults: %v", err)
	}

	output := buf.String()
	if !strings.Contains(output, "CATEGORY") || !strings.Contains(output, "INTENT") || !strings.Contains(output, "vendor") || !strings.Contains(output, "invoice") || !strings.Contains(output, "unknown") {
		t.Fatalf("unexpected table output: %q", output)
	}
	if !strings.Contains(output, "Total: 3") || !strings.Contains(output, "Unknown: 33.3%") {
		t.Fatalf("missing summary footer: %q", output)
	}
}

func TestRunClassifyResults_JSON(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")
	seedClassifyResultsData(t, cfg.StoragePath)

	var buf bytes.Buffer
	if err := runClassifyResults(context.Background(), &buf, cfg, "user@example.com", "json"); err != nil {
		t.Fatalf("runClassifyResults: %v", err)
	}

	output := buf.String()
	if !strings.Contains(output, "\"mailbox_id\": \"user@example.com\"") ||
		!strings.Contains(output, "\"total\": 3") ||
		!strings.Contains(output, "\"intent\": \"invoice\"") ||
		!strings.Contains(output, "\"unknown_pct\": 33.333333333333336") {
		t.Fatalf("unexpected json output: %q", output)
	}
}

func TestRunClassifyResults_Table_NoIntentColumn(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")
	seedClassifyResultsNoIntentData(t, cfg.StoragePath)

	var buf bytes.Buffer
	if err := runClassifyResults(context.Background(), &buf, cfg, "user@example.com", "table"); err != nil {
		t.Fatalf("runClassifyResults: %v", err)
	}

	output := buf.String()
	if !strings.Contains(output, "CATEGORY") || strings.Contains(output, "INTENT") {
		t.Fatalf("unexpected no-intent table output: %q", output)
	}
}

func TestRunClassifyMessages_Table(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")
	seedClassifiedMessagesData(t, cfg.StoragePath)

	var buf bytes.Buffer
	if err := runClassifyMessages(context.Background(), &buf, cfg, "user@example.com", "client", "invoice", "", "table", 100); err != nil {
		t.Fatalf("runClassifyMessages: %v", err)
	}

	output := buf.String()
	if !strings.Contains(output, "MESSAGE ID") || !strings.Contains(output, "HAS ATTACHMENT") || !strings.Contains(output, "gmail-1") || !strings.Contains(output, "client") || !strings.Contains(output, "invoice") || !strings.Contains(output, "true") {
		t.Fatalf("unexpected table output: %q", output)
	}
}

func TestRunClassifyMessages_JSON(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")
	seedClassifiedMessagesData(t, cfg.StoragePath)

	var buf bytes.Buffer
	if err := runClassifyMessages(context.Background(), &buf, cfg, "user@example.com", "", "", "", "json", 100); err != nil {
		t.Fatalf("runClassifyMessages: %v", err)
	}
	if !strings.Contains(buf.String(), "\"message_id\": \"gmail-2\"") || !strings.Contains(buf.String(), "\"intent\": \"invoice\"") {
		t.Fatalf("unexpected json output: %q", buf.String())
	}
	if !strings.Contains(buf.String(), "\"has_attachment\": true") {
		t.Fatalf("expected has_attachment field in json output: %q", buf.String())
	}
}

func TestRunClassifyMessages_CSV(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")
	seedClassifiedMessagesData(t, cfg.StoragePath)

	var buf bytes.Buffer
	if err := runClassifyMessages(context.Background(), &buf, cfg, "user@example.com", "", "", "", "csv", 100); err != nil {
		t.Fatalf("runClassifyMessages: %v", err)
	}
	output := buf.String()
	if !strings.Contains(output, "MessageID,Timestamp,Sender,Domain,Subject,Intent,Category,HasAttachment") {
		t.Fatalf("unexpected csv header: %q", output)
	}
	if !strings.Contains(output, "gmail-1,") || !strings.Contains(output, "acct1@client.example,client.example,Invoice for April services with more text,invoice,client,true") {
		t.Fatalf("unexpected csv body: %q", output)
	}
}

func TestRunClassifyMessages_Since(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")
	seedClassifiedMessagesData(t, cfg.StoragePath)

	var buf bytes.Buffer
	if err := runClassifyMessages(context.Background(), &buf, cfg, "user@example.com", "", "", "2026-04-02T10:30:00Z", "json", 100); err != nil {
		t.Fatalf("runClassifyMessages: %v", err)
	}
	output := buf.String()
	if strings.Contains(output, "\"message_id\": \"gmail-1\"") {
		t.Fatalf("expected since-filtered output, got %q", output)
	}
	if !strings.Contains(output, "\"message_id\": \"gmail-2\"") {
		t.Fatalf("expected newest message in since-filtered output, got %q", output)
	}
}

func TestRunClassifyMessages_InvalidSince(t *testing.T) {
	err := runClassifyMessages(context.Background(), io.Discard, config.Default(), "user@example.com", "", "", "2026-04-02", "json", 100)
	if err == nil || !strings.Contains(err.Error(), "must be RFC3339") {
		t.Fatalf("expected since validation error, got %v", err)
	}
}

func TestRunClassifyMessages_UnknownCategory(t *testing.T) {
	err := runClassifyMessages(context.Background(), io.Discard, config.Default(), "user@example.com", "bogus", "", "", "table", 100)
	if err == nil || !strings.Contains(err.Error(), "classify categories") {
		t.Fatalf("expected category validation error, got %v", err)
	}
}

func TestRunClassifyMessages_UnknownIntent(t *testing.T) {
	err := runClassifyMessages(context.Background(), io.Discard, config.Default(), "user@example.com", "", "bogus", "", "table", 100)
	if err == nil || !strings.Contains(err.Error(), "classify intents") {
		t.Fatalf("expected intent validation error, got %v", err)
	}
}

func TestRunClassifyMessages_UnknownFormat(t *testing.T) {
	err := runClassifyMessages(context.Background(), io.Discard, config.Default(), "user@example.com", "", "", "", "xml", 100)
	if err == nil || !strings.Contains(err.Error(), "table, csv, json") {
		t.Fatalf("expected format validation error, got %v", err)
	}
}

func TestRunClassifyMessages_Empty(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")

	st, err := storage.Open(cfg.StoragePath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.CreateMailbox(context.Background(), models.Mailbox{ID: "user@example.com", Provider: "gmail"}); err != nil {
		t.Fatalf("CreateMailbox: %v", err)
	}
	_ = st.Close()

	var buf bytes.Buffer
	if err := runClassifyMessages(context.Background(), &buf, cfg, "user@example.com", "", "", "", "table", 100); err != nil {
		t.Fatalf("runClassifyMessages: %v", err)
	}
	if !strings.Contains(buf.String(), "No classified messages found for user@example.com.") {
		t.Fatalf("unexpected empty output: %q", buf.String())
	}
}

func TestRunClassifyResults_Empty(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")

	st, err := storage.Open(cfg.StoragePath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.CreateMailbox(context.Background(), models.Mailbox{ID: "user@example.com", Provider: "gmail"}); err != nil {
		t.Fatalf("CreateMailbox: %v", err)
	}
	_ = st.Close()

	var buf bytes.Buffer
	if err := runClassifyResults(context.Background(), &buf, cfg, "user@example.com", "table"); err != nil {
		t.Fatalf("runClassifyResults: %v", err)
	}
	if !strings.Contains(buf.String(), "No classifications found for user@example.com.") {
		t.Fatalf("unexpected empty output: %q", buf.String())
	}
}

func TestRunClassifyPromote_SuccessAndIdempotent(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")

	st, err := storage.Open(cfg.StoragePath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.CreateMailbox(context.Background(), models.Mailbox{ID: "user@example.com", Provider: "gmail"}); err != nil {
		t.Fatalf("CreateMailbox: %v", err)
	}
	if err := st.UpsertDomainStat(context.Background(), "user@example.com", "healthymd.com", 6); err != nil {
		t.Fatalf("UpsertDomainStat: %v", err)
	}
	_ = st.Close()

	req := engine.PromoteSuggestionRequest{
		PatternType:  "domain",
		PatternValue: "healthymd.com",
		Category:     "client",
	}

	var first bytes.Buffer
	if err := runClassifyPromote(context.Background(), &first, cfg, "user@example.com", req); err != nil {
		t.Fatalf("first runClassifyPromote: %v", err)
	}
	if !strings.Contains(first.String(), "Promoted suggestion") {
		t.Fatalf("unexpected first output: %q", first.String())
	}

	var second bytes.Buffer
	if err := runClassifyPromote(context.Background(), &second, cfg, "user@example.com", req); err != nil {
		t.Fatalf("second runClassifyPromote: %v", err)
	}
	if !strings.Contains(second.String(), "already promoted") {
		t.Fatalf("unexpected second output: %q", second.String())
	}
}

func TestRunClassifyPromote_InvalidSuggestion(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")

	st, err := storage.Open(cfg.StoragePath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.CreateMailbox(context.Background(), models.Mailbox{ID: "user@example.com", Provider: "gmail"}); err != nil {
		t.Fatalf("CreateMailbox: %v", err)
	}
	_ = st.Close()

	err = runClassifyPromote(context.Background(), io.Discard, cfg, "user@example.com", engine.PromoteSuggestionRequest{
		PatternType:  "domain",
		PatternValue: "not-a-suggestion.com",
		Category:     "vendor",
	})
	if err == nil {
		t.Fatal("expected invalid suggestion error")
	}
}

func TestRunClassifySeedsList_Table(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")

	st, err := storage.Open(cfg.StoragePath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.CreateMailbox(context.Background(), models.Mailbox{ID: "user@example.com", Provider: "gmail"}); err != nil {
		t.Fatalf("CreateMailbox: %v", err)
	}
	if err := st.InsertSeed(context.Background(), storage.ClassificationSeed{
		MailboxID:    "user@example.com",
		PatternType:  "domain",
		PatternValue: "mailbox.example",
		Category:     "client",
		Source:       "operator",
		Priority:     50,
	}); err != nil {
		t.Fatalf("InsertSeed: %v", err)
	}
	_ = st.Close()

	var buf bytes.Buffer
	if err := runClassifySeedsList(context.Background(), &buf, cfg, "user@example.com", "table"); err != nil {
		t.Fatalf("runClassifySeedsList: %v", err)
	}
	if !strings.Contains(buf.String(), "ID") || !strings.Contains(buf.String(), "mailbox.example") {
		t.Fatalf("unexpected output: %q", buf.String())
	}
}

func TestRunClassifySeedsList_JSON(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")

	st, err := storage.Open(cfg.StoragePath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.CreateMailbox(context.Background(), models.Mailbox{ID: "user@example.com", Provider: "gmail"}); err != nil {
		t.Fatalf("CreateMailbox: %v", err)
	}
	if err := st.InsertSeed(context.Background(), storage.ClassificationSeed{
		MailboxID:    "user@example.com",
		PatternType:  "domain",
		PatternValue: "mailbox.example",
		Category:     "client",
		Source:       "operator",
		Priority:     50,
	}); err != nil {
		t.Fatalf("InsertSeed: %v", err)
	}
	_ = st.Close()

	var buf bytes.Buffer
	if err := runClassifySeedsList(context.Background(), &buf, cfg, "user@example.com", "json"); err != nil {
		t.Fatalf("runClassifySeedsList: %v", err)
	}
	if !strings.Contains(buf.String(), "\"id\": ") || !strings.Contains(buf.String(), "\"pattern_value\": \"mailbox.example\"") {
		t.Fatalf("unexpected json output: %q", buf.String())
	}
}

func TestRunClassifySeedsList_Empty(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")

	st, err := storage.Open(cfg.StoragePath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.CreateMailbox(context.Background(), models.Mailbox{ID: "user@example.com", Provider: "gmail"}); err != nil {
		t.Fatalf("CreateMailbox: %v", err)
	}
	_ = st.Close()

	var buf bytes.Buffer
	if err := runClassifySeedsList(context.Background(), &buf, cfg, "user@example.com", "table"); err != nil {
		t.Fatalf("runClassifySeedsList: %v", err)
	}
	if !strings.Contains(buf.String(), "No mailbox-scoped seeds found") {
		t.Fatalf("unexpected output: %q", buf.String())
	}
}

func TestRunClassifySeedsList_InvalidFormat(t *testing.T) {
	err := runClassifySeedsList(context.Background(), io.Discard, config.Default(), "user@example.com", "yaml")
	if err == nil {
		t.Fatal("expected invalid format error")
	}
}

func TestRunClassifySeedsDelete(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")

	st, err := storage.Open(cfg.StoragePath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.CreateMailbox(context.Background(), models.Mailbox{ID: "user@example.com", Provider: "gmail"}); err != nil {
		t.Fatalf("CreateMailbox: %v", err)
	}
	if err := st.InsertSeed(context.Background(), storage.ClassificationSeed{
		MailboxID:    "user@example.com",
		PatternType:  "domain",
		PatternValue: "mailbox.example",
		Category:     "client",
		Source:       "operator",
		Priority:     50,
	}); err != nil {
		t.Fatalf("InsertSeed: %v", err)
	}
	seeds, err := st.ListSeeds(context.Background(), "user@example.com")
	if err != nil {
		t.Fatalf("ListSeeds: %v", err)
	}
	seedID := seeds[0].ID
	_ = st.Close()

	var buf bytes.Buffer
	if err := runClassifySeedsDelete(context.Background(), &buf, cfg, "user@example.com", seedID); err != nil {
		t.Fatalf("runClassifySeedsDelete: %v", err)
	}
	if !strings.Contains(buf.String(), "Deleted mailbox seed") {
		t.Fatalf("unexpected output: %q", buf.String())
	}
}

func TestRunClassifySeedsDelete_GlobalSeedError(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")

	st, err := storage.Open(cfg.StoragePath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.CreateMailbox(context.Background(), models.Mailbox{ID: "user@example.com", Provider: "gmail"}); err != nil {
		t.Fatalf("CreateMailbox: %v", err)
	}
	if err := st.InsertSeed(context.Background(), storage.ClassificationSeed{
		PatternType:  "domain",
		PatternValue: "global.example",
		Category:     "vendor",
		Source:       "seed",
		Priority:     100,
	}); err != nil {
		t.Fatalf("InsertSeed: %v", err)
	}
	seeds, err := st.ListSeeds(context.Background(), "user@example.com")
	if err != nil {
		t.Fatalf("ListSeeds: %v", err)
	}
	seedID := seeds[0].ID
	_ = st.Close()

	err = runClassifySeedsDelete(context.Background(), io.Discard, cfg, "user@example.com", seedID)
	if err == nil {
		t.Fatal("expected global-seed guard error")
	}
}

func TestRunClassifyCategories(t *testing.T) {
	var buf bytes.Buffer
	if err := runClassifyCategories(&buf); err != nil {
		t.Fatalf("runClassifyCategories: %v", err)
	}
	if !strings.Contains(buf.String(), "internal") || !strings.Contains(buf.String(), "unknown") {
		t.Fatalf("unexpected categories output: %q", buf.String())
	}
}

func TestRunClassifyCategories_WriteError(t *testing.T) {
	if err := runClassifyCategories(alwaysErrorWriter{}); err == nil {
		t.Fatal("expected write error")
	}
}

func TestBuildClassifyCategoriesCmd_Executes(t *testing.T) {
	cmd := buildClassifyCategoriesCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetArgs(nil)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !strings.Contains(buf.String(), "internal") {
		t.Fatalf("unexpected output: %q", buf.String())
	}
}

func TestRunClassifyIntents(t *testing.T) {
	var buf bytes.Buffer
	if err := runClassifyIntents(&buf); err != nil {
		t.Fatalf("runClassifyIntents: %v", err)
	}
	if !strings.Contains(buf.String(), "invoice") || !strings.Contains(buf.String(), "request-for-information") {
		t.Fatalf("unexpected intents output: %q", buf.String())
	}
}

func TestRunClassifyIntents_WriteError(t *testing.T) {
	if err := runClassifyIntents(alwaysErrorWriter{}); err == nil {
		t.Fatal("expected write error")
	}
}

func TestBuildClassifyIntentsCmd_Executes(t *testing.T) {
	cmd := buildClassifyIntentsCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetArgs(nil)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !strings.Contains(buf.String(), "invoice") {
		t.Fatalf("unexpected output: %q", buf.String())
	}
}

func TestRunClassifyPatternTypes(t *testing.T) {
	var buf bytes.Buffer
	if err := runClassifyPatternTypes(&buf); err != nil {
		t.Fatalf("runClassifyPatternTypes: %v", err)
	}
	if !strings.Contains(buf.String(), "domain") || !strings.Contains(buf.String(), "subject_term") || !strings.Contains(buf.String(), "label") {
		t.Fatalf("unexpected pattern types output: %q", buf.String())
	}
}

func TestRunClassifyPatternTypes_WriteError(t *testing.T) {
	if err := runClassifyPatternTypes(alwaysErrorWriter{}); err == nil {
		t.Fatal("expected write error")
	}
}

func TestBuildClassifyPatternTypesCmd_Executes(t *testing.T) {
	cmd := buildClassifyPatternTypesCmd()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetArgs(nil)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !strings.Contains(buf.String(), "domain") {
		t.Fatalf("unexpected output: %q", buf.String())
	}
}

func TestRunClassifyLabelAnalysis_Table(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")

	st, err := storage.Open(cfg.StoragePath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.CreateMailbox(context.Background(), models.Mailbox{ID: "user@example.com", Provider: "gmail"}); err != nil {
		t.Fatalf("CreateMailbox: %v", err)
	}
	now := time.Date(2026, 4, 9, 12, 0, 0, 0, time.UTC)
	for _, msg := range []models.MessageMeta{
		{ProviderID: "l1", MailboxID: "user@example.com", Provider: "gmail", Labels: []string{"CATEGORY_PROMOTIONS", "INBOX"}, ReceivedAt: now},
		{ProviderID: "l2", MailboxID: "user@example.com", Provider: "gmail", Labels: []string{"INBOX"}, ReceivedAt: now.Add(time.Minute)},
	} {
		if err := st.UpsertMessage(context.Background(), msg); err != nil {
			t.Fatalf("UpsertMessage(%s): %v", msg.ProviderID, err)
		}
	}
	_ = st.Close()

	var buf bytes.Buffer
	if err := runClassifyLabelAnalysis(context.Background(), &buf, cfg, "user@example.com", "table", 1); err != nil {
		t.Fatalf("runClassifyLabelAnalysis: %v", err)
	}
	output := buf.String()
	if !strings.Contains(output, "LABEL") || !strings.Contains(output, "MESSAGES") || !strings.Contains(output, "INBOX") {
		t.Fatalf("unexpected table output: %q", output)
	}
}

func TestRunClassifyLabelAnalysis_JSON(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")

	st, err := storage.Open(cfg.StoragePath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.CreateMailbox(context.Background(), models.Mailbox{ID: "user@example.com", Provider: "gmail"}); err != nil {
		t.Fatalf("CreateMailbox: %v", err)
	}
	if err := st.UpsertMessage(context.Background(), models.MessageMeta{
		ProviderID: "l1", MailboxID: "user@example.com", Provider: "gmail",
		Labels: []string{"INBOX"}, ReceivedAt: time.Date(2026, 4, 9, 12, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatalf("UpsertMessage: %v", err)
	}
	_ = st.Close()

	var buf bytes.Buffer
	if err := runClassifyLabelAnalysis(context.Background(), &buf, cfg, "user@example.com", "json", 1); err != nil {
		t.Fatalf("runClassifyLabelAnalysis: %v", err)
	}
	if !strings.Contains(buf.String(), "\"Label\": \"INBOX\"") && !strings.Contains(buf.String(), "\"label\": \"INBOX\"") {
		t.Fatalf("unexpected json output: %q", buf.String())
	}
}

func TestRunClassifyLabelAnalysis_Empty(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")

	st, err := storage.Open(cfg.StoragePath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.CreateMailbox(context.Background(), models.Mailbox{ID: "user@example.com", Provider: "gmail"}); err != nil {
		t.Fatalf("CreateMailbox: %v", err)
	}
	_ = st.Close()

	var buf bytes.Buffer
	if err := runClassifyLabelAnalysis(context.Background(), &buf, cfg, "user@example.com", "table", 1); err != nil {
		t.Fatalf("runClassifyLabelAnalysis: %v", err)
	}
	if !strings.Contains(buf.String(), "No label stats found for user@example.com.") {
		t.Fatalf("unexpected empty output: %q", buf.String())
	}
}

func TestBuildClassifySeedsDeleteCmd_RequiresFlags(t *testing.T) {
	cmd := buildClassifySeedsDeleteCmd(config.Default())
	cmd.SetArgs([]string{"--account", "user@example.com"})
	if err := cmd.Execute(); err == nil {
		t.Fatal("expected missing flag error")
	}
}

func TestBuildClassifyPromoteCmd_RequiresFlags(t *testing.T) {
	cmd := buildClassifyPromoteCmd(config.Default())
	cmd.SetArgs([]string{"--account", "user@example.com"})
	if err := cmd.Execute(); err == nil {
		t.Fatal("expected missing flag error")
	}
}

func TestRunReportExport_ExcelWritesDeterministicFilename(t *testing.T) {
	outputDir := t.TempDir()
	reportsDir := filepath.Join("..", "..", "internal", "export", "testdata", "valid")

	var buf bytes.Buffer
	if err := runReportExport(context.Background(), &buf, reportExportOptions{
		reportsDir: reportsDir,
		outputDir:  outputDir,
		format:     "excel",
		ownerEmail: "owner@company.com",
	}); err != nil {
		t.Fatalf("runReportExport: %v", err)
	}

	wantPath := filepath.Join(outputDir, "inbox-report-owner-company-com-2025-01-to-2025-03.xlsx")
	data, err := os.ReadFile(wantPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("expected workbook bytes")
	}
	if !strings.Contains(buf.String(), wantPath) {
		t.Fatalf("expected output to mention %s, got %q", wantPath, buf.String())
	}
}

func TestRunReportExport_HTMLRequiresSummaryFile(t *testing.T) {
	err := runReportExport(context.Background(), io.Discard, reportExportOptions{
		reportsDir: filepath.Join("..", "..", "internal", "export", "testdata", "valid"),
		outputDir:  t.TempDir(),
		format:     "html",
		ownerEmail: "owner@company.com",
	})
	if err == nil {
		t.Fatal("expected summary file requirement error")
	}
	if !strings.Contains(err.Error(), "--summary-file is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunReportSummarize_WritesCanonicalSummary(t *testing.T) {
	reportsDir := filepath.Join("..", "..", "internal", "export", "testdata", "valid")
	outputFile := filepath.Join(t.TempDir(), "summary.md")

	prevReadPrompt := readSummaryPromptFile
	prevProvider := newSummaryProvider
	t.Cleanup(func() {
		readSummaryPromptFile = prevReadPrompt
		newSummaryProvider = prevProvider
	})

	readSummaryPromptFile = func(path string) ([]byte, error) {
		if path != "test-prompt.md" {
			t.Fatalf("unexpected prompt path: %s", path)
		}
		return []byte("prompt"), nil
	}
	newSummaryProvider = func(command string, args []string) exportpkg.SummaryProvider {
		if command != "stub-provider" {
			t.Fatalf("unexpected provider command: %s", command)
		}
		if len(args) != 1 || args[0] != "arg1" {
			t.Fatalf("unexpected provider args: %+v", args)
		}
		return summaryProviderStub{
			output: exportpkg.SummaryOutput{
				Title:                "Inbox Snapshot",
				Subtitle:             "owner@company.com",
				Headline:             "Email volume increased from 5 to 8 messages across 2025-01 to 2025-03.",
				SecondaryHeadline:    "alerts@vendor.com remained the top external sender with 9 messages.",
				SnapshotBullets:      []string{"vendor.com is the most active external domain with 9 messages."},
				WhatThisMeansBullets: []string{"A small number of external sources shape most of the 20 messages in scope."},
				OpportunitiesBullets: []string{"Promote repeated alerts from alerts@vendor.com into a review workflow."},
				BottomLine:           "The inbox shows stable patterns that are ready for structured automation review.",
			},
		}
	}

	var buf bytes.Buffer
	err := runReportSummarize(context.Background(), &buf, reportSummarizeOptions{
		reportsDir:      reportsDir,
		outputFile:      outputFile,
		ownerEmail:      "owner@company.com",
		providerCommand: "stub-provider",
		providerArgs:    []string{"arg1"},
		promptFile:      "test-prompt.md",
	})
	if err != nil {
		t.Fatalf("runReportSummarize: %v", err)
	}
	body, err := os.ReadFile(outputFile)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !strings.Contains(string(body), "## Key Takeaway") || !strings.Contains(string(body), "alerts@vendor.com") {
		t.Fatalf("unexpected summary body: %s", string(body))
	}
	if !strings.Contains(buf.String(), outputFile) {
		t.Fatalf("expected output path in command output, got %q", buf.String())
	}
}

func TestRunReportSummarize_InvalidProviderOutputDoesNotWriteFile(t *testing.T) {
	reportsDir := filepath.Join("..", "..", "internal", "export", "testdata", "valid")
	outputFile := filepath.Join(t.TempDir(), "summary.md")

	prevReadPrompt := readSummaryPromptFile
	prevProvider := newSummaryProvider
	t.Cleanup(func() {
		readSummaryPromptFile = prevReadPrompt
		newSummaryProvider = prevProvider
	})

	readSummaryPromptFile = func(path string) ([]byte, error) {
		return []byte("prompt"), nil
	}
	newSummaryProvider = func(command string, args []string) exportpkg.SummaryProvider {
		return summaryProviderStub{
			output: exportpkg.SummaryOutput{
				Headline:             "Email volume increased from 5 to 999 messages across 2025-01 to 2025-03.",
				SecondaryHeadline:    "alerts@vendor.com remained the top external sender with 9 messages.",
				SnapshotBullets:      []string{"vendor.com is the most active external domain with 9 messages."},
				WhatThisMeansBullets: []string{"A small number of external sources shape most of the 20 messages in scope."},
				OpportunitiesBullets: []string{"Promote repeated alerts from alerts@vendor.com into a review workflow."},
				BottomLine:           "The inbox shows stable patterns that are ready for structured automation review.",
			},
		}
	}

	err := runReportSummarize(context.Background(), io.Discard, reportSummarizeOptions{
		reportsDir:      reportsDir,
		outputFile:      outputFile,
		ownerEmail:      "owner@company.com",
		providerCommand: "stub-provider",
		promptFile:      "test-prompt.md",
	})
	if err == nil {
		t.Fatal("expected invalid provider output error")
	}
	if _, statErr := os.Stat(outputFile); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("expected no summary file, stat err=%v", statErr)
	}
}

func TestRunReportSummarize_RequiresProviderCommand(t *testing.T) {
	prevReadPrompt := readSummaryPromptFile
	t.Cleanup(func() { readSummaryPromptFile = prevReadPrompt })
	readSummaryPromptFile = func(path string) ([]byte, error) { return []byte("prompt"), nil }

	err := runReportSummarize(context.Background(), io.Discard, reportSummarizeOptions{
		reportsDir: filepath.Join("..", "..", "internal", "export", "testdata", "valid"),
		ownerEmail: "owner@company.com",
		promptFile: "test-prompt.md",
	})
	if err == nil {
		t.Fatal("expected provider command error")
	}
	if !strings.Contains(err.Error(), "summary provider command is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBuildReportSummarizeCmd(t *testing.T) {
	cmd := buildReportSummarizeCmd(config.Default())
	if cmd.Name() != "summarize" {
		t.Fatalf("unexpected command name: %s", cmd.Name())
	}
}

func TestRunReportExport_HTMLWritesSnapshot(t *testing.T) {
	outputDir := t.TempDir()
	reportsDir := filepath.Join("..", "..", "internal", "export", "testdata", "valid")
	summaryPath := filepath.Join(reportsDir, "summary.md")

	var buf bytes.Buffer
	if err := runReportExport(context.Background(), &buf, reportExportOptions{
		reportsDir:  reportsDir,
		outputDir:   outputDir,
		format:      "html",
		ownerEmail:  "owner@company.com",
		summaryFile: summaryPath,
	}); err != nil {
		t.Fatalf("runReportExport: %v", err)
	}

	wantPath := filepath.Join(outputDir, "inbox-report-owner-company-com-2025-01-to-2025-03.html")
	data, err := os.ReadFile(wantPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !strings.Contains(string(data), "<h2>Bottom Line</h2>") {
		t.Fatalf("expected snapshot html, got %q", string(data))
	}
}

func TestRunReportExport_PDFUnavailable(t *testing.T) {
	previous := reportExportPDFRenderer
	reportExportPDFRenderer = nil
	t.Cleanup(func() { reportExportPDFRenderer = previous })

	err := runReportExport(context.Background(), io.Discard, reportExportOptions{
		reportsDir:  filepath.Join("..", "..", "internal", "export", "testdata", "valid"),
		outputDir:   t.TempDir(),
		format:      "pdf",
		ownerEmail:  "owner@company.com",
		summaryFile: filepath.Join("..", "..", "internal", "export", "testdata", "valid", "summary.md"),
	})
	if err == nil {
		t.Fatal("expected pdf unavailable error")
	}
	if !errors.Is(err, exportpkg.ErrPDFRendererUnavailable) {
		t.Fatalf("expected ErrPDFRendererUnavailable, got %v", err)
	}
}

func TestRunReportExport_AllWritesDeterministicArtifacts(t *testing.T) {
	outputDir := t.TempDir()
	reportsDir := filepath.Join("..", "..", "internal", "export", "testdata", "valid")
	summaryPath := filepath.Join(reportsDir, "summary.md")

	previous := reportExportPDFRenderer
	reportExportPDFRenderer = reportExportPDFStub{returnBytes: []byte("%PDF-1.7")}
	t.Cleanup(func() { reportExportPDFRenderer = previous })

	if err := runReportExport(context.Background(), io.Discard, reportExportOptions{
		reportsDir:  reportsDir,
		outputDir:   outputDir,
		format:      "all",
		ownerEmail:  "owner@company.com",
		summaryFile: summaryPath,
	}); err != nil {
		t.Fatalf("runReportExport: %v", err)
	}

	for _, suffix := range []string{".xlsx", ".html", ".pdf"} {
		path := filepath.Join(outputDir, "inbox-report-owner-company-com-2025-01-to-2025-03"+suffix)
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected %s to exist: %v", path, err)
		}
	}
}

func TestRunReportExport_AllDoesNotLeavePartialOutputsWhenPDFFails(t *testing.T) {
	outputDir := t.TempDir()
	reportsDir := filepath.Join("..", "..", "internal", "export", "testdata", "valid")
	summaryPath := filepath.Join(reportsDir, "summary.md")

	previous := reportExportPDFRenderer
	reportExportPDFRenderer = reportExportPDFStub{err: errors.New("renderer missing")}
	t.Cleanup(func() { reportExportPDFRenderer = previous })

	err := runReportExport(context.Background(), io.Discard, reportExportOptions{
		reportsDir:  reportsDir,
		outputDir:   outputDir,
		format:      "all",
		ownerEmail:  "owner@company.com",
		summaryFile: summaryPath,
	})
	if err == nil {
		t.Fatal("expected renderer failure")
	}
	entries, readErr := os.ReadDir(outputDir)
	if readErr != nil {
		t.Fatalf("ReadDir: %v", readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("expected no partial output files, found %d", len(entries))
	}
}

func TestRunReportExport_InvalidReportsDirLeavesNoOutputs(t *testing.T) {
	outputDir := t.TempDir()
	err := runReportExport(context.Background(), io.Discard, reportExportOptions{
		reportsDir: filepath.Join(outputDir, "missing"),
		outputDir:  outputDir,
		format:     "excel",
	})
	if err == nil {
		t.Fatal("expected invalid reports dir error")
	}
	entries, readErr := os.ReadDir(outputDir)
	if readErr != nil {
		t.Fatalf("ReadDir: %v", readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("expected no output files, found %d", len(entries))
	}
}

func TestRunReportExport_MalformedSummaryLeavesNoOutputs(t *testing.T) {
	outputDir := t.TempDir()
	reportsDir := filepath.Join("..", "..", "internal", "export", "testdata", "valid")
	badSummary := filepath.Join(outputDir, "bad-summary.md")
	if err := os.WriteFile(badSummary, []byte("# Inbox Snapshot\n\n## Key Takeaway\nOnly one section.\n"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	err := runReportExport(context.Background(), io.Discard, reportExportOptions{
		reportsDir:  reportsDir,
		outputDir:   outputDir,
		format:      "html",
		ownerEmail:  "owner@company.com",
		summaryFile: badSummary,
	})
	if err == nil {
		t.Fatal("expected malformed summary error")
	}
	entries, readErr := os.ReadDir(outputDir)
	if readErr != nil {
		t.Fatalf("ReadDir: %v", readErr)
	}
	if len(entries) != 1 || entries[0].Name() != "bad-summary.md" {
		t.Fatalf("expected only fixture file to remain, found %+v", entries)
	}
}

func TestBuildReportExportCmd_RunE_RequiresFlags(t *testing.T) {
	cmd := buildReportExportCmd(config.Default())
	cmd.SetArgs([]string{})
	if err := cmd.Execute(); err == nil {
		t.Fatal("expected missing required flags error")
	}
}

func TestValidateExportFormat(t *testing.T) {
	for _, format := range []string{"excel", "html", "pdf", "all"} {
		got, err := validateExportFormat(format)
		if err != nil {
			t.Fatalf("validateExportFormat(%q): %v", format, err)
		}
		if got != format {
			t.Fatalf("validateExportFormat(%q): got %q", format, got)
		}
	}
	if _, err := validateExportFormat("zip"); err == nil {
		t.Fatal("expected invalid format error")
	}
}

func TestExportBaseName_Fallbacks(t *testing.T) {
	model := &exportpkg.Model{
		Owner: exportpkg.Owner{Domain: "Example.COM"},
	}
	if got := exportBaseName(model); got != "inbox-report-example-com-unknown-period" {
		t.Fatalf("exportBaseName fallback: got %q", got)
	}
	if got := sanitizeExportPart("  a+b@example.com "); got != "a-b-example-com" {
		t.Fatalf("sanitizeExportPart: got %q", got)
	}
	if got := sanitizeExportPart("!!!"); got != "unknown" {
		t.Fatalf("sanitizeExportPart unknown: got %q", got)
	}
}

// --- runReportDomains ---

func TestRunReportDomains_HappyPath_Table(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")
	seedReportData(t, cfg.StoragePath)

	var buf bytes.Buffer
	if err := runReportDomains(context.Background(), &buf, cfg, "user@example.com", false, "table", 25); err != nil {
		t.Fatalf("runReportDomains: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "DOMAIN") {
		t.Errorf("expected DOMAIN header, got: %q", out)
	}
	if !strings.Contains(out, "foo.com") {
		t.Errorf("expected foo.com in output: %q", out)
	}
}

func TestRunReportDomains_CSV(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")
	seedReportData(t, cfg.StoragePath)

	var buf bytes.Buffer
	if err := runReportDomains(context.Background(), &buf, cfg, "user@example.com", false, "csv", 25); err != nil {
		t.Fatalf("runReportDomains csv: %v", err)
	}
	if !strings.Contains(buf.String(), "domain") {
		t.Errorf("expected csv header, got: %q", buf.String())
	}
}

func TestRunReportDomains_JSON(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")
	seedReportData(t, cfg.StoragePath)

	var buf bytes.Buffer
	if err := runReportDomains(context.Background(), &buf, cfg, "user@example.com", false, "json", 25); err != nil {
		t.Fatalf("runReportDomains json: %v", err)
	}
	if !strings.Contains(buf.String(), "[") {
		t.Errorf("expected JSON array, got: %q", buf.String())
	}
}

func TestRunReportDomains_UnknownFormat(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")
	seedReportData(t, cfg.StoragePath)

	err := runReportDomains(context.Background(), io.Discard, cfg, "user@example.com", false, "xml", 25)
	if err == nil {
		t.Error("expected error for unknown format")
	}
}

func TestRunReportDomains_MissingAccount(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")

	err := runReportDomains(context.Background(), io.Discard, cfg, "", false, "table", 25)
	if err == nil {
		t.Error("expected error for missing account")
	}
}

func TestRunReportDomains_BothAccountAndAllAccounts(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")
	seedReportData(t, cfg.StoragePath)

	err := runReportDomains(context.Background(), io.Discard, cfg, "user@example.com", true, "table", 25)
	if err == nil {
		t.Error("expected error when both --account and --all-accounts set")
	}
}

func TestRunReportDomains_AllAccounts(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")
	seedReportData(t, cfg.StoragePath)

	var buf bytes.Buffer
	if err := runReportDomains(context.Background(), &buf, cfg, "", true, "table", 25); err != nil {
		t.Fatalf("runReportDomains --all-accounts: %v", err)
	}
	if !strings.Contains(buf.String(), "foo.com") {
		t.Errorf("expected foo.com in all-accounts output: %q", buf.String())
	}
}

// --- runReportSenders ---

func TestRunReportSenders_HappyPath_Table(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")
	seedReportData(t, cfg.StoragePath)

	var buf bytes.Buffer
	if err := runReportSenders(context.Background(), &buf, cfg, "user@example.com", false, "table", 25); err != nil {
		t.Fatalf("runReportSenders: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "EMAIL") {
		t.Errorf("expected EMAIL header, got: %q", out)
	}
	if !strings.Contains(out, "alice@foo.com") {
		t.Errorf("expected alice@foo.com in output: %q", out)
	}
}

func TestRunReportSenders_UnknownFormat(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")
	seedReportData(t, cfg.StoragePath)

	if err := runReportSenders(context.Background(), io.Discard, cfg, "user@example.com", false, "xml", 25); err == nil {
		t.Error("expected error for unknown format")
	}
}

func TestRunReportSenders_MissingAccount(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")

	if err := runReportSenders(context.Background(), io.Discard, cfg, "", false, "table", 25); err == nil {
		t.Error("expected error for missing account")
	}
}

func TestRunReportSenders_AllAccounts(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")
	seedReportData(t, cfg.StoragePath)

	var buf bytes.Buffer
	if err := runReportSenders(context.Background(), &buf, cfg, "", true, "table", 25); err != nil {
		t.Fatalf("runReportSenders --all-accounts: %v", err)
	}
	if !strings.Contains(buf.String(), "alice@foo.com") {
		t.Errorf("expected alice@foo.com in all-accounts output: %q", buf.String())
	}
}

// --- runReportSubjects ---

func TestRunReportSubjects_HappyPath_Table(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")
	seedReportData(t, cfg.StoragePath)

	var buf bytes.Buffer
	if err := runReportSubjects(context.Background(), &buf, cfg, "user@example.com", false, "table", 25); err != nil {
		t.Fatalf("runReportSubjects: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "TERM") {
		t.Errorf("expected TERM header, got: %q", out)
	}
}

func TestRunReportSubjects_UnknownFormat(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")
	seedReportData(t, cfg.StoragePath)

	if err := runReportSubjects(context.Background(), io.Discard, cfg, "user@example.com", false, "xml", 25); err == nil {
		t.Error("expected error for unknown format")
	}
}

func TestRunReportSubjects_MissingAccount(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")

	if err := runReportSubjects(context.Background(), io.Discard, cfg, "", false, "table", 25); err == nil {
		t.Error("expected error for missing account")
	}
}

func TestRunReportSubjects_AllAccounts(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")
	seedReportData(t, cfg.StoragePath)

	var buf bytes.Buffer
	if err := runReportSubjects(context.Background(), &buf, cfg, "", true, "table", 25); err != nil {
		t.Fatalf("runReportSubjects --all-accounts: %v", err)
	}
	if !strings.Contains(buf.String(), "TERM") {
		t.Errorf("expected TERM header in all-accounts output: %q", buf.String())
	}
}

// --- runReportVolume ---

func TestRunReportVolume_HappyPath_Table(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")
	seedReportData(t, cfg.StoragePath)

	var buf bytes.Buffer
	if err := runReportVolume(context.Background(), &buf, cfg, "user@example.com", false, "table"); err != nil {
		t.Fatalf("runReportVolume: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "PERIOD") {
		t.Errorf("expected PERIOD header, got: %q", out)
	}
	if !strings.Contains(out, "2025-01") {
		t.Errorf("expected 2025-01 in output: %q", out)
	}
}

func TestRunReportVolume_CSV(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")
	seedReportData(t, cfg.StoragePath)

	var buf bytes.Buffer
	if err := runReportVolume(context.Background(), &buf, cfg, "user@example.com", false, "csv"); err != nil {
		t.Fatalf("runReportVolume csv: %v", err)
	}
	if !strings.Contains(buf.String(), "period") {
		t.Errorf("expected csv header, got: %q", buf.String())
	}
}

func TestRunReportVolume_JSON(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")
	seedReportData(t, cfg.StoragePath)

	var buf bytes.Buffer
	if err := runReportVolume(context.Background(), &buf, cfg, "user@example.com", false, "json"); err != nil {
		t.Fatalf("runReportVolume json: %v", err)
	}
	if !strings.Contains(buf.String(), "[") {
		t.Errorf("expected JSON array, got: %q", buf.String())
	}
}

func TestRunReportVolume_UnknownFormat(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")
	seedReportData(t, cfg.StoragePath)

	if err := runReportVolume(context.Background(), io.Discard, cfg, "user@example.com", false, "xml"); err == nil {
		t.Error("expected error for unknown format")
	}
}

func TestRunReportVolume_MissingAccount(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")

	if err := runReportVolume(context.Background(), io.Discard, cfg, "", false, "table"); err == nil {
		t.Error("expected error for missing account")
	}
}

func TestRunReportVolume_AllAccounts(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")
	seedReportData(t, cfg.StoragePath)

	var buf bytes.Buffer
	if err := runReportVolume(context.Background(), &buf, cfg, "", true, "table"); err != nil {
		t.Fatalf("runReportVolume --all-accounts: %v", err)
	}
	if !strings.Contains(buf.String(), "PERIOD") {
		t.Errorf("expected PERIOD header in all-accounts output: %q", buf.String())
	}
}

func TestRunReportVolume_BothAccountAndAllAccounts(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")
	seedReportData(t, cfg.StoragePath)

	if err := runReportVolume(context.Background(), io.Discard, cfg, "user@example.com", true, "table"); err == nil {
		t.Error("expected error when both --account and --all-accounts set")
	}
}

// --- buildReportDomainsCmd RunE flags ---

func TestBuildReportDomainsCmd_RunE_NoAccount(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")

	cmd := buildReportDomainsCmd(cfg)
	cmd.SetArgs([]string{})
	if err := cmd.Execute(); err == nil {
		t.Error("expected error when no account or --all-accounts given")
	}
}

func TestBuildReportDomainsCmd_RunE_OutputFile(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")
	seedReportData(t, cfg.StoragePath)

	outputPath := filepath.Join(dir, "domains.txt")
	cmd := buildReportDomainsCmd(cfg)
	cmd.SetArgs([]string{"--account", "user@example.com", "--output", outputPath})
	var stdout bytes.Buffer
	cmd.SetOut(&stdout)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("expected stdout to remain empty when --output is set, got %q", stdout.String())
	}
	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !strings.Contains(string(data), "foo.com") {
		t.Fatalf("expected file output to contain report data, got %q", string(data))
	}
}

func TestBuildReportDomainsCmd_RunE_OutputFileError(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")
	seedReportData(t, cfg.StoragePath)

	outputPath := filepath.Join(dir, "missing", "domains.txt")
	cmd := buildReportDomainsCmd(cfg)
	cmd.SetArgs([]string{"--account", "user@example.com", "--output", outputPath})
	if err := cmd.Execute(); err == nil {
		t.Fatal("expected file output error")
	}
}

func TestBuildReportVolumeCmd_RunE_NoAccount(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")

	cmd := buildReportVolumeCmd(cfg)
	cmd.SetArgs([]string{})
	if err := cmd.Execute(); err == nil {
		t.Error("expected error when no account or --all-accounts given")
	}
}

func TestBuildReportSendersCmd_RunE_OutputFile(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")
	seedReportData(t, cfg.StoragePath)

	outputPath := filepath.Join(dir, "senders.txt")
	cmd := buildReportSendersCmd(cfg)
	cmd.SetArgs([]string{"--account", "user@example.com", "--output", outputPath})
	var stdout bytes.Buffer
	cmd.SetOut(&stdout)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("expected stdout to remain empty when --output is set, got %q", stdout.String())
	}
	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !strings.Contains(string(data), "alice@foo.com") {
		t.Fatalf("expected file output to contain sender data, got %q", string(data))
	}
}

func TestBuildReportSubjectsCmd_RunE_OutputFile(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")
	seedReportData(t, cfg.StoragePath)

	outputPath := filepath.Join(dir, "subjects.txt")
	cmd := buildReportSubjectsCmd(cfg)
	cmd.SetArgs([]string{"--account", "user@example.com", "--output", outputPath})
	var stdout bytes.Buffer
	cmd.SetOut(&stdout)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("expected stdout to remain empty when --output is set, got %q", stdout.String())
	}
	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !strings.Contains(string(data), "meeting") {
		t.Fatalf("expected file output to contain subject data, got %q", string(data))
	}
}

func TestBuildReportVolumeCmd_RunE_OutputFile(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(dir, "test.db")
	seedReportData(t, cfg.StoragePath)

	outputPath := filepath.Join(dir, "volume.txt")
	cmd := buildReportVolumeCmd(cfg)
	cmd.SetArgs([]string{"--account", "user@example.com", "--output", outputPath})
	var stdout bytes.Buffer
	cmd.SetOut(&stdout)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("expected stdout to remain empty when --output is set, got %q", stdout.String())
	}
	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !strings.Contains(string(data), "2025-01") {
		t.Fatalf("expected file output to contain volume data, got %q", string(data))
	}
}

type reportExportPDFStub struct {
	returnBytes []byte
	err         error
}

func (s reportExportPDFStub) RenderPDF(_ []byte) ([]byte, error) {
	if s.err != nil {
		return nil, s.err
	}
	return append([]byte(nil), s.returnBytes...), nil
}

type summaryProviderStub struct {
	output exportpkg.SummaryOutput
	err    error
}

func (s summaryProviderStub) GenerateSummary(_ context.Context, _ string, _ exportpkg.SummaryInput) (exportpkg.SummaryOutput, error) {
	if s.err != nil {
		return exportpkg.SummaryOutput{}, s.err
	}
	return s.output, nil
}
