package ingestion

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/UreaLaden/inboxatlas/internal/providers/mock"
	"github.com/UreaLaden/inboxatlas/internal/storage"
	"github.com/UreaLaden/inboxatlas/pkg/models"
)

// mockRetryableError is a sentinel error that signals the ingestion pipeline to
// retry with backoff.
type mockRetryableError struct{}

func (e *mockRetryableError) Error() string     { return "retryable" }
func (e *mockRetryableError) IsRetryable() bool { return true }

// callbackProvider is a flexible test double for models.MailProvider that
// delegates each method call to an injected function.
type callbackProvider struct {
	authenticate   func(context.Context) error
	listMessages   func(context.Context, string) ([]string, string, error)
	getMessageMeta func(context.Context, string) (*models.MessageMeta, error)
}

func (p *callbackProvider) Authenticate(ctx context.Context) error {
	if p.authenticate != nil {
		return p.authenticate(ctx)
	}
	return nil
}

func (p *callbackProvider) ListMessages(ctx context.Context, token string) ([]string, string, error) {
	if p.listMessages != nil {
		return p.listMessages(ctx, token)
	}
	return nil, "", nil
}

func (p *callbackProvider) GetMessageMeta(ctx context.Context, id string) (*models.MessageMeta, error) {
	if p.getMessageMeta != nil {
		return p.getMessageMeta(ctx, id)
	}
	return &models.MessageMeta{ProviderID: id, MailboxID: "user@example.com", Provider: "gmail"}, nil
}

// newTestStore opens an in-memory storage store and registers a cleanup.
// It also creates the canonical test mailbox so FK constraints are satisfied.
func newTestStore(t *testing.T) *storage.Store {
	t.Helper()
	st, err := storage.Open(":memory:")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.CreateMailbox(context.Background(), models.Mailbox{
		ID: "user@example.com", Provider: "gmail",
	}); err != nil {
		t.Fatalf("create mailbox: %v", err)
	}
	return st
}

// newMessages creates n test MessageMeta values with predictable IDs.
func newMessages(n int) []*models.MessageMeta {
	msgs := make([]*models.MessageMeta, n)
	for i := range msgs {
		msgs[i] = &models.MessageMeta{
			ProviderID: fmt.Sprintf("msg-%03d", i+1),
			MailboxID:  "user@example.com",
			Provider:   "gmail",
			Subject:    fmt.Sprintf("Subject %d", i+1),
		}
	}
	return msgs
}

// defaultOpts returns a baseline Options with zero-duration delays for fast tests.
func defaultOpts(p models.MailProvider, st *storage.Store, stdout *strings.Builder) Options {
	return Options{
		MailboxID:    "user@example.com",
		Provider:     "gmail",
		MailProvider: p,
		Store:        st,
		Stdout:       stdout,
		RequestDelay: time.Millisecond, // fast but non-zero so sleepCtx is exercised
		MaxRetries:   2,
	}
}

func TestRun_HappyPath_SinglePage(t *testing.T) {
	st := newTestStore(t)
	p := &mock.Provider{Messages: newMessages(3), PageSize: 10}
	var out strings.Builder

	if err := Run(context.Background(), defaultOpts(p, st, &out)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	cp, err := st.GetCheckpoint(context.Background(), "user@example.com", "gmail")
	if err != nil || cp == nil {
		t.Fatalf("GetCheckpoint: %v / %v", cp, err)
	}
	if cp.Status != "completed" {
		t.Errorf("Status = %q, want %q", cp.Status, "completed")
	}
	if cp.MessagesSynced != 3 {
		t.Errorf("MessagesSynced = %d, want 3", cp.MessagesSynced)
	}
	if !strings.Contains(out.String(), "Sync complete: 3 messages synced.") {
		t.Errorf("expected completion line in output, got:\n%s", out.String())
	}
}

func TestRun_HappyPath_MultiPage(t *testing.T) {
	st := newTestStore(t)
	p := &mock.Provider{Messages: newMessages(25), PageSize: 10}
	var out strings.Builder

	if err := Run(context.Background(), defaultOpts(p, st, &out)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	cp, _ := st.GetCheckpoint(context.Background(), "user@example.com", "gmail")
	if cp == nil || cp.Status != "completed" {
		t.Fatalf("expected completed checkpoint, got %v", cp)
	}
	if cp.MessagesSynced != 25 {
		t.Errorf("MessagesSynced = %d, want 25", cp.MessagesSynced)
	}
	// Three full pages: 10+10+5
	if !strings.Contains(out.String(), "Page 3:") {
		t.Errorf("expected at least 3 page lines, output:\n%s", out.String())
	}
}

func TestRun_Resume_Interrupted(t *testing.T) {
	st := newTestStore(t)
	// Save a checkpoint that simulates interrupted at page 2 (offset 10)
	_ = st.SaveCheckpoint(context.Background(), storage.SyncCheckpoint{
		MailboxID:      "user@example.com",
		Provider:       "gmail",
		PageCursor:     "10",
		MessagesSynced: 10,
		Status:         "interrupted",
		StartedAt:      time.Now().Add(-time.Minute),
		UpdatedAt:      time.Now().Add(-time.Minute),
	})

	p := &mock.Provider{Messages: newMessages(25), PageSize: 10}
	var out strings.Builder

	if err := Run(context.Background(), defaultOpts(p, st, &out)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	cp, _ := st.GetCheckpoint(context.Background(), "user@example.com", "gmail")
	if cp.Status != "completed" {
		t.Errorf("Status = %q, want completed", cp.Status)
	}
	// Should have synced 15 new messages (11-25) + 10 already counted = 25 total
	if cp.MessagesSynced != 25 {
		t.Errorf("MessagesSynced = %d, want 25", cp.MessagesSynced)
	}
	if !strings.Contains(out.String(), "Resuming") {
		t.Errorf("expected 'Resuming' in output, got:\n%s", out.String())
	}
}

func TestRun_Resume_Running(t *testing.T) {
	st := newTestStore(t)
	_ = st.SaveCheckpoint(context.Background(), storage.SyncCheckpoint{
		MailboxID:      "user@example.com",
		Provider:       "gmail",
		PageCursor:     "10",
		MessagesSynced: 10,
		Status:         "running",
		StartedAt:      time.Now().Add(-time.Minute),
		UpdatedAt:      time.Now().Add(-time.Minute),
	})

	p := &mock.Provider{Messages: newMessages(25), PageSize: 10}
	var out strings.Builder

	if err := Run(context.Background(), defaultOpts(p, st, &out)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	cp, _ := st.GetCheckpoint(context.Background(), "user@example.com", "gmail")
	if cp.Status != "completed" {
		t.Errorf("Status = %q, want completed", cp.Status)
	}
	if !strings.Contains(out.String(), "Resuming") {
		t.Errorf("expected 'Resuming' in output")
	}
}

func TestRun_NoResume_Completed(t *testing.T) {
	st := newTestStore(t)
	// Save a completed checkpoint — should trigger fresh start
	_ = st.SaveCheckpoint(context.Background(), storage.SyncCheckpoint{
		MailboxID:      "user@example.com",
		Provider:       "gmail",
		PageCursor:     "",
		MessagesSynced: 5,
		Status:         "completed",
		StartedAt:      time.Now().Add(-time.Minute),
		UpdatedAt:      time.Now().Add(-time.Minute),
	})

	p := &mock.Provider{Messages: newMessages(3), PageSize: 10}
	var out strings.Builder

	if err := Run(context.Background(), defaultOpts(p, st, &out)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	cp, _ := st.GetCheckpoint(context.Background(), "user@example.com", "gmail")
	if cp.MessagesSynced != 3 {
		t.Errorf("MessagesSynced = %d, want 3 (fresh sync)", cp.MessagesSynced)
	}
	if strings.Contains(out.String(), "Resuming") {
		t.Error("should not print 'Resuming' for completed checkpoint")
	}
}

func TestRun_ListError_NonRetryable(t *testing.T) {
	st := newTestStore(t)
	listCalls := 0
	p := &callbackProvider{
		listMessages: func(_ context.Context, token string) ([]string, string, error) {
			listCalls++
			if listCalls == 1 {
				return []string{"msg-001"}, "1", nil // first page OK
			}
			return nil, "", fmt.Errorf("permanent error") // second page fails
		},
		getMessageMeta: func(_ context.Context, id string) (*models.MessageMeta, error) {
			return &models.MessageMeta{ProviderID: id, MailboxID: "user@example.com", Provider: "gmail"}, nil
		},
	}
	var out strings.Builder

	err := Run(context.Background(), defaultOpts(p, st, &out))
	if err == nil {
		t.Fatal("expected error from non-retryable list error")
	}

	cp, _ := st.GetCheckpoint(context.Background(), "user@example.com", "gmail")
	if cp == nil || cp.Status != "interrupted" {
		t.Errorf("expected interrupted checkpoint, got %v", cp)
	}
}

func TestRun_GetMetaError_NonRetryable(t *testing.T) {
	st := newTestStore(t)
	p := &callbackProvider{
		listMessages: func(_ context.Context, _ string) ([]string, string, error) {
			return []string{"msg-001"}, "", nil
		},
		getMessageMeta: func(_ context.Context, _ string) (*models.MessageMeta, error) {
			return nil, fmt.Errorf("permanent get error")
		},
	}
	var out strings.Builder

	err := Run(context.Background(), defaultOpts(p, st, &out))
	if err == nil {
		t.Fatal("expected error from non-retryable get-meta error")
	}

	cp, _ := st.GetCheckpoint(context.Background(), "user@example.com", "gmail")
	if cp == nil || cp.Status != "interrupted" {
		t.Errorf("expected interrupted checkpoint, got %v", cp)
	}
}

func TestRun_BackoffRetryable(t *testing.T) {
	// Inject a very short backoff delay so the test runs fast.
	orig := backoffInitialDelay
	backoffInitialDelay = time.Millisecond
	t.Cleanup(func() { backoffInitialDelay = orig })

	st := newTestStore(t)
	calls := 0
	p := &callbackProvider{
		listMessages: func(_ context.Context, _ string) ([]string, string, error) {
			calls++
			if calls == 1 {
				return nil, "", &mockRetryableError{} // first call: retryable
			}
			return []string{"msg-001"}, "", nil // second call: success
		},
		getMessageMeta: func(_ context.Context, id string) (*models.MessageMeta, error) {
			return &models.MessageMeta{ProviderID: id, MailboxID: "user@example.com", Provider: "gmail"}, nil
		},
	}
	var out strings.Builder
	opts := defaultOpts(p, st, &out)
	opts.MaxRetries = 2

	if err := Run(context.Background(), opts); err != nil {
		t.Fatalf("Run: %v", err)
	}

	cp, _ := st.GetCheckpoint(context.Background(), "user@example.com", "gmail")
	if cp == nil || cp.Status != "completed" {
		t.Errorf("expected completed checkpoint after backoff retry")
	}
}

func TestRun_BackoffExhaustion(t *testing.T) {
	orig := backoffInitialDelay
	backoffInitialDelay = time.Millisecond
	t.Cleanup(func() { backoffInitialDelay = orig })

	st := newTestStore(t)
	p := &callbackProvider{
		listMessages: func(_ context.Context, _ string) ([]string, string, error) {
			return nil, "", &mockRetryableError{}
		},
	}
	var out strings.Builder
	opts := defaultOpts(p, st, &out)
	opts.MaxRetries = 2

	err := Run(context.Background(), opts)
	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
}

func TestRun_ContextCancellation(t *testing.T) {
	st := newTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())

	p := &callbackProvider{
		listMessages: func(_ context.Context, _ string) ([]string, string, error) {
			return []string{"msg-001"}, "", nil
		},
		getMessageMeta: func(_ context.Context, id string) (*models.MessageMeta, error) {
			cancel() // cancel context after first fetch
			return &models.MessageMeta{ProviderID: id, MailboxID: "user@example.com", Provider: "gmail"}, nil
		},
	}
	var out strings.Builder
	opts := defaultOpts(p, st, &out)
	opts.RequestDelay = 5 * time.Millisecond // ensure sleepCtx is hit

	err := Run(ctx, opts)
	if err == nil {
		t.Fatal("expected context cancellation error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled in error chain, got %v", err)
	}

	cp, _ := st.GetCheckpoint(context.Background(), "user@example.com", "gmail")
	if cp == nil || cp.Status != "interrupted" {
		t.Errorf("expected interrupted checkpoint after cancellation, got %v", cp)
	}
}

func TestRun_Idempotent(t *testing.T) {
	st := newTestStore(t)
	msgs := newMessages(5)
	p := &mock.Provider{Messages: msgs, PageSize: 10}
	var out strings.Builder

	if err := Run(context.Background(), defaultOpts(p, st, &out)); err != nil {
		t.Fatalf("first Run: %v", err)
	}

	// Second run — checkpoint is "completed" so fresh sync starts, but
	// INSERT OR IGNORE means no duplicates are inserted.
	p2 := &mock.Provider{Messages: msgs, PageSize: 10}
	var out2 strings.Builder
	if err := Run(context.Background(), defaultOpts(p2, st, &out2)); err != nil {
		t.Fatalf("second Run: %v", err)
	}

	// The second run should complete successfully with 5 messages synced
	cp, _ := st.GetCheckpoint(context.Background(), "user@example.com", "gmail")
	if cp.Status != "completed" || cp.MessagesSynced != 5 {
		t.Errorf("second run: status=%q synced=%d, want completed/5", cp.Status, cp.MessagesSynced)
	}
}

func TestRun_EmptyMailbox(t *testing.T) {
	st := newTestStore(t)
	p := &mock.Provider{Messages: nil}
	var out strings.Builder

	if err := Run(context.Background(), defaultOpts(p, st, &out)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	cp, _ := st.GetCheckpoint(context.Background(), "user@example.com", "gmail")
	if cp == nil || cp.Status != "completed" {
		t.Errorf("expected completed checkpoint for empty mailbox, got %v", cp)
	}
	if cp.MessagesSynced != 0 {
		t.Errorf("MessagesSynced = %d, want 0", cp.MessagesSynced)
	}
}

func TestRun_ProgressOutput(t *testing.T) {
	st := newTestStore(t)
	p := &mock.Provider{Messages: newMessages(5), PageSize: 10}
	var out strings.Builder

	if err := Run(context.Background(), defaultOpts(p, st, &out)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	output := out.String()
	for _, want := range []string{"Page 1:", "total:", "Sync complete:"} {
		if !strings.Contains(output, want) {
			t.Errorf("expected %q in output:\n%s", want, output)
		}
	}
}

func TestRun_GetMetaBackoffRetryable(t *testing.T) {
	orig := backoffInitialDelay
	backoffInitialDelay = time.Millisecond
	t.Cleanup(func() { backoffInitialDelay = orig })

	st := newTestStore(t)
	getCalls := 0
	p := &callbackProvider{
		listMessages: func(_ context.Context, _ string) ([]string, string, error) {
			return []string{"msg-001"}, "", nil
		},
		getMessageMeta: func(_ context.Context, id string) (*models.MessageMeta, error) {
			getCalls++
			if getCalls == 1 {
				return nil, &mockRetryableError{} // first call: retryable
			}
			return &models.MessageMeta{ProviderID: id, MailboxID: "user@example.com", Provider: "gmail"}, nil
		},
	}
	var out strings.Builder
	opts := defaultOpts(p, st, &out)
	opts.MaxRetries = 2

	if err := Run(context.Background(), opts); err != nil {
		t.Fatalf("Run: %v", err)
	}

	cp, _ := st.GetCheckpoint(context.Background(), "user@example.com", "gmail")
	if cp == nil || cp.Status != "completed" {
		t.Errorf("expected completed checkpoint after getMeta backoff retry")
	}
}

// TestSleepCtx_ZeroDelay exercises the d<=0 branch of sleepCtx directly.
func TestSleepCtx_ZeroDelay(t *testing.T) {
	// With zero delay and a live context, sleepCtx should return nil.
	if err := sleepCtx(context.Background(), 0); err != nil {
		t.Errorf("sleepCtx(0) with live ctx: got %v, want nil", err)
	}

	// With zero delay and a cancelled context, sleepCtx should return ctx.Err().
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sleepCtx(ctx, 0); !errors.Is(err, context.Canceled) {
		t.Errorf("sleepCtx(0) with cancelled ctx: got %v, want context.Canceled", err)
	}
}

func TestRun_AuthError(t *testing.T) {
	st := newTestStore(t)
	p := &mock.Provider{AuthErr: fmt.Errorf("auth failed")}
	var out strings.Builder

	err := Run(context.Background(), defaultOpts(p, st, &out))
	if err == nil {
		t.Fatal("expected error from auth failure")
	}

	// No checkpoint should be written on auth failure
	cp, _ := st.GetCheckpoint(context.Background(), "user@example.com", "gmail")
	if cp != nil {
		t.Errorf("expected no checkpoint after auth error, got %v", cp)
	}
}
