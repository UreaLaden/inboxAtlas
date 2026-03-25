package gmail

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"golang.org/x/oauth2"
	gmailapi "google.golang.org/api/gmail/v1"
	"google.golang.org/api/option"

	"github.com/UreaLaden/inboxatlas/internal/auth"
)

// stubTokenStorage is a minimal auth.TokenStorage for testing Authenticate errors.
type stubTokenStorage struct {
	token *oauth2.Token
	err   error
}

func (s *stubTokenStorage) Save(_, _ string, _ *oauth2.Token) error { return nil }
func (s *stubTokenStorage) Load(_, _ string) (*oauth2.Token, error) { return s.token, s.err }

var _ auth.TokenStorage = (*stubTokenStorage)(nil)

// --- Authenticate ---

func TestAuthenticate_LoadTokenError(t *testing.T) {
	ts := &stubTokenStorage{err: errors.New("no token")}
	p := New(&oauth2.Config{}, ts, "user@example.com")
	err := p.Authenticate(context.Background())
	if err == nil {
		t.Fatal("expected error when LoadToken fails")
	}
}

func TestAuthenticate_Success(t *testing.T) {
	ts := &stubTokenStorage{token: &oauth2.Token{AccessToken: "fake-token"}}
	p := New(&oauth2.Config{}, ts, "user@example.com")
	if err := p.Authenticate(context.Background()); err != nil {
		t.Fatalf("Authenticate: unexpected error: %v", err)
	}
	if p.svc == nil {
		t.Fatal("expected svc to be set after Authenticate")
	}
}

// --- newTestService helper ---

// newTestService creates an httptest.Server and a gmailapi.Service pointed at it.
// The caller must invoke the returned cleanup function when done.
func newTestService(t *testing.T, handler http.HandlerFunc) (*gmailapi.Service, func()) {
	t.Helper()
	srv := httptest.NewServer(handler)
	svc, err := gmailapi.NewService(context.Background(),
		option.WithEndpoint(srv.URL),
		option.WithoutAuthentication(),
	)
	if err != nil {
		srv.Close()
		t.Fatalf("NewService: %v", err)
	}
	return svc, srv.Close
}

// --- ListMessages guard ---

func TestListMessages_NotAuthenticated(t *testing.T) {
	p := &Provider{}
	_, _, err := p.ListMessages(context.Background(), "")
	if err == nil {
		t.Fatal("expected error when not authenticated")
	}
}

// --- GetMessageMeta guard ---

func TestGetMessageMeta_NotAuthenticated(t *testing.T) {
	p := &Provider{}
	_, err := p.GetMessageMeta(context.Background(), "msg1")
	if err == nil {
		t.Fatal("expected error when not authenticated")
	}
}

// --- parseMessageMeta ---

func TestParseMessageMeta_Standard(t *testing.T) {
	msg := &gmailapi.Message{
		Id:       "msg1",
		ThreadId: "thread1",
		Snippet:  "Hello world",
		LabelIds: []string{"INBOX", "UNREAD"},
		Payload: &gmailapi.MessagePart{
			Headers: []*gmailapi.MessagePartHeader{
				{Name: "From", Value: "John Doe <john@example.com>"},
				{Name: "Subject", Value: "Test Subject"},
				{Name: "Date", Value: "Mon, 01 Jan 2024 12:00:00 +0000"},
			},
		},
	}

	meta := parseMessageMeta("account@example.com", msg)

	if meta.ID != "msg1" {
		t.Errorf("ID = %q, want %q", meta.ID, "msg1")
	}
	if meta.ProviderID != "msg1" {
		t.Errorf("ProviderID = %q, want %q", meta.ProviderID, "msg1")
	}
	if meta.ThreadID != "thread1" {
		t.Errorf("ThreadID = %q, want %q", meta.ThreadID, "thread1")
	}
	if meta.Snippet != "Hello world" {
		t.Errorf("Snippet = %q, want %q", meta.Snippet, "Hello world")
	}
	if meta.MailboxID != "account@example.com" {
		t.Errorf("MailboxID = %q, want %q", meta.MailboxID, "account@example.com")
	}
	if meta.Provider != "gmail" {
		t.Errorf("Provider = %q, want %q", meta.Provider, "gmail")
	}
	if meta.FromName != "John Doe" {
		t.Errorf("FromName = %q, want %q", meta.FromName, "John Doe")
	}
	if meta.FromEmail != "john@example.com" {
		t.Errorf("FromEmail = %q, want %q", meta.FromEmail, "john@example.com")
	}
	if meta.Domain != "example.com" {
		t.Errorf("Domain = %q, want %q", meta.Domain, "example.com")
	}
	if meta.Subject != "Test Subject" {
		t.Errorf("Subject = %q, want %q", meta.Subject, "Test Subject")
	}
	if len(meta.Labels) != 2 || meta.Labels[0] != "INBOX" {
		t.Errorf("Labels = %v, want [INBOX UNREAD]", meta.Labels)
	}
	wantTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	if !meta.ReceivedAt.Equal(wantTime) {
		t.Errorf("ReceivedAt = %v, want %v", meta.ReceivedAt, wantTime)
	}
	// Mailbox (alias) is intentionally left empty — callers fill it.
	if meta.Mailbox != "" {
		t.Errorf("Mailbox should be empty, got %q", meta.Mailbox)
	}
}

func TestParseMessageMeta_MissingFromHeader(t *testing.T) {
	msg := &gmailapi.Message{
		Id: "msg2",
		Payload: &gmailapi.MessagePart{
			Headers: []*gmailapi.MessagePartHeader{
				{Name: "Subject", Value: "No From"},
			},
		},
	}
	meta := parseMessageMeta("a@b.com", msg)
	if meta.FromEmail != "" {
		t.Errorf("FromEmail = %q, want empty", meta.FromEmail)
	}
	if meta.FromName != "" {
		t.Errorf("FromName = %q, want empty", meta.FromName)
	}
	if meta.Domain != "" {
		t.Errorf("Domain = %q, want empty", meta.Domain)
	}
}

func TestParseMessageMeta_MalformedDate(t *testing.T) {
	msg := &gmailapi.Message{
		Id: "msg3",
		Payload: &gmailapi.MessagePart{
			Headers: []*gmailapi.MessagePartHeader{
				{Name: "Date", Value: "not-a-date"},
			},
		},
	}
	meta := parseMessageMeta("a@b.com", msg)
	if !meta.ReceivedAt.IsZero() {
		t.Errorf("ReceivedAt should be zero for malformed date, got %v", meta.ReceivedAt)
	}
}

func TestParseMessageMeta_NilPayload(t *testing.T) {
	msg := &gmailapi.Message{
		Id:       "msg4",
		ThreadId: "t4",
		Snippet:  "snip",
	}
	meta := parseMessageMeta("a@b.com", msg)
	if meta.ID != "msg4" {
		t.Errorf("ID = %q, want %q", meta.ID, "msg4")
	}
	if meta.ThreadID != "t4" {
		t.Errorf("ThreadID = %q, want %q", meta.ThreadID, "t4")
	}
	if meta.Snippet != "snip" {
		t.Errorf("Snippet = %q, want %q", meta.Snippet, "snip")
	}
	if meta.FromEmail != "" || meta.Subject != "" {
		t.Error("expected empty header fields for nil Payload")
	}
}

func TestParseMessageMeta_NilPayloadHeaders(t *testing.T) {
	msg := &gmailapi.Message{
		Id:      "msg5",
		Payload: &gmailapi.MessagePart{},
	}
	meta := parseMessageMeta("a@b.com", msg)
	if meta.ID != "msg5" {
		t.Errorf("ID = %q, want %q", meta.ID, "msg5")
	}
	if meta.FromEmail != "" || meta.Subject != "" {
		t.Error("expected empty header fields for nil Payload.Headers")
	}
}

func TestParseMessageMeta_FromWithDisplayName(t *testing.T) {
	msg := &gmailapi.Message{
		Id: "msg6",
		Payload: &gmailapi.MessagePart{
			Headers: []*gmailapi.MessagePartHeader{
				{Name: "From", Value: "Alice <alice@example.com>"},
			},
		},
	}
	meta := parseMessageMeta("me@example.com", msg)
	if meta.FromName != "Alice" {
		t.Errorf("FromName = %q, want %q", meta.FromName, "Alice")
	}
	if meta.FromEmail != "alice@example.com" {
		t.Errorf("FromEmail = %q, want %q", meta.FromEmail, "alice@example.com")
	}
}

func TestParseMessageMeta_MailboxIDLowercased(t *testing.T) {
	msg := &gmailapi.Message{Id: "msg7", Payload: &gmailapi.MessagePart{}}
	meta := parseMessageMeta("User@Example.COM", msg)
	if meta.MailboxID != "user@example.com" {
		t.Errorf("MailboxID = %q, want lowercase", meta.MailboxID)
	}
}

// --- ListMessages via httptest server ---

func TestListMessages_Success(t *testing.T) {
	svc, cleanup := newTestService(t, func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]interface{}{
			"messages": []map[string]string{
				{"id": "msg1", "threadId": "t1"},
				{"id": "msg2", "threadId": "t2"},
			},
			"nextPageToken": "tok2",
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp) //nolint:errcheck
	})
	defer cleanup()

	p := &Provider{svc: svc, email: "test@example.com"}
	ids, next, err := p.ListMessages(context.Background(), "")
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}
	if len(ids) != 2 || ids[0] != "msg1" || ids[1] != "msg2" {
		t.Errorf("ids = %v, want [msg1 msg2]", ids)
	}
	if next != "tok2" {
		t.Errorf("nextToken = %q, want %q", next, "tok2")
	}
}

func TestListMessages_WithPageToken(t *testing.T) {
	svc, cleanup := newTestService(t, func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("pageToken"); got != "cursor1" {
			t.Errorf("pageToken = %q, want %q", got, "cursor1")
		}
		resp := map[string]interface{}{
			"messages": []map[string]string{{"id": "msg3"}},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp) //nolint:errcheck
	})
	defer cleanup()

	p := &Provider{svc: svc, email: "test@example.com"}
	ids, next, err := p.ListMessages(context.Background(), "cursor1")
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}
	if len(ids) != 1 || ids[0] != "msg3" {
		t.Errorf("ids = %v, want [msg3]", ids)
	}
	if next != "" {
		t.Errorf("nextToken = %q, want empty", next)
	}
}

func TestListMessages_APIError(t *testing.T) {
	svc, cleanup := newTestService(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal error", http.StatusInternalServerError)
	})
	defer cleanup()

	p := &Provider{svc: svc, email: "test@example.com"}
	_, _, err := p.ListMessages(context.Background(), "")
	if err == nil {
		t.Fatal("expected error from API failure")
	}
}

// --- GetMessageMeta via httptest server ---

func TestGetMessageMeta_Success(t *testing.T) {
	svc, cleanup := newTestService(t, func(w http.ResponseWriter, r *http.Request) {
		msg := map[string]interface{}{
			"id":       "msgX",
			"threadId": "threadX",
			"snippet":  "Hello",
			"labelIds": []string{"INBOX"},
			"payload": map[string]interface{}{
				"headers": []map[string]string{
					{"name": "From", "value": "Sender <sender@example.com>"},
					{"name": "Subject", "value": "Test"},
					{"name": "Date", "value": "Mon, 01 Jan 2024 12:00:00 +0000"},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(msg) //nolint:errcheck
	})
	defer cleanup()

	p := &Provider{svc: svc, email: "test@example.com"}
	meta, err := p.GetMessageMeta(context.Background(), "msgX")
	if err != nil {
		t.Fatalf("GetMessageMeta: %v", err)
	}
	if meta.ID != "msgX" {
		t.Errorf("ID = %q, want %q", meta.ID, "msgX")
	}
	if meta.Subject != "Test" {
		t.Errorf("Subject = %q, want %q", meta.Subject, "Test")
	}
	if meta.FromEmail != "sender@example.com" {
		t.Errorf("FromEmail = %q, want %q", meta.FromEmail, "sender@example.com")
	}
}

func TestGetMessageMeta_APIError(t *testing.T) {
	svc, cleanup := newTestService(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	})
	defer cleanup()

	p := &Provider{svc: svc, email: "test@example.com"}
	_, err := p.GetMessageMeta(context.Background(), "missing")
	if err == nil {
		t.Fatal("expected error from API failure")
	}
}
