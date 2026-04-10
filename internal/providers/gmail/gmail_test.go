package gmail

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"golang.org/x/oauth2"
	gmailapi "google.golang.org/api/gmail/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"

	"github.com/UreaLaden/inboxatlas/pkg/models"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

// --- Authenticate ---

func TestAuthenticate_TokenSourceFactoryError(t *testing.T) {
	p := New("user@example.com", func(context.Context) (oauth2.TokenSource, error) {
		return nil, errors.New("no token source")
	})
	err := p.Authenticate(context.Background())
	if err == nil {
		t.Fatal("expected error when token source factory fails")
	}
}

func TestAuthenticate_Success(t *testing.T) {
	p := New("user@example.com", func(context.Context) (oauth2.TokenSource, error) {
		return oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "fake-token"}), nil
	})
	if err := p.Authenticate(context.Background()); err != nil {
		t.Fatalf("Authenticate: unexpected error: %v", err)
	}
	if p.svc == nil {
		t.Fatal("expected svc to be set after Authenticate")
	}
}

func TestAuthenticate_NilTokenSourceFactory(t *testing.T) {
	p := New("user@example.com", nil)
	err := p.Authenticate(context.Background())
	if err == nil {
		t.Fatal("expected error when token source factory is nil")
	}
}

// --- newTestService helper ---

// newTestService creates an httptest.Server and a gmailapi.Service pointed at it.
// The caller must invoke the returned cleanup function when done.
func newTestService(t *testing.T, handler http.HandlerFunc) (*gmailapi.Service, func()) {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		if strings.Contains(err.Error(), "operation not permitted") {
			t.Skipf("local TCP listeners unavailable in this environment: %v", err)
		}
		t.Fatalf("Listen: %v", err)
	}
	srv := httptest.NewUnstartedServer(handler)
	srv.Listener = listener
	srv.Start()
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

func newRoundTripService(t *testing.T, handler roundTripFunc) *gmailapi.Service {
	t.Helper()

	svc, err := gmailapi.NewService(context.Background(),
		option.WithHTTPClient(&http.Client{Transport: handler}),
		option.WithEndpoint("https://gmail.test/"),
		option.WithoutAuthentication(),
	)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return svc
}

// --- ListMessages guard ---

func TestListMessages_NotAuthenticated(t *testing.T) {
	p := &Provider{}
	_, _, err := p.ListMessages(context.Background(), "")
	if err == nil {
		t.Fatal("expected error when not authenticated")
	}
}

func TestListLabels_NotAuthenticated(t *testing.T) {
	p := &Provider{}
	_, err := p.ListLabels(context.Background())
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

func TestParseMessageMeta_ExtractsAttachmentMetadata(t *testing.T) {
	msg := &gmailapi.Message{
		Id: "msg-attach",
		Payload: &gmailapi.MessagePart{
			Headers: []*gmailapi.MessagePartHeader{
				{Name: "From", Value: "John Doe <john@example.com>"},
			},
			Parts: []*gmailapi.MessagePart{
				{
					MimeType: "multipart/alternative",
					Parts: []*gmailapi.MessagePart{
						{MimeType: "text/plain"},
						{Filename: "invoice.pdf", MimeType: "application/pdf"},
					},
				},
				{Filename: "receipt.PNG", MimeType: "image/png"},
				{Filename: "duplicate.pdf", MimeType: "application/pdf"},
			},
		},
	}

	meta := parseMessageMeta("account@example.com", msg)
	if !meta.HasAttachment {
		t.Fatal("HasAttachment: got false, want true")
	}
	if len(meta.AttachmentTypes) != 2 || meta.AttachmentTypes[0] != "application/pdf" || meta.AttachmentTypes[1] != "image/png" {
		t.Fatalf("AttachmentTypes: got %v", meta.AttachmentTypes)
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
		Id:           "msg3",
		InternalDate: time.Date(2024, 2, 3, 4, 5, 6, 0, time.UTC).UnixMilli(),
		Payload: &gmailapi.MessagePart{
			Headers: []*gmailapi.MessagePartHeader{
				{Name: "Date", Value: "not-a-date"},
			},
		},
	}
	meta := parseMessageMeta("a@b.com", msg)
	want := time.Date(2024, 2, 3, 4, 5, 6, 0, time.UTC)
	if !meta.ReceivedAt.Equal(want) {
		t.Errorf("ReceivedAt = %v, want %v", meta.ReceivedAt, want)
	}
}

func TestParseMessageMeta_UsesInternalDateWhenHeaderMissing(t *testing.T) {
	msg := &gmailapi.Message{
		Id:           "msg8",
		InternalDate: time.Date(2024, 3, 4, 5, 6, 7, 0, time.UTC).UnixMilli(),
		Payload:      &gmailapi.MessagePart{},
	}
	meta := parseMessageMeta("a@b.com", msg)
	want := time.Date(2024, 3, 4, 5, 6, 7, 0, time.UTC)
	if !meta.ReceivedAt.Equal(want) {
		t.Errorf("ReceivedAt = %v, want %v", meta.ReceivedAt, want)
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

func TestListLabels_Success(t *testing.T) {
	svc, cleanup := newTestService(t, func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]interface{}{
			"labels": []map[string]string{
				{"id": "INBOX", "name": "INBOX", "type": "system"},
				{"id": "Label_12345", "name": "Billing Queue", "type": "user"},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp) //nolint:errcheck
	})
	defer cleanup()

	p := &Provider{svc: svc, email: "test@example.com"}
	labels, err := p.ListLabels(context.Background())
	if err != nil {
		t.Fatalf("ListLabels: %v", err)
	}
	if len(labels) != 2 {
		t.Fatalf("expected 2 labels, got %d", len(labels))
	}
	if labels[0] != (LabelMeta{ID: "INBOX", DisplayName: "INBOX", Type: "system"}) {
		t.Fatalf("unexpected first label: %+v", labels[0])
	}
	if labels[1] != (LabelMeta{ID: "Label_12345", DisplayName: "Billing Queue", Type: "user"}) {
		t.Fatalf("unexpected second label: %+v", labels[1])
	}
}

// --- GetMessageMeta via httptest server ---

func TestGetMessageMeta_Success(t *testing.T) {
	svc, cleanup := newTestService(t, func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("format"); got != "full" {
			t.Fatalf("format: got %q want %q", got, "full")
		}
		fields := r.URL.Query().Get("fields")
		if !strings.Contains(fields, "payload") || strings.Contains(fields, "body/data") {
			t.Fatalf("unexpected fields filter: %q", fields)
		}
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
				"parts": []map[string]interface{}{
					{"filename": "invoice.pdf", "mimeType": "application/pdf"},
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
	if !meta.HasAttachment || len(meta.AttachmentTypes) != 1 || meta.AttachmentTypes[0] != "application/pdf" {
		t.Fatalf("attachment metadata: %+v", meta)
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

// --- wrapIfRetryable ---

func TestWrapIfRetryable_429(t *testing.T) {
	apiErr := &googleapi.Error{Code: 429, Message: "quota exceeded"}
	wrapped := wrapIfRetryable(apiErr)
	var re models.RetryableError
	if !errors.As(wrapped, &re) || !re.IsRetryable() {
		t.Error("expected retryable error for 429")
	}
}

func TestWrapIfRetryable_503(t *testing.T) {
	apiErr := &googleapi.Error{Code: 503, Message: "unavailable"}
	wrapped := wrapIfRetryable(apiErr)
	var re models.RetryableError
	if !errors.As(wrapped, &re) || !re.IsRetryable() {
		t.Error("expected retryable error for 503")
	}
}

func TestWrapIfRetryable_Other(t *testing.T) {
	apiErr := &googleapi.Error{Code: 404, Message: "not found"}
	wrapped := wrapIfRetryable(apiErr)
	var re models.RetryableError
	if errors.As(wrapped, &re) {
		t.Error("expected non-retryable error for 404")
	}
}

func TestWrapIfRetryable_Nil(t *testing.T) {
	if wrapIfRetryable(nil) != nil {
		t.Error("expected nil for nil input")
	}
}

func TestRetryableError_ErrorAndUnwrap(t *testing.T) {
	inner := errors.New("boom")
	err := &retryableError{err: inner}
	if err.Error() != "boom" {
		t.Fatalf("Error: got %q", err.Error())
	}
	if !errors.Is(err, inner) {
		t.Fatal("expected unwrap to expose inner error")
	}
}

func TestListMessages_RoundTripperSuccess(t *testing.T) {
	svc := newRoundTripService(t, func(r *http.Request) (*http.Response, error) {
		if got := r.URL.Query().Get("pageToken"); got != "cursor1" {
			t.Fatalf("pageToken: got %q", got)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body: io.NopCloser(strings.NewReader(`{
				"messages":[{"id":"msg1"},{"id":"msg2"}],
				"nextPageToken":"cursor2"
			}`)),
		}, nil
	})

	p := &Provider{svc: svc, email: "test@example.com"}
	ids, next, err := p.ListMessages(context.Background(), "cursor1")
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}
	if len(ids) != 2 || ids[0] != "msg1" || ids[1] != "msg2" {
		t.Fatalf("ids: got %v", ids)
	}
	if next != "cursor2" {
		t.Fatalf("next: got %q", next)
	}
}

func TestGetMessageMeta_RoundTripperRetryableError(t *testing.T) {
	svc := newRoundTripService(t, func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusTooManyRequests,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"error":{"code":429,"message":"quota exceeded"}}`)),
		}, nil
	})

	p := &Provider{svc: svc, email: "test@example.com"}
	_, err := p.GetMessageMeta(context.Background(), "msg1")
	if err == nil {
		t.Fatal("expected retryable error")
	}
	var re models.RetryableError
	if !errors.As(err, &re) || !re.IsRetryable() {
		t.Fatalf("expected retryable wrapped error, got %v", err)
	}
}
