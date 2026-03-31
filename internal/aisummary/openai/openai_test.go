package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	exportpkg "github.com/UreaLaden/inboxatlas/internal/export"
)

type roundTripDoer func(*http.Request) (*http.Response, error)

func (f roundTripDoer) Do(req *http.Request) (*http.Response, error) {
	return f(req)
}

type errReadCloser struct{}

func (errReadCloser) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func (errReadCloser) Close() error             { return nil }

func newHTTPTestServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()

	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		if strings.Contains(err.Error(), "operation not permitted") {
			t.Skipf("local TCP listeners unavailable in this environment: %v", err)
		}
		t.Fatalf("Listen: %v", err)
	}

	server := httptest.NewUnstartedServer(handler)
	server.Listener = listener
	server.Start()
	t.Cleanup(server.Close)
	return server
}

func TestNewConfigFromEnv_Defaults(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "test-key")
	t.Setenv("OPENAI_MODEL", "")
	t.Setenv("OPENAI_BASE_URL", "")
	t.Setenv("OPENAI_TIMEOUT_SECONDS", "")
	t.Setenv("OPENAI_DEBUG", "")

	cfg, err := NewConfigFromEnv()
	if err != nil {
		t.Fatalf("NewConfigFromEnv: %v", err)
	}
	if cfg.Model != defaultModel {
		t.Fatalf("Model: got %q want %q", cfg.Model, defaultModel)
	}
	if cfg.BaseURL != defaultBaseURL {
		t.Fatalf("BaseURL: got %q want %q", cfg.BaseURL, defaultBaseURL)
	}
	if cfg.Timeout != 90*time.Second {
		t.Fatalf("Timeout: got %v", cfg.Timeout)
	}
}

func TestNewConfigFromEnv_RequiresAPIKey(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "")
	_, err := NewConfigFromEnv()
	if !errors.Is(err, ErrAPIKeyRequired) {
		t.Fatalf("expected ErrAPIKeyRequired, got %v", err)
	}
}

func TestNewConfigFromEnv_InvalidTimeout(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "test-key")
	t.Setenv("OPENAI_TIMEOUT_SECONDS", "nope")
	_, err := NewConfigFromEnv()
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestClientGenerateSummary_SendsSchemaConstrainedRequest(t *testing.T) {
	var captured responsesRequest
	server := newHTTPTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/responses" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Fatalf("unexpected auth header: %q", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		_, _ = io.WriteString(w, `{"output_text":"{\"title\":\"Inbox Snapshot\",\"subtitle\":\"owner@company.com\",\"headline\":\"Email volume increased from 5 to 8 messages across 2025-01 to 2025-03.\",\"secondary_headline\":\"alerts@vendor.com remained the top external sender with 9 messages.\",\"snapshot_bullets\":[\"vendor.com is the most active external domain with 9 messages.\"],\"what_this_means_bullets\":[\"A small number of external sources shape most of the 20 messages in scope.\"],\"opportunities_bullets\":[\"Promote repeated alerts from alerts@vendor.com into a review workflow.\"],\"bottom_line\":\"The inbox shows stable patterns that are ready for structured automation review.\"}"}`)
	}))

	client := NewClient(Config{
		APIKey:         "test-key",
		Model:          "gpt-4o-mini",
		BaseURL:        server.URL,
		Timeout:        5 * time.Second,
		MaxAttempts:    1,
		InitialBackoff: time.Millisecond,
	})

	output, err := client.GenerateSummary(context.Background(), "authoritative prompt", validSummaryInput(t), io.Discard)
	if err != nil {
		t.Fatalf("GenerateSummary: %v", err)
	}
	if output.Headline == "" {
		t.Fatalf("expected parsed output, got %+v", output)
	}
	if captured.Text.Format.Type != "json_schema" || !captured.Text.Format.Strict {
		t.Fatalf("expected strict json_schema format, got %+v", captured.Text.Format)
	}
	if captured.Temperature != defaultTemperature {
		t.Fatalf("Temperature: got %v want %v", captured.Temperature, defaultTemperature)
	}
	if captured.Text.Format.Name != "summary_output" {
		t.Fatalf("unexpected schema name: %s", captured.Text.Format.Name)
	}
	if len(captured.Input) != 2 || captured.Input[0].Content[0].Text != "authoritative prompt" {
		t.Fatalf("unexpected prompt/input payload: %+v", captured.Input)
	}
	bodyText := captured.Input[1].Content[0].Text
	if !strings.Contains(bodyText, `"total_messages":20`) || !strings.Contains(bodyText, `"top_external_sender_count":9`) {
		t.Fatalf("expected deterministic payload in request, got %s", bodyText)
	}
}

func TestClientGenerateSummary_RetriesTransientFailures(t *testing.T) {
	attempts := 0
	server := newHTTPTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 3 {
			http.Error(w, `{"error":"rate limited"}`, http.StatusTooManyRequests)
			return
		}
		_, _ = io.WriteString(w, `{"output_text":"{\"title\":\"Inbox Snapshot\",\"subtitle\":\"owner@company.com\",\"headline\":\"Email volume increased from 5 to 8 messages across 2025-01 to 2025-03.\",\"secondary_headline\":\"alerts@vendor.com remained the top external sender with 9 messages.\",\"snapshot_bullets\":[\"vendor.com is the most active external domain with 9 messages.\"],\"what_this_means_bullets\":[\"A small number of external sources shape most of the 20 messages in scope.\"],\"opportunities_bullets\":[\"Promote repeated alerts from alerts@vendor.com into a review workflow.\"],\"bottom_line\":\"The inbox shows stable patterns that are ready for structured automation review.\"}"}`)
	}))

	client := NewClient(Config{
		APIKey:         "test-key",
		Model:          "gpt-4o-mini",
		BaseURL:        server.URL,
		Timeout:        5 * time.Second,
		MaxAttempts:    3,
		InitialBackoff: time.Millisecond,
	})
	client.sleep = func(time.Duration) {}

	if _, err := client.GenerateSummary(context.Background(), "prompt", validSummaryInput(t), io.Discard); err != nil {
		t.Fatalf("GenerateSummary: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
}

func TestClientGenerateSummary_DoesNotRetryAuthFailure(t *testing.T) {
	attempts := 0
	server := newHTTPTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
	}))

	client := NewClient(Config{
		APIKey:         "test-key",
		Model:          "gpt-4o-mini",
		BaseURL:        server.URL,
		Timeout:        5 * time.Second,
		MaxAttempts:    3,
		InitialBackoff: time.Millisecond,
	})
	client.sleep = func(time.Duration) {}

	_, err := client.GenerateSummary(context.Background(), "prompt", validSummaryInput(t), io.Discard)
	if err == nil {
		t.Fatal("expected auth failure")
	}
	if attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", attempts)
	}
}

func TestClientGenerateSummary_InvalidModelOutput(t *testing.T) {
	server := newHTTPTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"output_text":"not-json"}`)
	}))

	client := NewClient(Config{
		APIKey:         "test-key",
		Model:          "gpt-4o-mini",
		BaseURL:        server.URL,
		Timeout:        5 * time.Second,
		MaxAttempts:    1,
		InitialBackoff: time.Millisecond,
	})

	_, err := client.GenerateSummary(context.Background(), "prompt", validSummaryInput(t), io.Discard)
	if err == nil {
		t.Fatal("expected schema violation")
	}
	if !errors.Is(err, ErrSchemaViolation) {
		t.Fatalf("expected ErrSchemaViolation, got %v", err)
	}
}

func TestClientGenerateSummary_UsesNestedOutputTextAndDebugLogs(t *testing.T) {
	var stderr strings.Builder
	server := newHTTPTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"output":[{"content":[{"type":"output_text","text":"{\"title\":\"Inbox Snapshot\",\"subtitle\":\"owner@company.com\",\"headline\":\"Email volume increased from 5 to 8 messages across 2025-01 to 2025-03.\",\"secondary_headline\":\"alerts@vendor.com remained the top external sender with 9 messages.\",\"snapshot_bullets\":[\"vendor.com is the most active external domain with 9 messages.\"],\"what_this_means_bullets\":[\"A small number of external sources shape most of the 20 messages in scope.\"],\"opportunities_bullets\":[\"Promote repeated alerts from alerts@vendor.com into a review workflow.\"],\"bottom_line\":\"The inbox shows stable patterns that are ready for structured automation review.\"}"}]}]}`)
	}))

	client := NewClient(Config{
		APIKey:         "test-key",
		Model:          "gpt-4o-mini",
		BaseURL:        server.URL,
		Timeout:        5 * time.Second,
		Debug:          true,
		MaxAttempts:    1,
		InitialBackoff: time.Millisecond,
	})

	output, err := client.GenerateSummary(context.Background(), "prompt", validSummaryInput(t), &stderr)
	if err != nil {
		t.Fatalf("GenerateSummary: %v", err)
	}
	if output.Title != "Inbox Snapshot" {
		t.Fatalf("unexpected output: %+v", output)
	}
	if !strings.Contains(stderr.String(), "attempt 1") || !strings.Contains(stderr.String(), "status=200") {
		t.Fatalf("expected debug stderr output, got %q", stderr.String())
	}
}

func TestShouldRetryError(t *testing.T) {
	if !shouldRetryError(&net.DNSError{IsTimeout: true}) {
		t.Fatal("expected timeout network error to retry")
	}
	if shouldRetryError(errors.New("plain error")) {
		t.Fatal("expected plain error not to retry")
	}
}

func TestNewClient_UsesConfiguredTimeout(t *testing.T) {
	client := NewClient(Config{Timeout: 7 * time.Second})

	httpClient, ok := client.http.(*http.Client)
	if !ok {
		t.Fatalf("expected *http.Client, got %T", client.http)
	}
	if httpClient.Timeout != 7*time.Second {
		t.Fatalf("Timeout: got %v, want %v", httpClient.Timeout, 7*time.Second)
	}
	if client.sleep == nil {
		t.Fatal("expected sleep func to be set")
	}
}

func TestClientBuildRequest(t *testing.T) {
	client := NewClient(Config{Model: "gpt-4o-mini"})

	body, err := client.buildRequest("authoritative prompt", validSummaryInput(t))
	if err != nil {
		t.Fatalf("buildRequest: %v", err)
	}

	var req responsesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if req.Model != "gpt-4o-mini" {
		t.Fatalf("Model: got %q", req.Model)
	}
	if req.Text.Format.Type != "json_schema" || !req.Text.Format.Strict {
		t.Fatalf("unexpected format: %+v", req.Text.Format)
	}
	if req.Text.Format.Schema["type"] != "object" {
		t.Fatalf("unexpected schema: %+v", req.Text.Format.Schema)
	}
	if len(req.Input) != 2 || req.Input[0].Content[0].Text != "authoritative prompt" {
		t.Fatalf("unexpected input payload: %+v", req.Input)
	}
}

func TestClientBuildRequest_MarshalInputError(t *testing.T) {
	client := NewClient(Config{Model: "gpt-4o-mini"})
	input := validSummaryInput(t)
	input.TopExternalSenders[0].PercentOfTotal = math.NaN()

	_, err := client.buildRequest("prompt", input)
	if err == nil {
		t.Fatal("expected marshal error")
	}
}

func TestClientSendOnce_Success(t *testing.T) {
	client := &Client{
		cfg: Config{APIKey: "test-key", BaseURL: "https://api.example.test"},
		http: roundTripDoer(func(req *http.Request) (*http.Response, error) {
			if req.URL.String() != "https://api.example.test/responses" {
				t.Fatalf("URL: got %s", req.URL.String())
			}
			if got := req.Header.Get("Authorization"); got != "Bearer test-key" {
				t.Fatalf("Authorization: got %q", got)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{"output_text":"{\"title\":\"Inbox Snapshot\",\"subtitle\":\"owner@company.com\",\"headline\":\"Email volume increased from 5 to 8 messages across 2025-01 to 2025-03.\",\"secondary_headline\":\"alerts@vendor.com remained the top external sender with 9 messages.\",\"snapshot_bullets\":[\"vendor.com is the most active external domain with 9 messages.\"],\"what_this_means_bullets\":[\"A small number of external sources shape most of the 20 messages in scope.\"],\"opportunities_bullets\":[\"Promote repeated alerts from alerts@vendor.com into a review workflow.\"],\"bottom_line\":\"The inbox shows stable patterns that are ready for structured automation review.\"}"}`)),
			}, nil
		}),
	}

	output, retry, err := client.sendOnce(context.Background(), []byte(`{}`), io.Discard)
	if err != nil {
		t.Fatalf("sendOnce: %v", err)
	}
	if retry {
		t.Fatal("expected non-retryable success")
	}
	if output.Title != "Inbox Snapshot" {
		t.Fatalf("unexpected output: %+v", output)
	}
}

func TestClientSendOnce_TransientStatus(t *testing.T) {
	client := &Client{
		cfg: Config{APIKey: "test-key", BaseURL: "https://api.example.test"},
		http: roundTripDoer(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusTooManyRequests,
				Body:       io.NopCloser(strings.NewReader(`{"error":"rate limited"}`)),
			}, nil
		}),
	}

	_, retry, err := client.sendOnce(context.Background(), []byte(`{}`), io.Discard)
	if err == nil {
		t.Fatal("expected transient error")
	}
	if !retry {
		t.Fatal("expected retryable error")
	}
}

func TestClientSendOnce_RequestFailureIsRetryable(t *testing.T) {
	client := &Client{
		cfg: Config{APIKey: "test-key", BaseURL: "https://api.example.test"},
		http: roundTripDoer(func(req *http.Request) (*http.Response, error) {
			return nil, &net.DNSError{IsTimeout: true}
		}),
	}

	_, retry, err := client.sendOnce(context.Background(), []byte(`{}`), io.Discard)
	if err == nil {
		t.Fatal("expected request failure")
	}
	if !retry {
		t.Fatal("expected retryable network error")
	}
}

func TestClientSendOnce_NoOutputText(t *testing.T) {
	client := &Client{
		cfg: Config{APIKey: "test-key", BaseURL: "https://api.example.test"},
		http: roundTripDoer(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{"output":[{"content":[{"type":"output_text","text":"   "}]}]}`)),
			}, nil
		}),
	}

	_, retry, err := client.sendOnce(context.Background(), []byte(`{}`), io.Discard)
	if err == nil {
		t.Fatal("expected schema violation")
	}
	if retry {
		t.Fatal("expected non-retryable schema violation")
	}
	if !errors.Is(err, ErrSchemaViolation) {
		t.Fatalf("expected ErrSchemaViolation, got %v", err)
	}
}

func TestClientSendOnce_BadRequestDoesNotRetry(t *testing.T) {
	client := &Client{
		cfg: Config{APIKey: "test-key", BaseURL: "https://api.example.test"},
		http: roundTripDoer(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusBadRequest,
				Body:       io.NopCloser(strings.NewReader(`{"error":"bad request"}`)),
			}, nil
		}),
	}

	_, retry, err := client.sendOnce(context.Background(), []byte(`{}`), io.Discard)
	if err == nil {
		t.Fatal("expected bad request error")
	}
	if retry {
		t.Fatal("expected non-retryable 400")
	}
}

func TestClientSendOnce_ReadBodyError(t *testing.T) {
	client := &Client{
		cfg: Config{APIKey: "test-key", BaseURL: "https://api.example.test"},
		http: roundTripDoer(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       errReadCloser{},
			}, nil
		}),
	}

	_, retry, err := client.sendOnce(context.Background(), []byte(`{}`), io.Discard)
	if err == nil {
		t.Fatal("expected read error")
	}
	if retry {
		t.Fatal("expected read error not to retry")
	}
}

func TestClientSendOnce_DecodeError(t *testing.T) {
	client := &Client{
		cfg: Config{APIKey: "test-key", BaseURL: "https://api.example.test"},
		http: roundTripDoer(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`not-json`)),
			}, nil
		}),
	}

	_, retry, err := client.sendOnce(context.Background(), []byte(`{}`), io.Discard)
	if err == nil {
		t.Fatal("expected decode error")
	}
	if retry {
		t.Fatal("expected decode error not to retry")
	}
}

func TestClientGenerateSummary_RetriesWithInjectedDoer(t *testing.T) {
	attempts := 0
	var stderr bytes.Buffer
	client := &Client{
		cfg: Config{
			APIKey:         "test-key",
			Model:          "gpt-4o-mini",
			BaseURL:        "https://api.example.test",
			Debug:          true,
			MaxAttempts:    3,
			InitialBackoff: time.Millisecond,
		},
		http: roundTripDoer(func(req *http.Request) (*http.Response, error) {
			attempts++
			if attempts < 3 {
				return &http.Response{
					StatusCode: http.StatusInternalServerError,
					Body:       io.NopCloser(strings.NewReader(`{"error":"temporary"}`)),
				}, nil
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{"output_text":"{\"title\":\"Inbox Snapshot\",\"subtitle\":\"owner@company.com\",\"headline\":\"Email volume increased from 5 to 8 messages across 2025-01 to 2025-03.\",\"secondary_headline\":\"alerts@vendor.com remained the top external sender with 9 messages.\",\"snapshot_bullets\":[\"vendor.com is the most active external domain with 9 messages.\"],\"what_this_means_bullets\":[\"A small number of external sources shape most of the 20 messages in scope.\"],\"opportunities_bullets\":[\"Promote repeated alerts from alerts@vendor.com into a review workflow.\"],\"bottom_line\":\"The inbox shows stable patterns that are ready for structured automation review.\"}"}`)),
			}, nil
		}),
		sleep: func(time.Duration) {},
	}

	output, err := client.GenerateSummary(context.Background(), "prompt", validSummaryInput(t), &stderr)
	if err != nil {
		t.Fatalf("GenerateSummary: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts: got %d, want 3", attempts)
	}
	if output.Title != "Inbox Snapshot" {
		t.Fatalf("unexpected output: %+v", output)
	}
	if !strings.Contains(stderr.String(), "attempt 3") {
		t.Fatalf("expected debug attempt logs, got %q", stderr.String())
	}
}

func TestExtractResponseText(t *testing.T) {
	text, err := extractResponseText(responsesResponse{
		Output: []responseOutput{{
			Content: []responseOutputContent{{Type: "output_text", Text: "hello"}},
		}},
	})
	if err != nil {
		t.Fatalf("extractResponseText: %v", err)
	}
	if text != "hello" {
		t.Fatalf("text: got %q", text)
	}
}

func TestTruncateAndSchema(t *testing.T) {
	if got := truncate([]byte(strings.Repeat("a", 301))); len(got) != 303 || !strings.HasSuffix(got, "...") {
		t.Fatalf("unexpected truncate result: %q", got)
	}

	schema := summaryOutputSchema()
	if schema["type"] != "object" {
		t.Fatalf("unexpected schema type: %+v", schema)
	}
	required, ok := schema["required"].([]string)
	if !ok || len(required) == 0 {
		t.Fatalf("expected required fields, got %+v", schema["required"])
	}
}

func validSummaryInput(t *testing.T) exportpkg.SummaryInput {
	t.Helper()
	model, err := exportpkg.ParseReportsDir(exportpkg.Options{
		ReportsDir: filepath.Join("..", "..", "export", "testdata", "valid"),
		OwnerEmail: "owner@company.com",
	})
	if err != nil {
		t.Fatalf("ParseReportsDir: %v", err)
	}
	input, err := exportpkg.BuildSummaryInput(model)
	if err != nil {
		t.Fatalf("BuildSummaryInput: %v", err)
	}
	return input
}
