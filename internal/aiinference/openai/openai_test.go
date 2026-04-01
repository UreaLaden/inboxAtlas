package openai

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/UreaLaden/inboxatlas/internal/classification"
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
	t.Setenv("OPENAI_INFERENCE_MODEL", "")
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
}

func TestNewConfigFromEnv_PrefersInferenceModel(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "test-key")
	t.Setenv("OPENAI_INFERENCE_MODEL", "gpt-test")
	t.Setenv("OPENAI_MODEL", "gpt-fallback")

	cfg, err := NewConfigFromEnv()
	if err != nil {
		t.Fatalf("NewConfigFromEnv: %v", err)
	}
	if cfg.Model != "gpt-test" {
		t.Fatalf("Model: got %q", cfg.Model)
	}
}

func TestNewConfigFromEnv_FallbacksAndParsing(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "test-key")
	t.Setenv("OPENAI_INFERENCE_MODEL", "")
	t.Setenv("OPENAI_MODEL", "gpt-fallback")
	t.Setenv("OPENAI_BASE_URL", "https://example.test/")
	t.Setenv("OPENAI_TIMEOUT_SECONDS", "12")
	t.Setenv("OPENAI_DEBUG", "TRUE")

	cfg, err := NewConfigFromEnv()
	if err != nil {
		t.Fatalf("NewConfigFromEnv: %v", err)
	}
	if cfg.Model != "gpt-fallback" {
		t.Fatalf("Model: got %q", cfg.Model)
	}
	if cfg.BaseURL != "https://example.test" {
		t.Fatalf("BaseURL: got %q", cfg.BaseURL)
	}
	if cfg.Timeout != 12*time.Second {
		t.Fatalf("Timeout: got %v", cfg.Timeout)
	}
	if !cfg.Debug {
		t.Fatal("expected Debug true")
	}
	if cfg.MaxAttempts != maxAttempts {
		t.Fatalf("MaxAttempts: got %d", cfg.MaxAttempts)
	}
	if cfg.InitialBackoff <= 0 {
		t.Fatalf("InitialBackoff: got %v", cfg.InitialBackoff)
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
	t.Setenv("OPENAI_TIMEOUT_SECONDS", "bad")

	_, err := NewConfigFromEnv()
	if err == nil || !strings.Contains(err.Error(), "invalid OPENAI_TIMEOUT_SECONDS") {
		t.Fatalf("expected invalid timeout error, got %v", err)
	}
}

func TestClientInfer_SendsSchemaConstrainedRequest(t *testing.T) {
	var captured responsesRequest
	server := newHTTPTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		_, _ = io.WriteString(w, `{"output_text":"{\"candidates\":[{\"message_id\":\"m1\",\"category\":\"client\",\"confidence\":0.82,\"confidence_band\":\"high\",\"review_required\":false,\"evidence\":{\"subject_phrases\":[\"invoice\"],\"snippet_phrases\":[],\"sender_signal\":\"known sender\",\"domain_signal\":\"known domain\",\"label_signals\":[\"INBOX\"]}}]}"}`)
	}))

	client := NewClient(Config{
		APIKey:         "test-key",
		Model:          "gpt-4o-mini",
		BaseURL:        server.URL,
		Timeout:        5 * time.Second,
		MaxAttempts:    1,
		InitialBackoff: time.Millisecond,
	})

	output, err := client.Infer(context.Background(), validInferenceRequests(), io.Discard)
	if err != nil {
		t.Fatalf("Infer: %v", err)
	}
	if len(output) != 1 || output[0].MessageID != "m1" {
		t.Fatalf("unexpected output: %+v", output)
	}
	if captured.Text.Format.Type != "json_schema" || !captured.Text.Format.Strict {
		t.Fatalf("unexpected format: %+v", captured.Text.Format)
	}
	if captured.Text.Format.Name != "inference_candidates" {
		t.Fatalf("schema name: got %q", captured.Text.Format.Name)
	}
	if captured.Text.Format.Schema["type"] != "object" {
		t.Fatalf("schema type: got %+v", captured.Text.Format.Schema["type"])
	}
	properties, ok := captured.Text.Format.Schema["properties"].(map[string]any)
	if !ok {
		t.Fatalf("schema properties type: %T", captured.Text.Format.Schema["properties"])
	}
	candidatesSchema, ok := properties["candidates"].(map[string]any)
	if !ok {
		t.Fatalf("candidates schema type: %T", properties["candidates"])
	}
	itemsSchema, ok := candidatesSchema["items"].(map[string]any)
	if !ok {
		t.Fatalf("items schema type: %T", candidatesSchema["items"])
	}
	itemProperties, ok := itemsSchema["properties"].(map[string]any)
	if !ok {
		t.Fatalf("item properties type: %T", itemsSchema["properties"])
	}
	messageIDSchema, ok := itemProperties["message_id"].(map[string]any)
	if !ok {
		t.Fatalf("message_id schema type: %T", itemProperties["message_id"])
	}
	enum, ok := messageIDSchema["enum"].([]any)
	if !ok || len(enum) != 1 || enum[0] != "m1" {
		t.Fatalf("message_id enum: got %+v", messageIDSchema["enum"])
	}
	if len(captured.Input) != 2 || !strings.Contains(captured.Input[1].Content[0].Text, `"message_id":"m1"`) {
		t.Fatalf("unexpected request payload: %+v", captured.Input)
	}
	if math.Abs(captured.Temperature-defaultTemperature) > 0.000001 {
		t.Fatalf("Temperature: got %v want %v", captured.Temperature, defaultTemperature)
	}
}

func TestClientInfer_RetriesTransientFailures(t *testing.T) {
	attempts := 0
	server := newHTTPTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 3 {
			http.Error(w, `{"error":"rate limited"}`, http.StatusTooManyRequests)
			return
		}
		_, _ = io.WriteString(w, `{"output_text":"{\"candidates\":[]}"}`)
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

	if _, err := client.Infer(context.Background(), validInferenceRequests(), io.Discard); err != nil {
		t.Fatalf("Infer: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
}

func TestClientInfer_InvalidModelOutput(t *testing.T) {
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

	_, err := client.Infer(context.Background(), validInferenceRequests(), io.Discard)
	if err == nil {
		t.Fatal("expected schema violation")
	}
	if !errors.Is(err, ErrSchemaViolation) {
		t.Fatalf("expected ErrSchemaViolation, got %v", err)
	}
}

func TestClientInfer_InvalidCandidateRejected(t *testing.T) {
	server := newHTTPTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"output_text":"{\"candidates\":[{\"message_id\":\"missing\",\"category\":\"client\",\"confidence\":0.82,\"confidence_band\":\"high\",\"review_required\":false,\"evidence\":{\"subject_phrases\":[],\"snippet_phrases\":[],\"sender_signal\":\"\",\"domain_signal\":\"\",\"label_signals\":[]}}]}"}`)
	}))

	client := NewClient(Config{
		APIKey:         "test-key",
		Model:          "gpt-4o-mini",
		BaseURL:        server.URL,
		Timeout:        5 * time.Second,
		MaxAttempts:    1,
		InitialBackoff: time.Millisecond,
	})

	_, err := client.Infer(context.Background(), validInferenceRequests(), io.Discard)
	if err == nil {
		t.Fatal("expected validation failure")
	}
	if !errors.Is(err, ErrSchemaViolation) {
		t.Fatalf("expected ErrSchemaViolation, got %v", err)
	}
}

func TestClientInfer_NormalizesDerivedConfidenceFields(t *testing.T) {
	server := newHTTPTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"output_text":"{\"candidates\":[{\"message_id\":\"m1\",\"category\":\"client\",\"confidence\":0.78,\"confidence_band\":\"high\",\"review_required\":false,\"evidence\":{\"subject_phrases\":[\"invoice\"],\"snippet_phrases\":[],\"sender_signal\":\"known sender\",\"domain_signal\":\"known domain\",\"label_signals\":[\"INBOX\"]}}]}"}`)
	}))

	client := NewClient(Config{
		APIKey:         "test-key",
		Model:          "gpt-4o-mini",
		BaseURL:        server.URL,
		Timeout:        5 * time.Second,
		MaxAttempts:    1,
		InitialBackoff: time.Millisecond,
	})

	output, err := client.Infer(context.Background(), validInferenceRequests(), io.Discard)
	if err != nil {
		t.Fatalf("Infer: %v", err)
	}
	if len(output) != 1 {
		t.Fatalf("unexpected output: %+v", output)
	}
	if output[0].ConfidenceBand != "medium" {
		t.Fatalf("ConfidenceBand: got %q want medium", output[0].ConfidenceBand)
	}
	if !output[0].ReviewRequired {
		t.Fatal("expected ReviewRequired true")
	}
}

func TestClientInfer_StopsOnPermanentHTTPError(t *testing.T) {
	client := NewClient(Config{
		APIKey:         "test-key",
		Model:          "gpt-4o-mini",
		BaseURL:        "https://example.test",
		Timeout:        5 * time.Second,
		MaxAttempts:    3,
		InitialBackoff: time.Millisecond,
	})

	attempts := 0
	client.http = roundTripDoer(func(*http.Request) (*http.Response, error) {
		attempts++
		return &http.Response{
			StatusCode: http.StatusBadRequest,
			Body:       io.NopCloser(strings.NewReader(`{"error":"bad request"}`)),
		}, nil
	})

	_, err := client.Infer(context.Background(), validInferenceRequests(), io.Discard)
	if err == nil || !strings.Contains(err.Error(), "openai request failed (400)") {
		t.Fatalf("expected permanent HTTP error, got %v", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts: got %d want 1", attempts)
	}
}

func TestClientInfer_RetryExhaustedReturnsTransientError(t *testing.T) {
	client := NewClient(Config{
		APIKey:         "test-key",
		Model:          "gpt-4o-mini",
		BaseURL:        "https://example.test",
		Timeout:        5 * time.Second,
		MaxAttempts:    2,
		InitialBackoff: time.Millisecond,
	})

	sleeps := 0
	client.sleep = func(time.Duration) { sleeps++ }
	client.http = roundTripDoer(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusInternalServerError,
			Body:       io.NopCloser(strings.NewReader(`{"error":"server error"}`)),
		}, nil
	})

	_, err := client.Infer(context.Background(), validInferenceRequests(), io.Discard)
	if err == nil || !strings.Contains(err.Error(), "openai transient error (500)") {
		t.Fatalf("expected transient HTTP error, got %v", err)
	}
	if sleeps != 1 {
		t.Fatalf("sleeps: got %d want 1", sleeps)
	}
}

func TestClientInfer_InvalidTopLevelResponseJSON(t *testing.T) {
	server := newHTTPTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `not-json`)
	}))

	client := NewClient(Config{
		APIKey:         "test-key",
		Model:          "gpt-4o-mini",
		BaseURL:        server.URL,
		Timeout:        5 * time.Second,
		MaxAttempts:    1,
		InitialBackoff: time.Millisecond,
	})

	_, err := client.Infer(context.Background(), validInferenceRequests(), io.Discard)
	if err == nil || !strings.Contains(err.Error(), "decode openai response") {
		t.Fatalf("expected decode error, got %v", err)
	}
}

func TestClientInfer_UsesNestedOutputTextAndDebugLogs(t *testing.T) {
	var stderr strings.Builder
	server := newHTTPTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"output":[{"content":[{"type":"output_text","text":"{\"candidates\":[{\"message_id\":\"m1\",\"category\":\"client\",\"confidence\":0.82,\"confidence_band\":\"high\",\"review_required\":false,\"evidence\":{\"subject_phrases\":[\"invoice\"],\"snippet_phrases\":[],\"sender_signal\":\"known sender\",\"domain_signal\":\"known domain\",\"label_signals\":[\"INBOX\"]}}]}"}]}]}`)
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

	output, err := client.Infer(context.Background(), validInferenceRequests(), &stderr)
	if err != nil {
		t.Fatalf("Infer: %v", err)
	}
	if len(output) != 1 {
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
		t.Fatalf("timeout: got %v", httpClient.Timeout)
	}
}

func TestClientBuildRequest(t *testing.T) {
	client := NewClient(Config{Model: "gpt-4o-mini"})

	body, err := client.buildRequest(validInferenceRequests())
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
	if req.Text.Format.Name != "inference_candidates" {
		t.Fatalf("schema name: got %q", req.Text.Format.Name)
	}
	if req.Text.Format.Schema["type"] != "object" {
		t.Fatalf("unexpected schema: %+v", req.Text.Format.Schema)
	}
	properties, ok := req.Text.Format.Schema["properties"].(map[string]any)
	if !ok {
		t.Fatalf("schema properties type: %T", req.Text.Format.Schema["properties"])
	}
	candidatesSchema, ok := properties["candidates"].(map[string]any)
	if !ok {
		t.Fatalf("candidates schema type: %T", properties["candidates"])
	}
	itemsSchema, ok := candidatesSchema["items"].(map[string]any)
	if !ok {
		t.Fatalf("items schema type: %T", candidatesSchema["items"])
	}
	itemProperties, ok := itemsSchema["properties"].(map[string]any)
	if !ok {
		t.Fatalf("item properties type: %T", itemsSchema["properties"])
	}
	messageIDSchema, ok := itemProperties["message_id"].(map[string]any)
	if !ok {
		t.Fatalf("message_id schema type: %T", itemProperties["message_id"])
	}
	enum, ok := messageIDSchema["enum"].([]any)
	if !ok || len(enum) != 1 || enum[0] != "m1" {
		t.Fatalf("message_id enum: got %+v", messageIDSchema["enum"])
	}
	if len(req.Input) != 2 || req.Input[0].Role != "system" || req.Input[1].Role != "user" {
		t.Fatalf("unexpected input payload: %+v", req.Input)
	}
	if !strings.Contains(req.Input[1].Content[0].Text, `"message_id":"m1"`) {
		t.Fatalf("missing serialized requests: %q", req.Input[1].Content[0].Text)
	}
}

func TestExtractResponseText_NoOutput(t *testing.T) {
	_, err := extractResponseText(responsesResponse{})
	if err == nil {
		t.Fatal("expected no-output error")
	}
}

func TestExtractResponseText_IgnoresBlankContent(t *testing.T) {
	text, err := extractResponseText(responsesResponse{
		Output: []responseOutput{{
			Content: []responseOutputContent{
				{Type: "output_text", Text: "   "},
				{Type: "output_text", Text: "[]"},
			},
		}},
	})
	if err != nil {
		t.Fatalf("extractResponseText: %v", err)
	}
	if text != "[]" {
		t.Fatalf("text: got %q", text)
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
			if got := req.Header.Get("Content-Type"); got != "application/json" {
				t.Fatalf("Content-Type: got %q", got)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body: io.NopCloser(strings.NewReader(
					`{"output_text":"{\"candidates\":[{\"message_id\":\"m1\",\"category\":\"client\",\"confidence\":0.82,\"confidence_band\":\"high\",\"review_required\":false,\"evidence\":{\"subject_phrases\":[\"invoice\"],\"snippet_phrases\":[],\"sender_signal\":\"known sender\",\"domain_signal\":\"known domain\",\"label_signals\":[\"INBOX\"]}}]}"}`,
				)),
			}, nil
		}),
	}

	output, retry, err := client.sendOnce(context.Background(), []byte(`{"request":true}`), validInferenceRequests(), io.Discard)
	if err != nil {
		t.Fatalf("sendOnce: %v", err)
	}
	if retry {
		t.Fatal("expected retry false")
	}
	if len(output) != 1 || output[0].MessageID != "m1" {
		t.Fatalf("unexpected output: %+v", output)
	}
}

func TestClientSendOnce_InvalidURL(t *testing.T) {
	client := &Client{
		cfg:  Config{APIKey: "test-key", BaseURL: "://bad-url"},
		http: roundTripDoer(func(*http.Request) (*http.Response, error) { t.Fatal("unexpected Do call"); return nil, nil }),
	}

	_, retry, err := client.sendOnce(context.Background(), []byte(`{}`), validInferenceRequests(), io.Discard)
	if err == nil || !strings.Contains(err.Error(), "create openai request") {
		t.Fatalf("expected request creation error, got %v", err)
	}
	if retry {
		t.Fatal("expected retry false")
	}
}

func TestClientSendOnce_NoOutputText(t *testing.T) {
	client := &Client{
		cfg: Config{APIKey: "test-key", BaseURL: "https://api.example.test"},
		http: roundTripDoer(func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{"output":[{"content":[{"type":"output_text","text":"   "}]}]}`)),
			}, nil
		}),
	}

	_, retry, err := client.sendOnce(context.Background(), []byte(`{}`), validInferenceRequests(), io.Discard)
	if err == nil || !errors.Is(err, ErrSchemaViolation) {
		t.Fatalf("expected ErrSchemaViolation, got %v", err)
	}
	if retry {
		t.Fatal("expected retry false")
	}
}

func TestClientInfer_DoErrorRetries(t *testing.T) {
	client := NewClient(Config{
		APIKey:         "test-key",
		Model:          "gpt-4o-mini",
		BaseURL:        "https://example.test",
		Timeout:        5 * time.Second,
		MaxAttempts:    1,
		InitialBackoff: time.Millisecond,
	})
	client.http = roundTripDoer(func(*http.Request) (*http.Response, error) {
		return nil, &net.DNSError{IsTimeout: true}
	})

	_, err := client.Infer(context.Background(), validInferenceRequests(), io.Discard)
	if err == nil {
		t.Fatal("expected call failure")
	}
}

func TestClientInfer_ReadBodyError(t *testing.T) {
	client := NewClient(Config{
		APIKey:         "test-key",
		Model:          "gpt-4o-mini",
		BaseURL:        "https://example.test",
		Timeout:        5 * time.Second,
		MaxAttempts:    1,
		InitialBackoff: time.Millisecond,
	})
	client.http = roundTripDoer(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       errReadCloser{},
		}, nil
	})

	_, err := client.Infer(context.Background(), validInferenceRequests(), io.Discard)
	if err == nil {
		t.Fatal("expected read error")
	}
}

func TestTruncate(t *testing.T) {
	short := truncate([]byte("short body"))
	if short != "short body" {
		t.Fatalf("short truncate: got %q", short)
	}

	long := truncate([]byte(strings.Repeat("x", 350)))
	if !strings.HasSuffix(long, "...") || len(long) != 303 {
		t.Fatalf("long truncate: got len=%d text suffix=%t", len(long), strings.HasSuffix(long, "..."))
	}
}

func TestNormalizeCandidates(t *testing.T) {
	got := normalizeCandidates([]classification.InferenceCandidate{
		{Confidence: 0.82, ConfidenceBand: "low", ReviewRequired: true},
		{Confidence: 0.78, ConfidenceBand: "high", ReviewRequired: false},
		{Confidence: 0.30, ConfidenceBand: "medium", ReviewRequired: true},
	})
	if got[0].ConfidenceBand != "high" || got[0].ReviewRequired {
		t.Fatalf("high normalization: %+v", got[0])
	}
	if got[1].ConfidenceBand != "medium" || !got[1].ReviewRequired {
		t.Fatalf("medium normalization: %+v", got[1])
	}
	if got[2].ConfidenceBand != "low" || got[2].ReviewRequired {
		t.Fatalf("low normalization: %+v", got[2])
	}
}

func validInferenceRequests() []classification.InferenceRequest {
	return []classification.InferenceRequest{{
		MessageID:             "m1",
		MailboxID:             "user@example.com",
		FromEmail:             "sender@example.com",
		FromName:              "Sender",
		Domain:                "example.com",
		Subject:               "Invoice update",
		Snippet:               "Please review the attached invoice",
		Labels:                []string{"INBOX"},
		ReceivedAt:            "2026-04-01T00:00:00Z",
		SenderCount:           3,
		DomainCount:           7,
		DeterministicCategory: classification.CategoryUnknown,
	}}
}
