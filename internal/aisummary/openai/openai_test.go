package openai

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	exportpkg "github.com/UreaLaden/inboxatlas/internal/export"
)

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
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	defer server.Close()

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
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 3 {
			http.Error(w, `{"error":"rate limited"}`, http.StatusTooManyRequests)
			return
		}
		_, _ = io.WriteString(w, `{"output_text":"{\"title\":\"Inbox Snapshot\",\"subtitle\":\"owner@company.com\",\"headline\":\"Email volume increased from 5 to 8 messages across 2025-01 to 2025-03.\",\"secondary_headline\":\"alerts@vendor.com remained the top external sender with 9 messages.\",\"snapshot_bullets\":[\"vendor.com is the most active external domain with 9 messages.\"],\"what_this_means_bullets\":[\"A small number of external sources shape most of the 20 messages in scope.\"],\"opportunities_bullets\":[\"Promote repeated alerts from alerts@vendor.com into a review workflow.\"],\"bottom_line\":\"The inbox shows stable patterns that are ready for structured automation review.\"}"}`)
	}))
	defer server.Close()

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
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
	}))
	defer server.Close()

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
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"output_text":"not-json"}`)
	}))
	defer server.Close()

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
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"output":[{"content":[{"type":"output_text","text":"{\"title\":\"Inbox Snapshot\",\"subtitle\":\"owner@company.com\",\"headline\":\"Email volume increased from 5 to 8 messages across 2025-01 to 2025-03.\",\"secondary_headline\":\"alerts@vendor.com remained the top external sender with 9 messages.\",\"snapshot_bullets\":[\"vendor.com is the most active external domain with 9 messages.\"],\"what_this_means_bullets\":[\"A small number of external sources shape most of the 20 messages in scope.\"],\"opportunities_bullets\":[\"Promote repeated alerts from alerts@vendor.com into a review workflow.\"],\"bottom_line\":\"The inbox shows stable patterns that are ready for structured automation review.\"}"}]}]}`)
	}))
	defer server.Close()

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
