// Package openai implements the first-party OpenAI-backed inference provider
// used by the InboxAtlas classify infer workflow.
package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/UreaLaden/inboxatlas/internal/classification"
)

const (
	defaultModel          = "gpt-4o-mini"
	defaultBaseURL        = "https://api.openai.com/v1"
	defaultTimeoutSeconds = 90
	defaultTemperature    = 0.1
	maxAttempts           = 3
)

var (
	// ErrAPIKeyRequired is returned when the provider binary is started without
	// the required OpenAI API key.
	ErrAPIKeyRequired = errors.New("OPENAI_API_KEY is required")

	// ErrSchemaViolation is returned when the OpenAI response cannot be parsed as
	// the required inference candidate schema.
	ErrSchemaViolation = errors.New("openai response did not satisfy inference schema")
)

// Config carries environment-driven runtime settings for the OpenAI inference
// provider.
type Config struct {
	APIKey         string
	Model          string
	BaseURL        string
	Timeout        time.Duration
	Debug          bool
	MaxAttempts    int
	InitialBackoff time.Duration
}

type inferenceCandidatesEnvelope struct {
	Candidates []classification.InferenceCandidate `json:"candidates"`
}

// NewConfigFromEnv loads OpenAI inference-provider configuration from
// environment variables.
func NewConfigFromEnv() (Config, error) {
	apiKey := strings.TrimSpace(os.Getenv("OPENAI_API_KEY"))
	if apiKey == "" {
		return Config{}, ErrAPIKeyRequired
	}

	model := strings.TrimSpace(os.Getenv("OPENAI_INFERENCE_MODEL"))
	if model == "" {
		model = strings.TrimSpace(os.Getenv("OPENAI_MODEL"))
	}
	if model == "" {
		model = defaultModel
	}

	baseURL := strings.TrimSpace(os.Getenv("OPENAI_BASE_URL"))
	if baseURL == "" {
		baseURL = defaultBaseURL
	}
	baseURL = strings.TrimRight(baseURL, "/")

	timeout := time.Duration(defaultTimeoutSeconds) * time.Second
	if raw := strings.TrimSpace(os.Getenv("OPENAI_TIMEOUT_SECONDS")); raw != "" {
		seconds, err := strconv.Atoi(raw)
		if err != nil || seconds <= 0 {
			return Config{}, fmt.Errorf("invalid OPENAI_TIMEOUT_SECONDS %q", raw)
		}
		timeout = time.Duration(seconds) * time.Second
	}

	debug := strings.EqualFold(strings.TrimSpace(os.Getenv("OPENAI_DEBUG")), "true")

	return Config{
		APIKey:         apiKey,
		Model:          model,
		BaseURL:        baseURL,
		Timeout:        timeout,
		Debug:          debug,
		MaxAttempts:    maxAttempts,
		InitialBackoff: 200 * time.Millisecond,
	}, nil
}

type doer interface {
	Do(*http.Request) (*http.Response, error)
}

// Client calls the OpenAI API and returns provider-side schema-validated
// inference candidates.
type Client struct {
	cfg   Config
	http  doer
	sleep func(time.Duration)
}

// NewClient constructs an OpenAI inference client from the given
// configuration.
func NewClient(cfg Config) *Client {
	return &Client{
		cfg: cfg,
		http: &http.Client{
			Timeout: cfg.Timeout,
		},
		sleep: time.Sleep,
	}
}

// Infer sends the deterministic inference batch to OpenAI, enforces
// schema-constrained output generation, and returns provider-side validated
// inference candidates.
func (c *Client) Infer(ctx context.Context, requests []classification.InferenceRequest, stderr io.Writer) ([]classification.InferenceCandidate, error) {
	body, err := c.buildRequest(requests)
	if err != nil {
		return nil, err
	}

	var lastErr error
	backoff := c.cfg.InitialBackoff
	for attempt := 1; attempt <= c.cfg.MaxAttempts; attempt++ {
		if c.cfg.Debug {
			_, _ = fmt.Fprintf(stderr, "ai-inference-provider: attempt %d model=%s\n", attempt, c.cfg.Model)
		}

		output, retry, err := c.sendOnce(ctx, body, requests, stderr)
		if err == nil {
			return output, nil
		}
		lastErr = err
		if !retry || attempt == c.cfg.MaxAttempts {
			break
		}
		c.sleep(backoff)
		backoff *= 2
	}

	return nil, lastErr
}

func (c *Client) buildRequest(requests []classification.InferenceRequest) ([]byte, error) {
	requestsJSON, err := json.Marshal(requests)
	if err != nil {
		return nil, fmt.Errorf("marshal inference requests: %w", err)
	}

	allowedMessageIDs := make([]string, 0, len(requests))
	for _, request := range requests {
		allowedMessageIDs = append(allowedMessageIDs, request.MessageID)
	}

	req := responsesRequest{
		Model:       c.cfg.Model,
		Temperature: defaultTemperature,
		Input: []responseMessage{
			{
				Role: "system",
				Content: []responseContent{
					{Type: "input_text", Text: inferenceSystemPrompt()},
				},
			},
			{
				Role: "user",
				Content: []responseContent{
					{
						Type: "input_text",
						Text: "Return only JSON matching the required schema for the provided inference requests.\n\nRequests:\n" + string(requestsJSON),
					},
				},
			},
		},
		Text: responseTextConfig{
			Format: responseFormat{
				Type:        "json_schema",
				Name:        "inference_candidates",
				Description: "Structured inference candidates for InboxAtlas classify infer",
				Strict:      true,
				Schema:      inferenceCandidateSchema(allowedMessageIDs),
			},
		},
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal openai request: %w", err)
	}
	return body, nil
}

func (c *Client) sendOnce(ctx context.Context, body []byte, requests []classification.InferenceRequest, stderr io.Writer) ([]classification.InferenceCandidate, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.BaseURL+"/responses", bytes.NewReader(body))
	if err != nil {
		return nil, false, fmt.Errorf("create openai request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, shouldRetryError(err), fmt.Errorf("call openai: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, false, fmt.Errorf("read openai response: %w", err)
	}

	if c.cfg.Debug {
		_, _ = fmt.Fprintf(stderr, "ai-inference-provider: status=%d body=%s\n", resp.StatusCode, string(respBody))
	}

	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		return nil, true, fmt.Errorf("openai transient error (%d): %s", resp.StatusCode, truncate(respBody))
	}
	if resp.StatusCode >= 400 {
		return nil, false, fmt.Errorf("openai request failed (%d): %s", resp.StatusCode, truncate(respBody))
	}

	var parsed responsesResponse
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return nil, false, fmt.Errorf("decode openai response: %w", err)
	}

	text, err := extractResponseText(parsed)
	if err != nil {
		return nil, false, err
	}

	var envelope inferenceCandidatesEnvelope
	if err := json.Unmarshal([]byte(text), &envelope); err != nil {
		return nil, false, fmt.Errorf("%w: parse inference candidates: %v", ErrSchemaViolation, err)
	}
	envelope.Candidates = normalizeCandidates(envelope.Candidates)

	requestByID := make(map[string]classification.InferenceRequest, len(requests))
	for _, request := range requests {
		requestByID[request.MessageID] = request
	}
	for _, candidate := range envelope.Candidates {
		if err := classification.ValidateInferenceCandidate(candidate, requestByID); err != nil {
			return nil, false, fmt.Errorf("%w: %v", ErrSchemaViolation, err)
		}
	}
	return envelope.Candidates, false, nil
}

func normalizeCandidates(candidates []classification.InferenceCandidate) []classification.InferenceCandidate {
	for i := range candidates {
		band := classification.InferenceConfidenceBand(candidates[i].Confidence)
		candidates[i].ConfidenceBand = band
		candidates[i].ReviewRequired = band == "medium"
	}
	return candidates
}

func shouldRetryError(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.ErrUnexpectedEOF)
}

func extractResponseText(resp responsesResponse) (string, error) {
	if strings.TrimSpace(resp.OutputText) != "" {
		return resp.OutputText, nil
	}
	for _, item := range resp.Output {
		for _, content := range item.Content {
			if strings.TrimSpace(content.Text) != "" {
				return content.Text, nil
			}
		}
	}
	return "", fmt.Errorf("%w: response contained no output text", ErrSchemaViolation)
}

func truncate(body []byte) string {
	const max = 300
	text := strings.TrimSpace(string(body))
	if len(text) <= max {
		return text
	}
	return text[:max] + "..."
}

func inferenceSystemPrompt() string {
	return strings.TrimSpace(`
You are an InboxAtlas classification inference provider.
Review each request independently and return only a JSON array of inference candidates.
Only emit candidates when the message is still suitable for operator review.
Use only these categories: internal, client, vendor, government, system-generated, newsletter/marketing, social, unknown.
Confidence must be a number from 0.0 to 1.0.
Use confidence bands exactly as follows:
- high for confidence >= 0.80 with review_required false
- medium for confidence >= 0.55 and < 0.80 with review_required true
- low for confidence < 0.55 with review_required false
Evidence fields must stay concise and structured.
If there is not enough signal, return an empty array or a low-confidence candidate.
`)
}

func inferenceCandidateSchema(allowedMessageIDs []string) map[string]any {
	return map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"required":             []string{"candidates"},
		"properties": map[string]any{
			"candidates": map[string]any{
				"type": "array",
				"items": map[string]any{
					"type":                 "object",
					"additionalProperties": false,
					"required": []string{
						"message_id",
						"category",
						"confidence",
						"confidence_band",
						"evidence",
						"review_required",
					},
					"properties": map[string]any{
						"message_id":      map[string]any{"type": "string", "enum": allowedMessageIDs},
						"category":        map[string]any{"type": "string"},
						"confidence":      map[string]any{"type": "number"},
						"confidence_band": map[string]any{"type": "string", "enum": []string{"high", "medium", "low"}},
						"review_required": map[string]any{"type": "boolean"},
						"evidence": map[string]any{
							"type":                 "object",
							"additionalProperties": false,
							"required":             []string{"subject_phrases", "snippet_phrases", "sender_signal", "domain_signal", "label_signals"},
							"properties": map[string]any{
								"subject_phrases": map[string]any{
									"type":  "array",
									"items": map[string]any{"type": "string"},
								},
								"snippet_phrases": map[string]any{
									"type":  "array",
									"items": map[string]any{"type": "string"},
								},
								"sender_signal": map[string]any{"type": "string"},
								"domain_signal": map[string]any{"type": "string"},
								"label_signals": map[string]any{
									"type":  "array",
									"items": map[string]any{"type": "string"},
								},
							},
						},
					},
				},
			},
		},
	}
}

type responsesRequest struct {
	Model       string             `json:"model"`
	Temperature float64            `json:"temperature,omitempty"`
	Input       []responseMessage  `json:"input"`
	Text        responseTextConfig `json:"text"`
}

type responseMessage struct {
	Role    string            `json:"role"`
	Content []responseContent `json:"content"`
}

type responseContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type responseTextConfig struct {
	Format responseFormat `json:"format"`
}

type responseFormat struct {
	Type        string         `json:"type"`
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	Strict      bool           `json:"strict"`
	Schema      map[string]any `json:"schema"`
}

type responsesResponse struct {
	OutputText string           `json:"output_text"`
	Output     []responseOutput `json:"output"`
}

type responseOutput struct {
	Content []responseOutputContent `json:"content"`
}

type responseOutputContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}
