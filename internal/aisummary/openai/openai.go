// Package openai implements the first-party OpenAI-backed summary provider
// used by the InboxAtlas report summarize workflow.
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

	exportpkg "github.com/UreaLaden/inboxatlas/internal/export"
)

const (
	defaultModel          = "gpt-4o-mini"
	defaultBaseURL        = "https://api.openai.com/v1"
	defaultTimeoutSeconds = 90
	defaultTemperature    = 0.2
	maxAttempts           = 3
)

var (
	// ErrAPIKeyRequired is returned when the provider binary is started without
	// the required OpenAI API key.
	ErrAPIKeyRequired = errors.New("OPENAI_API_KEY is required")

	// ErrSchemaViolation is returned when the OpenAI response cannot be parsed as
	// the required SummaryOutput schema.
	ErrSchemaViolation = errors.New("openai response did not satisfy summary schema")
)

// Config carries environment-driven runtime settings for the OpenAI summary
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

// NewConfigFromEnv loads OpenAI provider configuration from environment
// variables.
func NewConfigFromEnv() (Config, error) {
	apiKey := strings.TrimSpace(os.Getenv("OPENAI_API_KEY"))
	if apiKey == "" {
		return Config{}, ErrAPIKeyRequired
	}

	model := strings.TrimSpace(os.Getenv("OPENAI_MODEL"))
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
// SummaryOutput values.
type Client struct {
	cfg   Config
	http  doer
	sleep func(time.Duration)
}

// NewClient constructs an OpenAI summary client from the given configuration.
func NewClient(cfg Config) *Client {
	return &Client{
		cfg: cfg,
		http: &http.Client{
			Timeout: cfg.Timeout,
		},
		sleep: time.Sleep,
	}
}

// GenerateSummary sends the deterministic summary input plus prompt contract to
// OpenAI, enforces schema-constrained output generation, and returns a
// provider-side syntax-validated SummaryOutput.
func (c *Client) GenerateSummary(ctx context.Context, prompt string, input exportpkg.SummaryInput, stderr io.Writer) (exportpkg.SummaryOutput, error) {
	body, err := c.buildRequest(prompt, input)
	if err != nil {
		return exportpkg.SummaryOutput{}, err
	}

	var lastErr error
	backoff := c.cfg.InitialBackoff
	for attempt := 1; attempt <= c.cfg.MaxAttempts; attempt++ {
		if c.cfg.Debug {
			_, _ = fmt.Fprintf(stderr, "openai-summary-provider: attempt %d model=%s\n", attempt, c.cfg.Model)
		}

		output, retry, err := c.sendOnce(ctx, body, stderr)
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

	return exportpkg.SummaryOutput{}, lastErr
}

func (c *Client) buildRequest(prompt string, input exportpkg.SummaryInput) ([]byte, error) {
	inputJSON, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("marshal summary input: %w", err)
	}

	req := responsesRequest{
		Model:       c.cfg.Model,
		Temperature: defaultTemperature,
		Input: []responseMessage{
			{
				Role: "system",
				Content: []responseContent{
					{Type: "input_text", Text: prompt},
				},
			},
			{
				Role: "user",
				Content: []responseContent{
					{
						Type: "input_text",
						Text: "Use the provided deterministic payload and return only JSON matching the required schema.\n\nPayload:\n" + string(inputJSON),
					},
				},
			},
		},
		Text: responseTextConfig{
			Format: responseFormat{
				Type:        "json_schema",
				Name:        "summary_output",
				Description: "Structured summary output for InboxAtlas snapshot generation",
				Strict:      true,
				Schema:      summaryOutputSchema(),
			},
		},
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal openai request: %w", err)
	}
	return body, nil
}

func (c *Client) sendOnce(ctx context.Context, body []byte, stderr io.Writer) (exportpkg.SummaryOutput, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.BaseURL+"/responses", bytes.NewReader(body))
	if err != nil {
		return exportpkg.SummaryOutput{}, false, fmt.Errorf("create openai request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return exportpkg.SummaryOutput{}, shouldRetryError(err), fmt.Errorf("call openai: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return exportpkg.SummaryOutput{}, false, fmt.Errorf("read openai response: %w", err)
	}

	if c.cfg.Debug {
		_, _ = fmt.Fprintf(stderr, "openai-summary-provider: status=%d body=%s\n", resp.StatusCode, string(respBody))
	}

	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		return exportpkg.SummaryOutput{}, true, fmt.Errorf("openai transient error (%d): %s", resp.StatusCode, truncate(respBody))
	}
	if resp.StatusCode >= 400 {
		return exportpkg.SummaryOutput{}, false, fmt.Errorf("openai request failed (%d): %s", resp.StatusCode, truncate(respBody))
	}

	var parsed responsesResponse
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return exportpkg.SummaryOutput{}, false, fmt.Errorf("decode openai response: %w", err)
	}

	text, err := extractResponseText(parsed)
	if err != nil {
		return exportpkg.SummaryOutput{}, false, err
	}

	output, err := exportpkg.ParseSummaryOutputJSON([]byte(text))
	if err != nil {
		return exportpkg.SummaryOutput{}, false, fmt.Errorf("%w: %v", ErrSchemaViolation, err)
	}
	return output, false, nil
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

func summaryOutputSchema() map[string]any {
	bullets := map[string]any{
		"type":        "array",
		"minItems":    1,
		"maxItems":    5,
		"items":       map[string]any{"type": "string"},
		"description": "A concise list of one-sentence bullets.",
	}

	return map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"required": []string{
			"title",
			"subtitle",
			"headline",
			"secondary_headline",
			"snapshot_bullets",
			"what_this_means_bullets",
			"opportunities_bullets",
			"bottom_line",
		},
		"properties": map[string]any{
			"title":                   map[string]any{"type": "string"},
			"subtitle":                map[string]any{"type": "string"},
			"headline":                map[string]any{"type": "string"},
			"secondary_headline":      map[string]any{"type": "string"},
			"snapshot_bullets":        bullets,
			"what_this_means_bullets": bullets,
			"opportunities_bullets":   bullets,
			"bottom_line":             map[string]any{"type": "string"},
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
