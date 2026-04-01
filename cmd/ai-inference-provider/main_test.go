package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	aiinferenceopenai "github.com/UreaLaden/inboxatlas/internal/aiinference/openai"
	"github.com/UreaLaden/inboxatlas/internal/classification"
)

func TestRun_MissingAPIKey(t *testing.T) {
	prevLoad := loadProviderConfig
	prevClient := newOpenAIClient
	t.Cleanup(func() {
		loadProviderConfig = prevLoad
		newOpenAIClient = prevClient
	})

	loadProviderConfig = func() (aiinferenceopenai.Config, error) {
		return aiinferenceopenai.Config{}, aiinferenceopenai.ErrAPIKeyRequired
	}

	var stdout, stderr bytes.Buffer
	err := run(context.Background(), strings.NewReader(`{"requests":[{"message_id":"m1"}]}`), &stdout, &stderr)
	if !errors.Is(err, aiinferenceopenai.ErrAPIKeyRequired) {
		t.Fatalf("expected ErrAPIKeyRequired, got %v", err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("expected empty stdout, got %q", stdout.String())
	}
}

func TestRun_MalformedStdinRequest(t *testing.T) {
	prevLoad := loadProviderConfig
	prevClient := newOpenAIClient
	t.Cleanup(func() {
		loadProviderConfig = prevLoad
		newOpenAIClient = prevClient
	})

	loadProviderConfig = func() (aiinferenceopenai.Config, error) {
		return aiinferenceopenai.Config{APIKey: "x"}, nil
	}
	newOpenAIClient = func(cfg aiinferenceopenai.Config) providerClient {
		t.Fatalf("client should not be constructed for malformed input")
		return nil
	}

	err := run(context.Background(), strings.NewReader(`{"requests":`), &bytes.Buffer{}, &bytes.Buffer{})
	if err == nil {
		t.Fatal("expected malformed stdin error")
	}
}

func TestRun_SuccessJSONOnlyStdout(t *testing.T) {
	prevLoad := loadProviderConfig
	prevClient := newOpenAIClient
	t.Cleanup(func() {
		loadProviderConfig = prevLoad
		newOpenAIClient = prevClient
	})

	loadProviderConfig = func() (aiinferenceopenai.Config, error) {
		return aiinferenceopenai.Config{APIKey: "x"}, nil
	}
	newOpenAIClient = func(cfg aiinferenceopenai.Config) providerClient {
		return providerClientStub{
			output: []classification.InferenceCandidate{{
				MessageID:      "m1",
				Category:       classification.CategoryClient,
				Confidence:     0.82,
				ConfidenceBand: "high",
				ReviewRequired: false,
			}},
		}
	}

	req := `{"requests":[{"message_id":"m1","mailbox_id":"user@example.com","from_email":"sender@example.com","domain":"example.com","subject":"Invoice","snippet":"Please review","labels":["INBOX"],"received_at":"2026-04-01T00:00:00Z","sender_count":3,"domain_count":7,"deterministic_category":"unknown"}]}`

	var stdout, stderr bytes.Buffer
	if err := run(context.Background(), strings.NewReader(req), &stdout, &stderr); err != nil {
		t.Fatalf("run: %v", err)
	}
	if stderr.Len() != 0 {
		t.Fatalf("expected empty stderr, got %q", stderr.String())
	}
	if !strings.HasPrefix(stdout.String(), `[{"message_id":"m1"`) {
		t.Fatalf("expected JSON-only stdout, got %q", stdout.String())
	}
}

func TestReadProviderRequest_RequestsRequired(t *testing.T) {
	_, err := readProviderRequest(strings.NewReader(`{"requests":[]}`))
	if err == nil {
		t.Fatal("expected requests required error")
	}
}

func TestReadProviderRequest_ExtraTrailingContent(t *testing.T) {
	_, err := readProviderRequest(strings.NewReader(`{"requests":[{"message_id":"m1"}]}{"extra":true}`))
	if err == nil {
		t.Fatal("expected trailing content error")
	}
}

func TestReadProviderRequest_UnknownFieldRejected(t *testing.T) {
	_, err := readProviderRequest(strings.NewReader(`{"requests":[{"message_id":"m1"}],"extra":true}`))
	if err == nil {
		t.Fatal("expected unknown field error")
	}
}

func TestRun_ClientErrorLeavesStdoutEmpty(t *testing.T) {
	prevLoad := loadProviderConfig
	prevClient := newOpenAIClient
	t.Cleanup(func() {
		loadProviderConfig = prevLoad
		newOpenAIClient = prevClient
	})

	loadProviderConfig = func() (aiinferenceopenai.Config, error) {
		return aiinferenceopenai.Config{APIKey: "x"}, nil
	}
	newOpenAIClient = func(cfg aiinferenceopenai.Config) providerClient {
		return providerClientStub{err: errors.New("provider failure")}
	}

	var stdout, stderr bytes.Buffer
	err := run(context.Background(), strings.NewReader(`{"requests":[{"message_id":"m1"}]}`), &stdout, &stderr)
	if err == nil {
		t.Fatal("expected failure")
	}
	if stdout.Len() != 0 {
		t.Fatalf("expected empty stdout, got %q", stdout.String())
	}
}

func TestRun_EncodeFailure(t *testing.T) {
	prevLoad := loadProviderConfig
	prevClient := newOpenAIClient
	t.Cleanup(func() {
		loadProviderConfig = prevLoad
		newOpenAIClient = prevClient
	})

	loadProviderConfig = func() (aiinferenceopenai.Config, error) {
		return aiinferenceopenai.Config{APIKey: "x"}, nil
	}
	newOpenAIClient = func(cfg aiinferenceopenai.Config) providerClient {
		return providerClientStub{
			output: []classification.InferenceCandidate{{
				MessageID:      "m1",
				Category:       classification.CategoryClient,
				Confidence:     0.82,
				ConfidenceBand: "high",
			}},
		}
	}

	err := run(context.Background(), strings.NewReader(`{"requests":[{"message_id":"m1"}]}`), failingWriter{}, &bytes.Buffer{})
	if err == nil {
		t.Fatal("expected encode error")
	}
}

func TestMain_ErrorExit(t *testing.T) {
	prevRun := runProviderMain
	prevStdin := providerStdin
	prevStdout := providerStdout
	prevStderr := providerStderr
	prevExit := exitProvider
	t.Cleanup(func() {
		runProviderMain = prevRun
		providerStdin = prevStdin
		providerStdout = prevStdout
		providerStderr = prevStderr
		exitProvider = prevExit
	})

	var stderr bytes.Buffer
	runProviderMain = func(context.Context, io.Reader, io.Writer, io.Writer) error {
		return errors.New("boom")
	}
	providerStdin = strings.NewReader("")
	providerStdout = &bytes.Buffer{}
	providerStderr = &stderr

	exitCode := 0
	exitProvider = func(code int) {
		exitCode = code
	}

	main()

	if exitCode != 1 {
		t.Fatalf("exitCode: got %d, want 1", exitCode)
	}
	if !strings.Contains(stderr.String(), "boom") {
		t.Fatalf("stderr: got %q", stderr.String())
	}
}

func TestMain_Success(t *testing.T) {
	prevRun := runProviderMain
	prevStdin := providerStdin
	prevStdout := providerStdout
	prevStderr := providerStderr
	prevExit := exitProvider
	t.Cleanup(func() {
		runProviderMain = prevRun
		providerStdin = prevStdin
		providerStdout = prevStdout
		providerStderr = prevStderr
		exitProvider = prevExit
	})

	runCalled := false
	runProviderMain = func(context.Context, io.Reader, io.Writer, io.Writer) error {
		runCalled = true
		return nil
	}
	providerStdin = strings.NewReader("")
	providerStdout = &bytes.Buffer{}
	providerStderr = &bytes.Buffer{}

	exitCode := 0
	exitProvider = func(code int) {
		exitCode = code
	}

	main()

	if !runCalled {
		t.Fatal("expected run to be called")
	}
	if exitCode != 0 {
		t.Fatalf("unexpected exit code: %d", exitCode)
	}
}

type providerClientStub struct {
	output []classification.InferenceCandidate
	err    error
}

func (s providerClientStub) Infer(context.Context, []classification.InferenceRequest, io.Writer) ([]classification.InferenceCandidate, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.output, nil
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) {
	return 0, errors.New("write failed")
}
