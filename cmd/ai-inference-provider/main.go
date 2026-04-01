package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	aiinferenceopenai "github.com/UreaLaden/inboxatlas/internal/aiinference/openai"
	"github.com/UreaLaden/inboxatlas/internal/classification"
)

var (
	loadProviderConfig = aiinferenceopenai.NewConfigFromEnv
	newOpenAIClient    = func(cfg aiinferenceopenai.Config) providerClient {
		return aiinferenceopenai.NewClient(cfg)
	}
	runProviderMain           = run
	providerStdin   io.Reader = os.Stdin
	providerStdout  io.Writer = os.Stdout
	providerStderr  io.Writer = os.Stderr
	exitProvider              = os.Exit
)

type providerRequest struct {
	Requests []classification.InferenceRequest `json:"requests"`
}

type providerClient interface {
	Infer(ctx context.Context, requests []classification.InferenceRequest, stderr io.Writer) ([]classification.InferenceCandidate, error)
}

func main() {
	if err := runProviderMain(context.Background(), providerStdin, providerStdout, providerStderr); err != nil {
		_, _ = fmt.Fprintln(providerStderr, err)
		exitProvider(1)
	}
}

func run(ctx context.Context, stdin io.Reader, stdout, stderr io.Writer) error {
	cfg, err := loadProviderConfig()
	if err != nil {
		return err
	}

	req, err := readProviderRequest(stdin)
	if err != nil {
		return err
	}

	output, err := newOpenAIClient(cfg).Infer(ctx, req.Requests, stderr)
	if err != nil {
		return err
	}

	enc := json.NewEncoder(stdout)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(output); err != nil {
		return fmt.Errorf("write inference output: %w", err)
	}
	return nil
}

func readProviderRequest(r io.Reader) (providerRequest, error) {
	var req providerRequest
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		return providerRequest{}, fmt.Errorf("decode provider request: %w", err)
	}
	if err := dec.Decode(new(struct{})); err != io.EOF {
		return providerRequest{}, errors.New("decode provider request: extra trailing content")
	}
	if len(req.Requests) == 0 {
		return providerRequest{}, errors.New("decode provider request: requests are required")
	}
	return req, nil
}
