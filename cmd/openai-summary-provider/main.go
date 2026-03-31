package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	aisummaryopenai "github.com/UreaLaden/inboxatlas/internal/aisummary/openai"
	exportpkg "github.com/UreaLaden/inboxatlas/internal/export"
)

var (
	loadProviderConfig = aisummaryopenai.NewConfigFromEnv
	newOpenAIClient    = func(cfg aisummaryopenai.Config) providerClient {
		return aisummaryopenai.NewClient(cfg)
	}
	runProviderMain           = run
	providerStdin   io.Reader = os.Stdin
	providerStdout  io.Writer = os.Stdout
	providerStderr  io.Writer = os.Stderr
	exitProvider              = os.Exit
)

type providerRequest struct {
	Prompt string                 `json:"prompt"`
	Input  exportpkg.SummaryInput `json:"input"`
}

type providerClient interface {
	GenerateSummary(ctx context.Context, prompt string, input exportpkg.SummaryInput, stderr io.Writer) (exportpkg.SummaryOutput, error)
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

	output, err := newOpenAIClient(cfg).GenerateSummary(ctx, req.Prompt, req.Input, stderr)
	if err != nil {
		return err
	}

	enc := json.NewEncoder(stdout)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(output); err != nil {
		return fmt.Errorf("write summary output: %w", err)
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
	if req.Prompt == "" {
		return providerRequest{}, errors.New("decode provider request: prompt is required")
	}
	return req, nil
}
