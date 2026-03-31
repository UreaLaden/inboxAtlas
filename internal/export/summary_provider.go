package export

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

// SummaryProvider generates structured summary output from a deterministic
// summary input payload and prompt contract.
type SummaryProvider interface {
	GenerateSummary(ctx context.Context, prompt string, input SummaryInput) (SummaryOutput, error)
}

// CommandSummaryProvider invokes an external command that accepts a JSON
// request on stdin and returns a JSON SummaryOutput on stdout.
type CommandSummaryProvider struct {
	Command string
	Args    []string
}

type summaryProviderRequest struct {
	Prompt string       `json:"prompt"`
	Input  SummaryInput `json:"input"`
}

// GenerateSummary executes the configured command-backed provider and returns
// validated structured summary output.
func (p CommandSummaryProvider) GenerateSummary(ctx context.Context, prompt string, input SummaryInput) (SummaryOutput, error) {
	if p.Command == "" {
		return SummaryOutput{}, fmt.Errorf("%w: command is required", ErrSummaryOutputInvalid)
	}

	reqBody, err := json.Marshal(summaryProviderRequest{
		Prompt: prompt,
		Input:  input,
	})
	if err != nil {
		return SummaryOutput{}, fmt.Errorf("marshal summary request: %w", err)
	}

	cmd := exec.CommandContext(ctx, p.Command, p.Args...)
	cmd.Stdin = bytes.NewReader(reqBody)
	var stderr bytes.Buffer
	cmd.Stderr = io.MultiWriter(os.Stderr, &stderr)
	output, err := cmd.Output()
	if err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			return SummaryOutput{}, fmt.Errorf("run summary provider: %s", strings.TrimSpace(stderr.String()))
		}
		return SummaryOutput{}, fmt.Errorf("run summary provider: %w", err)
	}

	return ParseSummaryOutputJSON(output)
}
