package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	aisummaryopenai "github.com/UreaLaden/inboxatlas/internal/aisummary/openai"
	exportpkg "github.com/UreaLaden/inboxatlas/internal/export"
)

func TestRun_MissingAPIKey(t *testing.T) {
	prevLoad := loadProviderConfig
	prevClient := newOpenAIClient
	t.Cleanup(func() {
		loadProviderConfig = prevLoad
		newOpenAIClient = prevClient
	})

	loadProviderConfig = func() (aisummaryopenai.Config, error) {
		return aisummaryopenai.Config{}, aisummaryopenai.ErrAPIKeyRequired
	}

	var stdout, stderr bytes.Buffer
	err := run(context.Background(), strings.NewReader(`{"prompt":"x","input":{"total_messages":0}}`), &stdout, &stderr)
	if !errors.Is(err, aisummaryopenai.ErrAPIKeyRequired) {
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

	loadProviderConfig = func() (aisummaryopenai.Config, error) {
		return aisummaryopenai.Config{APIKey: "x"}, nil
	}
	newOpenAIClient = func(cfg aisummaryopenai.Config) providerClient {
		t.Fatalf("client should not be constructed for malformed input")
		return nil
	}

	err := run(context.Background(), strings.NewReader(`{"prompt":"x","input":`), &bytes.Buffer{}, &bytes.Buffer{})
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

	loadProviderConfig = func() (aisummaryopenai.Config, error) {
		return aisummaryopenai.Config{APIKey: "x"}, nil
	}
	newOpenAIClient = func(cfg aisummaryopenai.Config) providerClient {
		return providerClientStub{
			output: exportpkg.SummaryOutput{
				Title:                "Inbox Snapshot",
				Subtitle:             "owner@company.com",
				Headline:             "Email volume increased from 5 to 8 messages across 2025-01 to 2025-03.",
				SecondaryHeadline:    "alerts@vendor.com remained the top external sender with 9 messages.",
				SnapshotBullets:      []string{"vendor.com is the most active external domain with 9 messages."},
				WhatThisMeansBullets: []string{"A small number of external sources shape most of the 20 messages in scope."},
				OpportunitiesBullets: []string{"Promote repeated alerts from alerts@vendor.com into a review workflow."},
				BottomLine:           "The inbox shows stable patterns that are ready for structured automation review.",
			},
		}
	}

	req := `{"prompt":"prompt","input":{"owner":{"email":"owner@company.com","domain":"company.com"},"reporting_period":{"start":"2025-01","end":"2025-03","label":"2025-01 to 2025-03"},"total_messages":20,"top_external_senders":[],"top_external_domains":[],"top_subject_themes":[],"volume_highlights":{"first_period":"2025-01","first_count":5,"last_period":"2025-03","last_count":8,"absolute_change":3,"percent_change":60,"peak_period":"2025-03","peak_count":8},"derived_metrics":{"top_external_sender":"alerts@vendor.com","top_external_sender_count":9,"top_external_domain":"vendor.com","top_external_domain_count":9}}}`

	var stdout, stderr bytes.Buffer
	if err := run(context.Background(), strings.NewReader(req), &stdout, &stderr); err != nil {
		t.Fatalf("run: %v", err)
	}
	if stderr.Len() != 0 {
		t.Fatalf("expected empty stderr, got %q", stderr.String())
	}
	if !strings.HasPrefix(stdout.String(), `{"title":"Inbox Snapshot"`) {
		t.Fatalf("expected JSON-only stdout, got %q", stdout.String())
	}
}

func TestRun_InvalidModelOutputFailure(t *testing.T) {
	prevLoad := loadProviderConfig
	prevClient := newOpenAIClient
	t.Cleanup(func() {
		loadProviderConfig = prevLoad
		newOpenAIClient = prevClient
	})

	loadProviderConfig = func() (aisummaryopenai.Config, error) {
		return aisummaryopenai.Config{APIKey: "x"}, nil
	}
	newOpenAIClient = func(cfg aisummaryopenai.Config) providerClient {
		return providerClientStub{err: errors.New("schema violation")}
	}

	var stdout, stderr bytes.Buffer
	err := run(context.Background(), strings.NewReader(`{"prompt":"prompt","input":{"total_messages":0,"owner":{},"reporting_period":{},"top_external_senders":[],"top_external_domains":[],"top_subject_themes":[],"volume_highlights":{},"derived_metrics":{}}}`), &stdout, &stderr)
	if err == nil {
		t.Fatal("expected failure")
	}
	if stdout.Len() != 0 {
		t.Fatalf("expected empty stdout, got %q", stdout.String())
	}
}

func TestReadProviderRequest_PromptRequired(t *testing.T) {
	_, err := readProviderRequest(strings.NewReader(`{"prompt":"","input":{"total_messages":0,"owner":{},"reporting_period":{},"top_external_senders":[],"top_external_domains":[],"top_subject_themes":[],"volume_highlights":{},"derived_metrics":{}}}`))
	if err == nil {
		t.Fatal("expected prompt required error")
	}
}

func TestReadProviderRequest_ExtraTrailingContent(t *testing.T) {
	_, err := readProviderRequest(strings.NewReader(`{"prompt":"x","input":{"total_messages":0,"owner":{},"reporting_period":{},"top_external_senders":[],"top_external_domains":[],"top_subject_themes":[],"volume_highlights":{},"derived_metrics":{}}}{"extra":true}`))
	if err == nil {
		t.Fatal("expected trailing content error")
	}
}

func TestReadProviderRequest_UnknownFieldRejected(t *testing.T) {
	_, err := readProviderRequest(strings.NewReader(`{"prompt":"x","input":{"total_messages":0,"owner":{},"reporting_period":{},"top_external_senders":[],"top_external_domains":[],"top_subject_themes":[],"volume_highlights":{},"derived_metrics":{}},"extra":true}`))
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

	loadProviderConfig = func() (aisummaryopenai.Config, error) {
		return aisummaryopenai.Config{APIKey: "x"}, nil
	}
	newOpenAIClient = func(cfg aisummaryopenai.Config) providerClient {
		return providerClientStub{err: errors.New("provider failure")}
	}

	var stdout, stderr bytes.Buffer
	err := run(context.Background(), strings.NewReader(`{"prompt":"prompt","input":{"total_messages":0,"owner":{},"reporting_period":{},"top_external_senders":[],"top_external_domains":[],"top_subject_themes":[],"volume_highlights":{},"derived_metrics":{}}}`), &stdout, &stderr)
	if err == nil {
		t.Fatal("expected failure")
	}
	if stdout.Len() != 0 {
		t.Fatalf("expected empty stdout, got %q", stdout.String())
	}
}

type providerClientStub struct {
	output exportpkg.SummaryOutput
	err    error
}

func (s providerClientStub) GenerateSummary(_ context.Context, _ string, _ exportpkg.SummaryInput, _ io.Writer) (exportpkg.SummaryOutput, error) {
	if s.err != nil {
		return exportpkg.SummaryOutput{}, s.err
	}
	return s.output, nil
}
