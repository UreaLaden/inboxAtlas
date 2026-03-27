package export

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestParseSnapshotNarrative_ValidMarkdown(t *testing.T) {
	narrative, err := ParseSnapshotNarrative([]byte(snapshotMarkdown))
	if err != nil {
		t.Fatalf("ParseSnapshotNarrative: %v", err)
	}

	if narrative.Title != "Inbox Snapshot" {
		t.Fatalf("Title: got %q", narrative.Title)
	}
	if narrative.Subtitle != "owner@company.com" {
		t.Fatalf("Subtitle: got %q", narrative.Subtitle)
	}
	if narrative.Headline == "" || narrative.SecondaryHeadline == "" || narrative.BottomLine == "" {
		t.Fatalf("expected populated headline fields, got %+v", narrative)
	}
	if len(narrative.Snapshot) != 3 || len(narrative.WhatThisMeans) != 2 || len(narrative.Opportunities) != 3 {
		t.Fatalf("unexpected narrative bullets: %+v", narrative)
	}
}

func TestLoadSnapshotNarrative_FromFile(t *testing.T) {
	narrative, err := LoadSnapshotNarrative("testdata/valid/summary.md")
	if err != nil {
		t.Fatalf("LoadSnapshotNarrative: %v", err)
	}
	if narrative.BottomLine == "" {
		t.Fatalf("expected parsed bottom line, got %+v", narrative)
	}
}

func TestLoadSnapshotNarrative_MissingFile(t *testing.T) {
	_, err := LoadSnapshotNarrative("testdata/valid/missing-summary.md")
	if err == nil {
		t.Fatal("expected read error")
	}
	if !strings.Contains(err.Error(), "read snapshot narrative") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParseSnapshotNarrative_MissingRequiredSection(t *testing.T) {
	_, err := ParseSnapshotNarrative([]byte(`# Inbox Snapshot

## Key Takeaway
Volume surged.
`))
	if err == nil {
		t.Fatal("expected narrative validation error")
	}
	if !errors.Is(err, ErrSnapshotNarrativeRequired) {
		t.Fatalf("expected ErrSnapshotNarrativeRequired, got %v", err)
	}
	if !strings.Contains(err.Error(), "secondary takeaway") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParseSnapshotNarrative_UsesReportingPeriodAsSubtitleFallback(t *testing.T) {
	narrative, err := ParseSnapshotNarrative([]byte(`# Inbox Snapshot

## Reporting Period
January to March 2025

## Key Takeaway
Volume surged.

## Secondary Takeaway
Automated senders dominate.

## Snapshot
- One

## What This Means
- Two

## Opportunities to Improve
- Three

## Bottom Line
Four
`))
	if err != nil {
		t.Fatalf("ParseSnapshotNarrative: %v", err)
	}
	if narrative.Subtitle != "January to March 2025" {
		t.Fatalf("Subtitle: got %q", narrative.Subtitle)
	}
}

func TestBuildSnapshotHTML_RequiredSectionsAndContent(t *testing.T) {
	model, err := ParseReportsDir(Options{
		ReportsDir: "testdata/valid",
		OwnerEmail: "owner@company.com",
	})
	if err != nil {
		t.Fatalf("ParseReportsDir: %v", err)
	}

	narrative, err := ParseSnapshotNarrative([]byte(snapshotMarkdown))
	if err != nil {
		t.Fatalf("ParseSnapshotNarrative: %v", err)
	}

	html, err := BuildSnapshotHTML(model, narrative, SnapshotOptions{
		GeneratedAt: time.Date(2026, time.March, 27, 15, 30, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("BuildSnapshotHTML: %v", err)
	}

	body := string(html)
	for _, want := range []string{
		"<!DOCTYPE html>",
		"<h1 class=\"title\">Inbox Snapshot</h1>",
		"owner@company.com",
		"Reporting Period: 2025-01 to 2025-03 | Generated: 2026-03-27",
		"<h2>Snapshot</h2>",
		"<h2>What This Means</h2>",
		"<h2>Opportunities to Improve</h2>",
		"<h2>Bottom Line</h2>",
		"alerts@vendor.com (9)",
		"vendor.com (9)",
		"invoice (5)",
		"Email volume increased from 5 to 8 messages across the reporting window.",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("snapshot html missing %q", want)
		}
	}
}

func TestBuildSnapshotHTML_DefaultTitleAndUnscopedSubtitle(t *testing.T) {
	model, err := ParseReportsDir(Options{
		ReportsDir: "testdata/valid",
	})
	if err != nil {
		t.Fatalf("ParseReportsDir: %v", err)
	}

	narrative := SnapshotNarrative{
		Headline:          "Volume surged.",
		SecondaryHeadline: "External sources dominate.",
		Snapshot:          []string{"Point one"},
		WhatThisMeans:     []string{"Point two"},
		Opportunities:     []string{"Point three"},
		BottomLine:        "Point four",
	}
	html, err := BuildSnapshotHTML(model, narrative, SnapshotOptions{
		GeneratedAt: time.Date(2026, time.March, 27, 15, 30, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("BuildSnapshotHTML: %v", err)
	}
	body := string(html)
	if !strings.Contains(body, "<h1 class=\"title\">Inbox Snapshot</h1>") {
		t.Fatalf("expected default title, got %s", body)
	}
	if !strings.Contains(body, "Unscoped reports") {
		t.Fatalf("expected unscoped subtitle, got %s", body)
	}
}

func TestBuildSnapshotHTML_RequiresModel(t *testing.T) {
	_, err := BuildSnapshotHTML(nil, SnapshotNarrative{
		Headline:          "a",
		SecondaryHeadline: "b",
		Snapshot:          []string{"c"},
		WhatThisMeans:     []string{"d"},
		Opportunities:     []string{"e"},
		BottomLine:        "f",
	}, SnapshotOptions{})
	if err == nil {
		t.Fatal("expected nil model error")
	}
	if !strings.Contains(err.Error(), "model is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBuildSnapshotPDF_RendererUnavailable(t *testing.T) {
	model, err := ParseReportsDir(Options{
		ReportsDir: "testdata/valid",
		OwnerEmail: "owner@company.com",
	})
	if err != nil {
		t.Fatalf("ParseReportsDir: %v", err)
	}

	narrative, err := ParseSnapshotNarrative([]byte(snapshotMarkdown))
	if err != nil {
		t.Fatalf("ParseSnapshotNarrative: %v", err)
	}

	_, err = BuildSnapshotPDF(model, narrative, SnapshotOptions{}, nil)
	if err == nil {
		t.Fatal("expected pdf unavailable error")
	}
	if !errors.Is(err, ErrPDFRendererUnavailable) {
		t.Fatalf("expected ErrPDFRendererUnavailable, got %v", err)
	}
}

func TestBuildSnapshotPDF_RendererFailureWrapped(t *testing.T) {
	model, err := ParseReportsDir(Options{
		ReportsDir: "testdata/valid",
		OwnerEmail: "owner@company.com",
	})
	if err != nil {
		t.Fatalf("ParseReportsDir: %v", err)
	}

	narrative, err := ParseSnapshotNarrative([]byte(snapshotMarkdown))
	if err != nil {
		t.Fatalf("ParseSnapshotNarrative: %v", err)
	}

	_, err = BuildSnapshotPDF(model, narrative, SnapshotOptions{}, &stubPDFRenderer{
		err: errors.New("wkhtmltopdf missing"),
	})
	if err == nil {
		t.Fatal("expected wrapped renderer failure")
	}
	if !errors.Is(err, ErrPDFRendererUnavailable) {
		t.Fatalf("expected ErrPDFRendererUnavailable, got %v", err)
	}
	if !strings.Contains(err.Error(), "wkhtmltopdf missing") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBuildSnapshotPDF_RendererCalled(t *testing.T) {
	model, err := ParseReportsDir(Options{
		ReportsDir: "testdata/valid",
		OwnerEmail: "owner@company.com",
	})
	if err != nil {
		t.Fatalf("ParseReportsDir: %v", err)
	}

	narrative, err := ParseSnapshotNarrative([]byte(snapshotMarkdown))
	if err != nil {
		t.Fatalf("ParseSnapshotNarrative: %v", err)
	}

	renderer := &stubPDFRenderer{returnBytes: []byte("%PDF-1.7")}
	got, err := BuildSnapshotPDF(model, narrative, SnapshotOptions{
		GeneratedAt: time.Date(2026, time.March, 27, 15, 30, 0, 0, time.UTC),
	}, renderer)
	if err != nil {
		t.Fatalf("BuildSnapshotPDF: %v", err)
	}
	if string(got) != "%PDF-1.7" {
		t.Fatalf("unexpected pdf bytes: %q", got)
	}
	if !strings.Contains(string(renderer.html), "<h2>Bottom Line</h2>") {
		t.Fatalf("renderer did not receive rendered html: %s", renderer.html)
	}
}

type stubPDFRenderer struct {
	html        []byte
	returnBytes []byte
	err         error
}

func (s *stubPDFRenderer) RenderPDF(html []byte) ([]byte, error) {
	s.html = append([]byte(nil), html...)
	if s.err != nil {
		return nil, s.err
	}
	return append([]byte(nil), s.returnBytes...), nil
}

const snapshotMarkdown = `# Inbox Snapshot
owner@company.com

## Key Takeaway
Email volume increased from 5 to 8 messages across the reporting window.

## Secondary Takeaway
Most inbound activity comes from a small number of automated external sources.

## Snapshot
- Top external sender alerts@vendor.com generated the most messages.
- vendor.com is the most active external domain in the sample period.
- Invoice themes recur more often than update themes.

## What This Means
- Important messages are competing with repeat notifications for attention.
- A small number of external sources shape most daily inbox activity.

## Opportunities to Improve
- Prioritize the most important external senders.
- Batch or suppress lower-value notifications.
- Promote repeated business themes into workflow rules.

## Bottom Line
This inbox already shows enough pattern concentration to support a first export-driven automation review.
`
