package export

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBuildSummaryInput_FromModel(t *testing.T) {
	model, err := ParseReportsDir(Options{
		ReportsDir: "testdata/valid",
		OwnerEmail: "owner@company.com",
	})
	if err != nil {
		t.Fatalf("ParseReportsDir: %v", err)
	}

	input, err := BuildSummaryInput(model)
	if err != nil {
		t.Fatalf("BuildSummaryInput: %v", err)
	}

	if input.TotalMessages != 20 {
		t.Fatalf("TotalMessages: got %d, want 20", input.TotalMessages)
	}
	if input.ReportingPeriod.Label != "2025-01 to 2025-03" {
		t.Fatalf("ReportingPeriod.Label: got %q", input.ReportingPeriod.Label)
	}
	if len(input.TopExternalSenders) != 2 || input.TopExternalSenders[0].Email != "alerts@vendor.com" {
		t.Fatalf("unexpected TopExternalSenders: %+v", input.TopExternalSenders)
	}
	if len(input.TopExternalDomains) != 2 || input.TopExternalDomains[0].Domain != "vendor.com" {
		t.Fatalf("unexpected TopExternalDomains: %+v", input.TopExternalDomains)
	}
	if len(input.TopSubjectThemes) != 2 || input.TopSubjectThemes[0].Term != "invoice" {
		t.Fatalf("unexpected TopSubjectThemes: %+v", input.TopSubjectThemes)
	}
	if input.VolumeHighlights.FirstCount != 5 || input.VolumeHighlights.LastCount != 8 {
		t.Fatalf("unexpected volume counts: %+v", input.VolumeHighlights)
	}
	if input.VolumeHighlights.AbsoluteChange != 3 || input.VolumeHighlights.PeakPeriod != "2025-03" {
		t.Fatalf("unexpected volume highlights: %+v", input.VolumeHighlights)
	}
	if input.DerivedMetrics.TopExternalSenderCount != 9 || input.DerivedMetrics.TopExternalDomainCount != 9 {
		t.Fatalf("unexpected derived metrics: %+v", input.DerivedMetrics)
	}
}

func TestBuildSummaryInput_RequiresModel(t *testing.T) {
	_, err := BuildSummaryInput(nil)
	if !errors.Is(err, ErrSummaryInputRequired) {
		t.Fatalf("expected ErrSummaryInputRequired, got %v", err)
	}
}

func TestParseSummaryOutputJSON_StrictSchema(t *testing.T) {
	_, err := ParseSummaryOutputJSON([]byte(`{
  "headline": "a",
  "secondary_headline": "b",
  "snapshot_bullets": ["c"],
  "what_this_means_bullets": ["d"],
  "opportunities_bullets": ["e"],
  "bottom_line": "f",
  "extra": true
}`))
	if err == nil {
		t.Fatal("expected schema error")
	}
	if !errors.Is(err, ErrSummaryOutputInvalid) {
		t.Fatalf("expected ErrSummaryOutputInvalid, got %v", err)
	}
	if !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAdaptSummaryOutput_ValidStructuredOutputToNarrativeAndMarkdown(t *testing.T) {
	model, err := ParseReportsDir(Options{
		ReportsDir: "testdata/valid",
		OwnerEmail: "owner@company.com",
	})
	if err != nil {
		t.Fatalf("ParseReportsDir: %v", err)
	}
	input, err := BuildSummaryInput(model)
	if err != nil {
		t.Fatalf("BuildSummaryInput: %v", err)
	}

	output, err := ParseSummaryOutputJSON([]byte(`{
  "title": "Inbox Snapshot",
  "subtitle": "owner@company.com",
  "headline": "External volume rose from 5 to 8 messages across 2025-01 to 2025-03.",
  "secondary_headline": "alerts@vendor.com remained the top external sender with 9 messages.",
  "snapshot_bullets": [
    "vendor.com led external domain volume with 9 messages.",
    "invoice remained the top subject theme with 5 messages."
  ],
  "what_this_means_bullets": [
    "A small number of external sources drive a large share of the 20 total messages."
  ],
  "opportunities_bullets": [
    "Review repeat notifications from alerts@vendor.com for rule-based handling."
  ],
  "bottom_line": "The inbox shows stable, measurable patterns that are ready for structured automation review."
}`))
	if err != nil {
		t.Fatalf("ParseSummaryOutputJSON: %v", err)
	}

	narrative, err := AdaptSummaryOutput(input, output)
	if err != nil {
		t.Fatalf("AdaptSummaryOutput: %v", err)
	}
	if narrative.Headline != output.Headline || len(narrative.Snapshot) != 2 {
		t.Fatalf("unexpected narrative: %+v", narrative)
	}

	markdown, err := FormatSnapshotNarrativeMarkdown(narrative)
	if err != nil {
		t.Fatalf("FormatSnapshotNarrativeMarkdown: %v", err)
	}
	roundTrip, err := ParseSnapshotNarrative(markdown)
	if err != nil {
		t.Fatalf("ParseSnapshotNarrative: %v", err)
	}
	if roundTrip.BottomLine != narrative.BottomLine {
		t.Fatalf("round-trip bottom line: got %q want %q", roundTrip.BottomLine, narrative.BottomLine)
	}
}

func TestValidateSummaryOutput_RejectsUnsupportedNumericFact(t *testing.T) {
	input := validSummaryInput(t)
	err := ValidateSummaryOutput(input, SummaryOutput{
		Headline:             "Volume jumped to 999 messages.",
		SecondaryHeadline:    "External sources still dominate.",
		SnapshotBullets:      []string{"alerts@vendor.com remains prominent."},
		WhatThisMeansBullets: []string{"Prioritization is still needed."},
		OpportunitiesBullets: []string{"Create rules for repeat notifications."},
		BottomLine:           "Automation opportunities remain strong.",
	})
	if err == nil {
		t.Fatal("expected numeric fact validation error")
	}
	if !strings.Contains(err.Error(), "unsupported numeric fact") {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(err.Error(), `Volume jumped to 999 messages.`) {
		t.Fatalf("expected rejected text context, got %v", err)
	}
}

func TestValidateSummaryOutput_RejectsDerivedDurationFact(t *testing.T) {
	input := validSummaryInput(t)
	err := ValidateSummaryOutput(input, SummaryOutput{
		Headline:             "Significant increase in email activity over the last 10 months.",
		SecondaryHeadline:    "alerts@vendor.com remained the top external sender with 9 messages.",
		SnapshotBullets:      []string{"vendor.com led external domain volume with 9 messages."},
		WhatThisMeansBullets: []string{"A small number of external sources drive a large share of the 20 total messages."},
		OpportunitiesBullets: []string{"Review repeat notifications from alerts@vendor.com for rule-based handling."},
		BottomLine:           "The inbox shows stable patterns that are ready for structured automation review.",
	})
	if err == nil {
		t.Fatal("expected derived duration validation error")
	}
	if !strings.Contains(err.Error(), `unsupported numeric fact "10"`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateSummaryOutput_RejectsApproximatePercentageRestatement(t *testing.T) {
	input := validSummaryInput(t)
	err := ValidateSummaryOutput(input, SummaryOutput{
		Headline:             "Total messages increased by over 100% from 2025-01 to 2025-03.",
		SecondaryHeadline:    "alerts@vendor.com remained the top external sender with 9 messages.",
		SnapshotBullets:      []string{"vendor.com led external domain volume with 9 messages."},
		WhatThisMeansBullets: []string{"A small number of external sources drive a large share of the 20 total messages."},
		OpportunitiesBullets: []string{"Review repeat notifications from alerts@vendor.com for rule-based handling."},
		BottomLine:           "The inbox shows stable patterns that are ready for structured automation review.",
	})
	if err == nil {
		t.Fatal("expected approximate percentage validation error")
	}
	if !strings.Contains(err.Error(), `unsupported numeric fact "100"`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateSummaryOutput_AcceptsCommaSeparatedCounts(t *testing.T) {
	input := validLargeSummaryInput(t)
	err := ValidateSummaryOutput(input, SummaryOutput{
		Headline:             "Total messages reached 30,075.",
		SecondaryHeadline:    "groupupdates@facebookmail.com remained the top external sender with 1,726 messages.",
		SnapshotBullets:      []string{"healthymd.com led external domain volume with 1,927 messages."},
		WhatThisMeansBullets: []string{"Email volume increased from 1,189 in 2025-05 to 2,458 in 2026-03."},
		OpportunitiesBullets: []string{"Review repeated traffic driving the 1,269-message absolute change."},
		BottomLine:           "The inbox shows repeatable patterns across 30,075 messages.",
	})
	if err != nil {
		t.Fatalf("expected comma-separated counts to validate, got %v", err)
	}
}

func TestValidateSummaryOutput_AcceptsEquivalentPercentFormatting(t *testing.T) {
	input := validSummaryInput(t)
	err := ValidateSummaryOutput(input, SummaryOutput{
		Headline:             "Volume rose by 60.00% from 2025-01 to 2025-03.",
		SecondaryHeadline:    "alerts@vendor.com remained the top external sender with 9 messages.",
		SnapshotBullets:      []string{"vendor.com led external domain volume with 9 messages."},
		WhatThisMeansBullets: []string{"invoice represented 25.00% of the total messages."},
		OpportunitiesBullets: []string{"Review repeat notifications from alerts@vendor.com for rule-based handling."},
		BottomLine:           "The inbox shows stable patterns that are ready for structured automation review.",
	})
	if err != nil {
		t.Fatalf("expected equivalent percent formatting to validate, got %v", err)
	}
}

func TestValidateSummaryOutput_RejectsDifferentNormalizedNumericValue(t *testing.T) {
	input := validLargeSummaryInput(t)
	err := ValidateSummaryOutput(input, SummaryOutput{
		Headline:             "Total messages reached 30,076.",
		SecondaryHeadline:    "groupupdates@facebookmail.com remained the top external sender with 1,726 messages.",
		SnapshotBullets:      []string{"healthymd.com led external domain volume with 1,927 messages."},
		WhatThisMeansBullets: []string{"Email volume increased from 1,189 in 2025-05 to 2,458 in 2026-03."},
		OpportunitiesBullets: []string{"Review repeated traffic driving the 1,269-message absolute change."},
		BottomLine:           "The inbox shows repeatable patterns across 30,075 messages.",
	})
	if err == nil {
		t.Fatal("expected different numeric value to fail validation")
	}
	if !strings.Contains(err.Error(), `unsupported numeric fact "30,076"`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateSummaryOutput_RejectsUnsupportedDomainReference(t *testing.T) {
	input := validSummaryInput(t)
	err := ValidateSummaryOutput(input, SummaryOutput{
		Headline:             "Volume rose from 5 to 8 messages.",
		SecondaryHeadline:    "External sources still dominate.",
		SnapshotBullets:      []string{"unknown.example.com became the most important source."},
		WhatThisMeansBullets: []string{"Prioritization is still needed."},
		OpportunitiesBullets: []string{"Create rules for repeat notifications."},
		BottomLine:           "Automation opportunities remain strong.",
	})
	if err == nil {
		t.Fatal("expected domain validation error")
	}
	if !strings.Contains(err.Error(), "unsupported domain reference") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateSummaryOutput_RejectsEmptyBulletArrays(t *testing.T) {
	input := validSummaryInput(t)
	err := ValidateSummaryOutput(input, SummaryOutput{
		Headline:             "Volume rose from 5 to 8 messages.",
		SecondaryHeadline:    "External sources still dominate.",
		WhatThisMeansBullets: []string{"Prioritization is still needed."},
		OpportunitiesBullets: []string{"Create rules for repeat notifications."},
		BottomLine:           "Automation opportunities remain strong.",
	})
	if err == nil {
		t.Fatal("expected bullet validation error")
	}
	if !strings.Contains(err.Error(), "snapshot_bullets") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCommandSummaryProvider_GenerateSummary(t *testing.T) {
	script := filepath.Join(t.TempDir(), "provider.sh")
	writeExecutable(t, script, `#!/bin/sh
cat >/dev/null
cat <<'EOF'
{"title":"Inbox Snapshot","subtitle":"owner@company.com","headline":"Volume rose from 5 to 8 messages across 2025-01 to 2025-03.","secondary_headline":"alerts@vendor.com remained the top external sender with 9 messages.","snapshot_bullets":["vendor.com led external domain volume with 9 messages."],"what_this_means_bullets":["A small number of external sources drive a large share of the 20 total messages."],"opportunities_bullets":["Review repeat notifications from alerts@vendor.com for rule-based handling."],"bottom_line":"The inbox shows stable, measurable patterns that are ready for structured automation review."}
EOF
`)

	provider := CommandSummaryProvider{Command: script}
	output, err := provider.GenerateSummary(t.Context(), "prompt", validSummaryInput(t))
	if err != nil {
		t.Fatalf("GenerateSummary: %v", err)
	}
	if output.Headline == "" || len(output.SnapshotBullets) != 1 {
		t.Fatalf("unexpected output: %+v", output)
	}
}

func TestCommandSummaryProvider_InvalidJSON(t *testing.T) {
	script := filepath.Join(t.TempDir(), "provider.sh")
	writeExecutable(t, script, `#!/bin/sh
cat >/dev/null
printf 'not-json'
`)

	provider := CommandSummaryProvider{Command: script}
	_, err := provider.GenerateSummary(t.Context(), "prompt", validSummaryInput(t))
	if err == nil {
		t.Fatal("expected invalid json error")
	}
	if !errors.Is(err, ErrSummaryOutputInvalid) {
		t.Fatalf("expected ErrSummaryOutputInvalid, got %v", err)
	}
}

func TestCommandSummaryProvider_ExitErrorIncludesStderr(t *testing.T) {
	script := filepath.Join(t.TempDir(), "provider.sh")
	writeExecutable(t, script, `#!/bin/sh
cat >/dev/null
printf 'debug trace\n' >&2
exit 9
`)

	provider := CommandSummaryProvider{Command: script}
	_, err := provider.GenerateSummary(t.Context(), "prompt", validSummaryInput(t))
	if err == nil {
		t.Fatal("expected provider failure")
	}
	if !strings.Contains(err.Error(), "debug trace") {
		t.Fatalf("expected stderr in error, got %v", err)
	}
}

func validSummaryInput(t *testing.T) SummaryInput {
	t.Helper()
	model, err := ParseReportsDir(Options{
		ReportsDir: "testdata/valid",
		OwnerEmail: "owner@company.com",
	})
	if err != nil {
		t.Fatalf("ParseReportsDir: %v", err)
	}
	input, err := BuildSummaryInput(model)
	if err != nil {
		t.Fatalf("BuildSummaryInput: %v", err)
	}
	return input
}

func validLargeSummaryInput(t *testing.T) SummaryInput {
	t.Helper()
	model, err := ParseReportsDir(Options{
		ReportsDir: filepath.Join("..", "..", ".ai", "references", "reports", "out"),
		OwnerEmail: "acr@acrbookkeepingplus.com",
	})
	if err != nil {
		t.Fatalf("ParseReportsDir: %v", err)
	}
	input, err := BuildSummaryInput(model)
	if err != nil {
		t.Fatalf("BuildSummaryInput: %v", err)
	}
	return input
}

func writeExecutable(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o700); err != nil {
		t.Fatalf("WriteFile(%s): %v", path, err)
	}
}
