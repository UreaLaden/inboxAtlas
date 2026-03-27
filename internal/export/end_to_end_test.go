package export

import (
	"strings"
	"testing"
	"time"
)

func TestEndToEndFixture_WorkbookAndSnapshotHTML(t *testing.T) {
	model, err := ParseReportsDir(Options{
		ReportsDir: "testdata/valid",
		OwnerEmail: "owner@company.com",
	})
	if err != nil {
		t.Fatalf("ParseReportsDir: %v", err)
	}

	narrative, err := LoadSnapshotNarrative("testdata/valid/summary.md")
	if err != nil {
		t.Fatalf("LoadSnapshotNarrative: %v", err)
	}

	workbook, err := BuildWorkbook(model, WorkbookOptions{
		GeneratedAt: time.Date(2026, time.March, 27, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("BuildWorkbook: %v", err)
	}
	if len(workbook) == 0 {
		t.Fatal("expected workbook bytes")
	}

	html, err := BuildSnapshotHTML(model, narrative, SnapshotOptions{
		GeneratedAt: time.Date(2026, time.March, 27, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("BuildSnapshotHTML: %v", err)
	}

	files := unzipWorkbook(t, workbook)
	if !strings.Contains(files["xl/worksheets/sheet1.xml"], "Top External Sender") {
		t.Fatalf("workbook overview missing expected content: %s", files["xl/worksheets/sheet1.xml"])
	}
	if !strings.Contains(string(html), "Most inbound activity comes from a small number of automated external sources.") {
		t.Fatalf("snapshot html missing expected narrative content: %s", html)
	}
}

func TestEndToEndFixture_SnapshotNarrativeMismatchStaysExplicit(t *testing.T) {
	_, err := ParseSnapshotNarrative([]byte(`# Inbox Snapshot

## Key Takeaway
Only one section exists.
`))
	if err == nil {
		t.Fatal("expected explicit narrative validation error")
	}
	if !strings.Contains(err.Error(), "secondary takeaway") {
		t.Fatalf("unexpected error: %v", err)
	}
}
