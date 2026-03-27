package export

import (
	"archive/zip"
	"bytes"
	"io"
	"strings"
	"testing"
	"time"
)

func TestBuildWorkbook_SheetStructureAndRepresentativeContent(t *testing.T) {
	model, err := ParseReportsDir(Options{
		ReportsDir: "testdata/valid",
		OwnerEmail: "owner@company.com",
	})
	if err != nil {
		t.Fatalf("ParseReportsDir: %v", err)
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

	files := unzipWorkbook(t, workbook)
	required := []string{
		"[Content_Types].xml",
		"_rels/.rels",
		"docProps/app.xml",
		"docProps/core.xml",
		"xl/workbook.xml",
		"xl/_rels/workbook.xml.rels",
		"xl/styles.xml",
		"xl/worksheets/sheet1.xml",
		"xl/worksheets/sheet2.xml",
		"xl/worksheets/sheet3.xml",
		"xl/worksheets/sheet4.xml",
		"xl/worksheets/sheet5.xml",
		"xl/worksheets/sheet6.xml",
	}
	for _, name := range required {
		if _, ok := files[name]; !ok {
			t.Fatalf("missing workbook part %s", name)
		}
	}

	workbookXML := files["xl/workbook.xml"]
	wantSheets := []string{
		`<sheet name="Overview" sheetId="1" r:id="rId1"/>`,
		`<sheet name="Volume Trends" sheetId="2" r:id="rId2"/>`,
		`<sheet name="Top Senders" sheetId="3" r:id="rId3"/>`,
		`<sheet name="Top Domains" sheetId="4" r:id="rId4"/>`,
		`<sheet name="Subject Themes" sheetId="5" r:id="rId5"/>`,
		`<sheet name="Source Data" sheetId="6" r:id="rId6"/>`,
	}
	for _, sheet := range wantSheets {
		if !strings.Contains(workbookXML, sheet) {
			t.Fatalf("workbook xml missing sheet entry %q", sheet)
		}
	}

	overview := files["xl/worksheets/sheet1.xml"]
	for _, want := range []string{
		"Inbox Snapshot",
		"owner@company.com",
		"2025-01 to 2025-03",
		"alerts@vendor.com",
		"vendor.com",
		"invoice",
	} {
		if !strings.Contains(overview, want) {
			t.Fatalf("overview missing %q", want)
		}
	}

	volume := files["xl/worksheets/sheet2.xml"]
	for _, want := range []string{
		`<pane ySplit="1" topLeftCell="A2" activePane="bottomLeft" state="frozen"/>`,
		`<autoFilter ref="A1:E4"/>`,
		`2025-02`,
		`<c r="C3" s="4"><v>2</v></c>`,
	} {
		if !strings.Contains(volume, want) {
			t.Fatalf("volume sheet missing %q", want)
		}
	}

	senders := files["xl/worksheets/sheet3.xml"]
	for _, want := range []string{
		`<autoFilter ref="A1:F4"/>`,
		"billing@client.org",
		"alerts@vendor.com",
		"internal",
		"external",
	} {
		if !strings.Contains(senders, want) {
			t.Fatalf("senders sheet missing %q", want)
		}
	}

	sourceData := files["xl/worksheets/sheet6.xml"]
	for _, want := range []string{
		`<autoFilter ref="A1:I12"/>`,
		"sender",
		"domain",
		"subject",
		"volume",
	} {
		if !strings.Contains(sourceData, want) {
			t.Fatalf("source data sheet missing %q", want)
		}
	}

	styles := files["xl/styles.xml"]
	for _, want := range []string{
		`formatCode="#,##0"`,
		`formatCode="0.0%"`,
		`<fgColor rgb="FFD9EAF7"/>`,
		`wrapText="1"`,
	} {
		if !strings.Contains(styles, want) {
			t.Fatalf("styles xml missing %q", want)
		}
	}
}

func TestBuildWorkbook_RequiresModel(t *testing.T) {
	_, err := BuildWorkbook(nil, WorkbookOptions{})
	if err == nil {
		t.Fatal("expected nil model error")
	}
	if !strings.Contains(err.Error(), "model is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func unzipWorkbook(t *testing.T, data []byte) map[string]string {
	t.Helper()

	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("zip.NewReader: %v", err)
	}

	files := make(map[string]string, len(zr.File))
	for _, file := range zr.File {
		rc, err := file.Open()
		if err != nil {
			t.Fatalf("open zip entry %s: %v", file.Name, err)
		}
		content, err := io.ReadAll(rc)
		_ = rc.Close()
		if err != nil {
			t.Fatalf("read zip entry %s: %v", file.Name, err)
		}
		files[file.Name] = string(content)
	}
	return files
}
