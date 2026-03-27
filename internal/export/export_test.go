package export

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseReportsDir_ValidContractAndAggregation(t *testing.T) {
	model, err := ParseReportsDir(Options{
		ReportsDir: filepath.Join("testdata", "valid"),
		OwnerEmail: "owner@company.com",
	})
	if err != nil {
		t.Fatalf("ParseReportsDir: %v", err)
	}

	if model.Owner.Domain != "company.com" {
		t.Fatalf("Owner.Domain: got %q, want %q", model.Owner.Domain, "company.com")
	}
	if model.Summary.TotalMessages != 20 {
		t.Fatalf("TotalMessages: got %d, want %d", model.Summary.TotalMessages, 20)
	}
	if model.Summary.ReportingPeriodStart != "2025-01" || model.Summary.ReportingPeriodEnd != "2025-03" {
		t.Fatalf("unexpected reporting period: %+v", model.Summary)
	}
	if model.Summary.TopExternalSender != "alerts@vendor.com" {
		t.Fatalf("TopExternalSender: got %q, want %q", model.Summary.TopExternalSender, "alerts@vendor.com")
	}
	if model.Summary.TopExternalDomain != "vendor.com" {
		t.Fatalf("TopExternalDomain: got %q, want %q", model.Summary.TopExternalDomain, "vendor.com")
	}
	if len(model.Senders) != 3 || len(model.ExternalTopSenders) != 2 {
		t.Fatalf("unexpected sender counts: total=%d external=%d", len(model.Senders), len(model.ExternalTopSenders))
	}
	internalSender := findSender(model.Senders, "owner@company.com")
	if internalSender == nil || !internalSender.Internal {
		t.Fatalf("expected internal sender to be flagged, got %+v", model.Senders)
	}
	if len(model.Domains) != 3 || len(model.ExternalTopDomains) != 2 {
		t.Fatalf("unexpected domain counts: total=%d external=%d", len(model.Domains), len(model.ExternalTopDomains))
	}
	internalDomain := findDomain(model.Domains, "company.com")
	if internalDomain == nil || !internalDomain.Internal {
		t.Fatalf("expected owner domain to be flagged internal, got %+v", model.Domains)
	}
	if len(model.Subjects) != 2 || model.Subjects[0].Term != "invoice" {
		t.Fatalf("unexpected subjects: %+v", model.Subjects)
	}
	if len(model.Volume) != 3 {
		t.Fatalf("unexpected volume rows: %+v", model.Volume)
	}
	if model.Volume[1].MoMChange != 2 {
		t.Fatalf("Volume[1].MoMChange: got %d, want %d", model.Volume[1].MoMChange, 2)
	}
}

func TestParseReportsDir_HeaderOnlyFilesYieldEmptyModel(t *testing.T) {
	model, err := ParseReportsDir(Options{
		ReportsDir: filepath.Join("testdata", "empty"),
	})
	if err != nil {
		t.Fatalf("ParseReportsDir: %v", err)
	}
	if model.Summary.TotalMessages != 0 {
		t.Fatalf("TotalMessages: got %d, want 0", model.Summary.TotalMessages)
	}
	if len(model.Senders) != 0 || len(model.Domains) != 0 || len(model.Subjects) != 0 || len(model.Volume) != 0 {
		t.Fatalf("expected empty model, got %+v", model)
	}
}

func TestParseReportsDir_MissingRequiredFile(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, domainsFilename, "domain,count\nvendor.com,2\n")
	writeFile(t, dir, sendersFilename, "email,name,domain,count\nalerts@vendor.com,Alerts,vendor.com,2\n")
	writeFile(t, dir, subjectsFilename, "term,count\ninvoice,2\n")

	_, err := ParseReportsDir(Options{ReportsDir: dir})
	if err == nil {
		t.Fatal("expected missing file error")
	}
	if !strings.Contains(err.Error(), volumeFilename) {
		t.Fatalf("expected missing %s error, got %v", volumeFilename, err)
	}
}

func TestParseReportsDir_MalformedHeader(t *testing.T) {
	dir := t.TempDir()
	writeValidContract(t, dir)
	writeFile(t, dir, sendersFilename, "sender,name,domain,count\nalerts@vendor.com,Alerts,vendor.com,2\n")

	_, err := ParseReportsDir(Options{ReportsDir: dir})
	if err == nil {
		t.Fatal("expected malformed header error")
	}
	if !strings.Contains(err.Error(), "header") {
		t.Fatalf("expected header error, got %v", err)
	}
}

func TestParseReportsDir_MalformedCount(t *testing.T) {
	dir := t.TempDir()
	writeValidContract(t, dir)
	writeFile(t, dir, domainsFilename, "domain,count\nvendor.com,not-a-number\n")

	_, err := ParseReportsDir(Options{ReportsDir: dir})
	if err == nil {
		t.Fatal("expected malformed count error")
	}
	if !strings.Contains(err.Error(), "parse count") {
		t.Fatalf("expected parse count error, got %v", err)
	}
}

func TestParseReportsDir_InvalidPeriod(t *testing.T) {
	dir := t.TempDir()
	writeValidContract(t, dir)
	writeFile(t, dir, volumeFilename, "period,count\n2025/01,10\n")

	_, err := ParseReportsDir(Options{ReportsDir: dir})
	if err == nil {
		t.Fatal("expected invalid period error")
	}
	if !strings.Contains(err.Error(), "parse period") {
		t.Fatalf("expected parse period error, got %v", err)
	}
}

func TestParseReportsDir_RequiresDirectory(t *testing.T) {
	file := filepath.Join(t.TempDir(), "domains.csv")
	if err := os.WriteFile(file, []byte("domain,count\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := ParseReportsDir(Options{ReportsDir: file})
	if err == nil {
		t.Fatal("expected non-directory error")
	}
	if !strings.Contains(err.Error(), "not a directory") {
		t.Fatalf("expected directory error, got %v", err)
	}
}

func TestParseReportsDir_ReportsDirRequired(t *testing.T) {
	_, err := ParseReportsDir(Options{})
	if err == nil {
		t.Fatal("expected reports dir required error")
	}
	if !strings.Contains(err.Error(), "reports dir is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParseReportsDir_EmptyFile(t *testing.T) {
	dir := t.TempDir()
	writeValidContract(t, dir)
	writeFile(t, dir, domainsFilename, "")

	_, err := ParseReportsDir(Options{ReportsDir: dir})
	if err == nil {
		t.Fatal("expected empty file error")
	}
	if !strings.Contains(err.Error(), "is empty") {
		t.Fatalf("expected empty file error, got %v", err)
	}
}

func TestParseReportsDir_BlankRowsIgnored(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, domainsFilename, "domain,count\nvendor.com,9\n,\n")
	writeFile(t, dir, sendersFilename, "email,name,domain,count\nalerts@vendor.com,Alerts,vendor.com,9\n,,,\n")
	writeFile(t, dir, subjectsFilename, "term,count\ninvoice,5\n,\n")
	writeFile(t, dir, volumeFilename, "period,count\n2025-01,9\n,\n")

	model, err := ParseReportsDir(Options{ReportsDir: dir})
	if err != nil {
		t.Fatalf("ParseReportsDir: %v", err)
	}
	if len(model.Domains) != 1 || len(model.Senders) != 1 || len(model.Subjects) != 1 || len(model.Volume) != 1 {
		t.Fatalf("expected blank rows to be ignored, got %+v", model)
	}
}

func TestParseReportsDir_RowShapeError(t *testing.T) {
	dir := t.TempDir()
	writeValidContract(t, dir)
	writeFile(t, dir, subjectsFilename, "term,count\ninvoice,5,extra\n")

	_, err := ParseReportsDir(Options{ReportsDir: dir})
	if err == nil {
		t.Fatal("expected row shape error")
	}
	if !strings.Contains(err.Error(), "wrong number of fields") {
		t.Fatalf("expected malformed CSV row error, got %v", err)
	}
}

func TestNormalizeOwner_DerivesDomainFromEmail(t *testing.T) {
	owner := normalizeOwner("Owner@Company.com", "")
	if owner.Email != "owner@company.com" {
		t.Fatalf("Owner.Email: got %q", owner.Email)
	}
	if owner.Domain != "company.com" {
		t.Fatalf("Owner.Domain: got %q", owner.Domain)
	}
}

func TestPercent_ZeroTotal(t *testing.T) {
	if got := percent(5, 0); got != 0 {
		t.Fatalf("percent with zero total: got %v, want 0", got)
	}
}

func writeValidContract(t *testing.T, dir string) {
	t.Helper()
	writeFile(t, dir, domainsFilename, "domain,count\nvendor.com,9\nclient.org,3\ncompany.com,8\n")
	writeFile(t, dir, sendersFilename, "email,name,domain,count\nalerts@vendor.com,Alerts,vendor.com,9\nbilling@client.org,Billing,client.org,3\nowner@company.com,Owner,company.com,8\n")
	writeFile(t, dir, subjectsFilename, "term,count\ninvoice,5\nupdate,4\n")
	writeFile(t, dir, volumeFilename, "period,count\n2025-01,5\n2025-02,7\n2025-03,8\n")
}

func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}

func findSender(rows []SenderMetric, email string) *SenderMetric {
	for i := range rows {
		if rows[i].Email == email {
			return &rows[i]
		}
	}
	return nil
}

func findDomain(rows []DomainMetric, domain string) *DomainMetric {
	for i := range rows {
		if rows[i].Domain == domain {
			return &rows[i]
		}
	}
	return nil
}
