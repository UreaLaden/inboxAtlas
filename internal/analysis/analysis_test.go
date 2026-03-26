package analysis

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/UreaLaden/inboxatlas/internal/storage"
	"github.com/UreaLaden/inboxatlas/pkg/models"
)

// --- helpers ---

func newTestStore(t *testing.T) *storage.Store {
	t.Helper()
	st, err := storage.Open(":memory:")
	if err != nil {
		t.Fatalf("open :memory: store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func seedMailbox(t *testing.T, st *storage.Store, id string) {
	t.Helper()
	if err := st.CreateMailbox(context.Background(), models.Mailbox{ID: id, Provider: "gmail"}); err != nil {
		t.Fatalf("CreateMailbox(%q): %v", id, err)
	}
}

func seedMsg(t *testing.T, st *storage.Store, providerID, mailboxID, fromEmail, fromName, domain, subject string, receivedAt time.Time) {
	t.Helper()
	msg := models.MessageMeta{
		ProviderID: providerID,
		MailboxID:  mailboxID,
		Provider:   "gmail",
		FromEmail:  fromEmail,
		FromName:   fromName,
		Domain:     domain,
		Subject:    subject,
		ReceivedAt: receivedAt,
	}
	if err := st.UpsertMessage(context.Background(), msg); err != nil {
		t.Fatalf("UpsertMessage(%q): %v", providerID, err)
	}
}

// --- TokenizeSubjects ---

func TestTokenizeSubjects_Basic(t *testing.T) {
	terms := TokenizeSubjects([]string{"Hello World", "Hello Go"}, 10)
	freq := make(map[string]int)
	for _, st := range terms {
		freq[st.Term] = st.Count
	}
	if freq["hello"] != 2 {
		t.Errorf("expected hello=2, got %d", freq["hello"])
	}
	if freq["world"] != 1 {
		t.Errorf("expected world=1, got %d", freq["world"])
	}
	if freq["go"] != 1 {
		t.Errorf("expected go=1, got %d", freq["go"])
	}
}

func TestTokenizeSubjects_StopWordsFiltered(t *testing.T) {
	subjects := []string{"re: the meeting", "fwd: an update for you", "re is in and for the or"}
	terms := TokenizeSubjects(subjects, 50)
	stopList := []string{"re", "the", "fwd", "an", "for", "is", "in", "and", "or"}
	found := make(map[string]bool)
	for _, st := range terms {
		found[st.Term] = true
	}
	for _, stop := range stopList {
		if found[stop] {
			t.Errorf("stop word %q should have been filtered", stop)
		}
	}
}

func TestTokenizeSubjects_Lowercase(t *testing.T) {
	terms := TokenizeSubjects([]string{"HELLO WORLD"}, 10)
	for _, st := range terms {
		if st.Term != strings.ToLower(st.Term) {
			t.Errorf("expected lowercase term, got %q", st.Term)
		}
	}
}

func TestTokenizeSubjects_PunctuationStripped(t *testing.T) {
	terms := TokenizeSubjects([]string{"hello, world! how-are (you)?"}, 10)
	freq := make(map[string]int)
	for _, st := range terms {
		freq[st.Term] = st.Count
	}
	if freq["hello"] != 1 {
		t.Errorf("expected hello=1, got %v", freq)
	}
	if freq["world"] != 1 {
		t.Errorf("expected world=1")
	}
}

func TestTokenizeSubjects_ShortTokensDiscarded(t *testing.T) {
	terms := TokenizeSubjects([]string{"a I x yo hello"}, 10)
	for _, st := range terms {
		if len(st.Term) < 2 {
			t.Errorf("token %q shorter than 2 chars should be discarded", st.Term)
		}
	}
}

func TestTokenizeSubjects_LimitEnforced(t *testing.T) {
	subjects := []string{"alpha beta gamma delta epsilon zeta eta"}
	terms := TokenizeSubjects(subjects, 3)
	if len(terms) > 3 {
		t.Errorf("expected at most 3 terms, got %d", len(terms))
	}
}

func TestTokenizeSubjects_Empty(t *testing.T) {
	terms := TokenizeSubjects(nil, 10)
	if len(terms) != 0 {
		t.Errorf("expected empty result for nil input, got %v", terms)
	}
	terms = TokenizeSubjects([]string{}, 10)
	if len(terms) != 0 {
		t.Errorf("expected empty result for empty slice, got %v", terms)
	}
}

func TestTokenizeSubjects_SortedByCountDesc(t *testing.T) {
	subjects := []string{"foo bar foo baz foo baz"}
	terms := TokenizeSubjects(subjects, 10)
	if len(terms) < 2 {
		t.Fatal("expected at least 2 terms")
	}
	for i := 1; i < len(terms); i++ {
		if terms[i].Count > terms[i-1].Count {
			t.Errorf("terms not sorted desc: %v > %v", terms[i], terms[i-1])
		}
	}
}

func TestTokenizeSubjects_TieBreakAlpha(t *testing.T) {
	// "apple" and "zebra" both appear once — alpha tie-break should put apple first
	terms := TokenizeSubjects([]string{"zebra apple"}, 10)
	if len(terms) < 2 {
		t.Fatal("expected 2 terms")
	}
	if terms[0].Term != "apple" {
		t.Errorf("expected apple first (tie-break), got %q", terms[0].Term)
	}
}

// --- RenderDomains ---

func TestRenderDomains_Table(t *testing.T) {
	rows := []DomainRow{{Domain: "foo.com", Count: 5}, {Domain: "bar.com", Count: 2}}
	var buf bytes.Buffer
	if err := RenderDomains(&buf, rows, FormatTable); err != nil {
		t.Fatalf("RenderDomains table: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "DOMAIN") || !strings.Contains(out, "COUNT") {
		t.Errorf("expected header in table output: %q", out)
	}
	if !strings.Contains(out, "foo.com") {
		t.Errorf("expected foo.com in table output")
	}
}

func TestRenderDomains_CSV(t *testing.T) {
	rows := []DomainRow{{Domain: "foo.com", Count: 3}}
	var buf bytes.Buffer
	if err := RenderDomains(&buf, rows, FormatCSV); err != nil {
		t.Fatalf("RenderDomains csv: %v", err)
	}
	records, err := csv.NewReader(&buf).ReadAll()
	if err != nil {
		t.Fatalf("parse CSV: %v", err)
	}
	if len(records) < 2 {
		t.Fatal("expected header + 1 data row")
	}
	if records[0][0] != "domain" {
		t.Errorf("expected 'domain' header, got %q", records[0][0])
	}
	if records[1][0] != "foo.com" {
		t.Errorf("expected foo.com in data row")
	}
}

func TestRenderDomains_JSON(t *testing.T) {
	rows := []DomainRow{{Domain: "foo.com", Count: 3}}
	var buf bytes.Buffer
	if err := RenderDomains(&buf, rows, FormatJSON); err != nil {
		t.Fatalf("RenderDomains json: %v", err)
	}
	var parsed []DomainRow
	if err := json.Unmarshal(buf.Bytes(), &parsed); err != nil {
		t.Fatalf("parse JSON: %v", err)
	}
	if len(parsed) != 1 || parsed[0].Domain != "foo.com" {
		t.Errorf("unexpected JSON result: %+v", parsed)
	}
}

func TestRenderDomains_UnknownFormat(t *testing.T) {
	if err := RenderDomains(&bytes.Buffer{}, nil, Format("xml")); err == nil {
		t.Error("expected error for unknown format")
	}
}

// --- RenderSenders ---

func TestRenderSenders_Table(t *testing.T) {
	rows := []SenderRow{{Email: "alice@foo.com", Name: "Alice", Domain: "foo.com", Count: 4}}
	var buf bytes.Buffer
	if err := RenderSenders(&buf, rows, FormatTable); err != nil {
		t.Fatalf("RenderSenders table: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "EMAIL") || !strings.Contains(out, "NAME") {
		t.Errorf("expected header in table output: %q", out)
	}
	if !strings.Contains(out, "alice@foo.com") {
		t.Errorf("expected alice@foo.com in table output")
	}
}

func TestRenderSenders_CSV(t *testing.T) {
	rows := []SenderRow{{Email: "alice@foo.com", Name: "Alice", Domain: "foo.com", Count: 4}}
	var buf bytes.Buffer
	if err := RenderSenders(&buf, rows, FormatCSV); err != nil {
		t.Fatalf("RenderSenders csv: %v", err)
	}
	records, err := csv.NewReader(&buf).ReadAll()
	if err != nil {
		t.Fatalf("parse CSV: %v", err)
	}
	if len(records) < 2 {
		t.Fatal("expected header + 1 data row")
	}
	if records[0][0] != "email" {
		t.Errorf("expected 'email' header, got %q", records[0][0])
	}
}

func TestRenderSenders_JSON(t *testing.T) {
	rows := []SenderRow{{Email: "alice@foo.com", Name: "Alice", Domain: "foo.com", Count: 4}}
	var buf bytes.Buffer
	if err := RenderSenders(&buf, rows, FormatJSON); err != nil {
		t.Fatalf("RenderSenders json: %v", err)
	}
	var parsed []SenderRow
	if err := json.Unmarshal(buf.Bytes(), &parsed); err != nil {
		t.Fatalf("parse JSON: %v", err)
	}
	if len(parsed) != 1 || parsed[0].Email != "alice@foo.com" {
		t.Errorf("unexpected JSON result: %+v", parsed)
	}
}

func TestRenderSenders_UnknownFormat(t *testing.T) {
	if err := RenderSenders(&bytes.Buffer{}, nil, Format("xml")); err == nil {
		t.Error("expected error for unknown format")
	}
}

// --- RenderSubjectTerms ---

func TestRenderSubjectTerms_Table(t *testing.T) {
	rows := []SubjectTerm{{Term: "meeting", Count: 7}, {Term: "update", Count: 3}}
	var buf bytes.Buffer
	if err := RenderSubjectTerms(&buf, rows, FormatTable); err != nil {
		t.Fatalf("RenderSubjectTerms table: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "TERM") || !strings.Contains(out, "COUNT") {
		t.Errorf("expected header in table output: %q", out)
	}
	if !strings.Contains(out, "meeting") {
		t.Errorf("expected 'meeting' in table output")
	}
}

func TestRenderSubjectTerms_CSV(t *testing.T) {
	rows := []SubjectTerm{{Term: "meeting", Count: 7}}
	var buf bytes.Buffer
	if err := RenderSubjectTerms(&buf, rows, FormatCSV); err != nil {
		t.Fatalf("RenderSubjectTerms csv: %v", err)
	}
	records, err := csv.NewReader(&buf).ReadAll()
	if err != nil {
		t.Fatalf("parse CSV: %v", err)
	}
	if len(records) < 2 {
		t.Fatal("expected header + 1 data row")
	}
	if records[0][0] != "term" {
		t.Errorf("expected 'term' header, got %q", records[0][0])
	}
}

func TestRenderSubjectTerms_JSON(t *testing.T) {
	rows := []SubjectTerm{{Term: "meeting", Count: 7}}
	var buf bytes.Buffer
	if err := RenderSubjectTerms(&buf, rows, FormatJSON); err != nil {
		t.Fatalf("RenderSubjectTerms json: %v", err)
	}
	var parsed []SubjectTerm
	if err := json.Unmarshal(buf.Bytes(), &parsed); err != nil {
		t.Fatalf("parse JSON: %v", err)
	}
	if len(parsed) != 1 || parsed[0].Term != "meeting" {
		t.Errorf("unexpected JSON result: %+v", parsed)
	}
}

func TestRenderSubjectTerms_UnknownFormat(t *testing.T) {
	if err := RenderSubjectTerms(&bytes.Buffer{}, nil, Format("xml")); err == nil {
		t.Error("expected error for unknown format")
	}
}

// --- RenderVolume ---

func TestRenderVolume_Table(t *testing.T) {
	rows := []VolumeRow{{Period: "2025-01", Count: 10}, {Period: "2025-02", Count: 5}}
	var buf bytes.Buffer
	if err := RenderVolume(&buf, rows, FormatTable); err != nil {
		t.Fatalf("RenderVolume table: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "PERIOD") || !strings.Contains(out, "COUNT") {
		t.Errorf("expected header in table output: %q", out)
	}
	if !strings.Contains(out, "2025-01") {
		t.Errorf("expected 2025-01 in table output")
	}
}

func TestRenderVolume_CSV(t *testing.T) {
	rows := []VolumeRow{{Period: "2025-01", Count: 10}}
	var buf bytes.Buffer
	if err := RenderVolume(&buf, rows, FormatCSV); err != nil {
		t.Fatalf("RenderVolume csv: %v", err)
	}
	records, err := csv.NewReader(&buf).ReadAll()
	if err != nil {
		t.Fatalf("parse CSV: %v", err)
	}
	if len(records) < 2 {
		t.Fatal("expected header + 1 data row")
	}
	if records[0][0] != "period" {
		t.Errorf("expected 'period' header, got %q", records[0][0])
	}
}

func TestRenderVolume_JSON(t *testing.T) {
	rows := []VolumeRow{{Period: "2025-01", Count: 10}}
	var buf bytes.Buffer
	if err := RenderVolume(&buf, rows, FormatJSON); err != nil {
		t.Fatalf("RenderVolume json: %v", err)
	}
	var parsed []VolumeRow
	if err := json.Unmarshal(buf.Bytes(), &parsed); err != nil {
		t.Fatalf("parse JSON: %v", err)
	}
	if len(parsed) != 1 || parsed[0].Period != "2025-01" {
		t.Errorf("unexpected JSON result: %+v", parsed)
	}
}

func TestRenderVolume_UnknownFormat(t *testing.T) {
	if err := RenderVolume(&bytes.Buffer{}, nil, Format("xml")); err == nil {
		t.Error("expected error for unknown format")
	}
}

// --- Query wrappers integration tests ---

func TestQueryDomains_Integration(t *testing.T) {
	st := newTestStore(t)
	seedMailbox(t, st, "user@example.com")

	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	seedMsg(t, st, "m1", "user@example.com", "a@foo.com", "", "foo.com", "", now)
	seedMsg(t, st, "m2", "user@example.com", "b@foo.com", "", "foo.com", "", now.Add(time.Hour))
	seedMsg(t, st, "m3", "user@example.com", "c@bar.com", "", "bar.com", "", now)

	rows, err := QueryDomains(context.Background(), st, "user@example.com", 10)
	if err != nil {
		t.Fatalf("QueryDomains: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0].Domain != "foo.com" || rows[0].Count != 2 {
		t.Errorf("first row: got %+v", rows[0])
	}
}

func TestQuerySenders_Integration(t *testing.T) {
	st := newTestStore(t)
	seedMailbox(t, st, "user@example.com")

	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	seedMsg(t, st, "m1", "user@example.com", "alice@foo.com", "Alice", "foo.com", "", now)
	seedMsg(t, st, "m2", "user@example.com", "alice@foo.com", "Alice", "foo.com", "", now.Add(time.Hour))

	rows, err := QuerySenders(context.Background(), st, "user@example.com", 10)
	if err != nil {
		t.Fatalf("QuerySenders: %v", err)
	}
	if len(rows) != 1 || rows[0].Count != 2 {
		t.Errorf("expected 1 row with count=2, got %+v", rows)
	}
	if rows[0].Name != "Alice" {
		t.Errorf("expected Name=Alice, got %q", rows[0].Name)
	}
}

func TestQuerySubjectTerms_Integration(t *testing.T) {
	st := newTestStore(t)
	seedMailbox(t, st, "user@example.com")

	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	seedMsg(t, st, "m1", "user@example.com", "a@x.com", "", "x.com", "Weekly meeting update", now)
	seedMsg(t, st, "m2", "user@example.com", "b@x.com", "", "x.com", "Meeting notes", now.Add(time.Hour))

	terms, err := QuerySubjectTerms(context.Background(), st, "user@example.com", 10)
	if err != nil {
		t.Fatalf("QuerySubjectTerms: %v", err)
	}
	freq := make(map[string]int)
	for _, st := range terms {
		freq[st.Term] = st.Count
	}
	if freq["meeting"] != 2 {
		t.Errorf("expected meeting=2, got %d", freq["meeting"])
	}
}

func TestQueryVolume_Integration(t *testing.T) {
	st := newTestStore(t)
	seedMailbox(t, st, "user@example.com")

	seedMsg(t, st, "m1", "user@example.com", "a@x.com", "", "x.com", "", time.Date(2025, 1, 5, 0, 0, 0, 0, time.UTC))
	seedMsg(t, st, "m2", "user@example.com", "b@x.com", "", "x.com", "", time.Date(2025, 2, 3, 0, 0, 0, 0, time.UTC))

	rows, err := QueryVolume(context.Background(), st, "user@example.com")
	if err != nil {
		t.Fatalf("QueryVolume: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0].Period != "2025-01" {
		t.Errorf("expected first period=2025-01, got %q", rows[0].Period)
	}
	if rows[1].Period != "2025-02" {
		t.Errorf("expected second period=2025-02, got %q", rows[1].Period)
	}
}
