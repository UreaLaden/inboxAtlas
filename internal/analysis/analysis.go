// Package analysis computes domain, sender, subject, and volume insights from stored messages.
package analysis

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"unicode"

	"github.com/UreaLaden/inboxatlas/internal/storage"
)

// DomainRow is a single domain aggregate result.
type DomainRow struct {
	Domain string
	Count  int
}

// SenderRow is a single sender aggregate result.
type SenderRow struct {
	Email  string
	Name   string
	Domain string
	Count  int
}

// SubjectTerm is a single subject term frequency result.
type SubjectTerm struct {
	Term  string
	Count int
}

// VolumeRow is a single monthly volume result.
type VolumeRow struct {
	Period string // "YYYY-MM"
	Count  int
}

// Format controls how render functions write their output.
type Format string

const (
	// FormatTable renders output as a human-readable tabwriter table.
	FormatTable Format = "table"
	// FormatCSV renders output as comma-separated values.
	FormatCSV Format = "csv"
	// FormatJSON renders output as indented JSON.
	FormatJSON Format = "json"
)

// subjectNoiseTokens is the set of low-value subject tokens filtered out by
// TokenizeSubjects before theme extraction.
var subjectNoiseTokens = map[string]bool{
	"a": true, "an": true, "and": true, "as": true, "at": true, "attachment": true,
	"be": true, "by": true, "co": true, "corp": true, "external": true,
	"for": true, "from": true, "fwd": true, "fw": true, "has": true,
	"have": true, "image": true, "img": true, "in": true, "inc": true,
	"is": true, "join": true, "llc": true, "ltd": true, "md": true,
	"message": true, "new": true, "of": true, "on": true, "or": true,
	"pllc": true, "re": true, "the": true, "to": true, "with": true,
	"you": true, "your": true,
}

type themeCandidate struct {
	Term       string
	Count      int
	DocFreq    int
	TokenCount int
	Score      int
}

// QueryDomains returns domain aggregate rows for the given mailbox, sorted by
// count desc. When mailboxID is empty, results aggregate across all mailboxes.
func QueryDomains(ctx context.Context, st *storage.Store, mailboxID string, limit int) ([]DomainRow, error) {
	counts, err := st.QueryMessagesByDomain(ctx, mailboxID, limit)
	if err != nil {
		return nil, err
	}
	rows := make([]DomainRow, len(counts))
	for i, c := range counts {
		rows[i] = DomainRow{Domain: c.Domain, Count: c.Count}
	}
	return rows, nil
}

// QuerySenders returns sender aggregate rows for the given mailbox, sorted by
// count desc. When mailboxID is empty, results aggregate across all mailboxes.
func QuerySenders(ctx context.Context, st *storage.Store, mailboxID string, limit int) ([]SenderRow, error) {
	counts, err := st.QueryMessagesBySender(ctx, mailboxID, limit)
	if err != nil {
		return nil, err
	}
	rows := make([]SenderRow, len(counts))
	for i, c := range counts {
		rows[i] = SenderRow{Email: c.Email, Name: c.Name, Domain: c.Domain, Count: c.Count}
	}
	return rows, nil
}

// QuerySubjectTerms returns the top subject term frequencies for the given
// mailbox. When mailboxID is empty, subjects are collected across all mailboxes.
func QuerySubjectTerms(ctx context.Context, st *storage.Store, mailboxID string, limit int) ([]SubjectTerm, error) {
	subjects, err := st.QuerySubjects(ctx, mailboxID)
	if err != nil {
		return nil, err
	}
	return TokenizeSubjects(subjects, limit), nil
}

// QueryVolume returns monthly message counts for the given mailbox, sorted by
// period asc. When mailboxID is empty, results aggregate across all mailboxes.
func QueryVolume(ctx context.Context, st *storage.Store, mailboxID string) ([]VolumeRow, error) {
	counts, err := st.QueryMessagesByVolume(ctx, mailboxID)
	if err != nil {
		return nil, err
	}
	rows := make([]VolumeRow, len(counts))
	for i, c := range counts {
		rows[i] = VolumeRow{Period: c.Period, Count: c.Count}
	}
	return rows, nil
}

// TokenizeSubjects extracts deterministic subject themes using phrase-first
// ranking with stronger normalization and noise filtering. Repeated bigrams and
// trigrams are preferred over unigrams; unigrams are used as fallback when no
// repeated phrase candidates exist.
func TokenizeSubjects(subjects []string, limit int) []SubjectTerm {
	if len(subjects) == 0 || limit == 0 {
		return nil
	}

	unigrams := map[string]*themeCandidate{}
	phrases := map[string]*themeCandidate{}
	for _, subject := range subjects {
		for _, tokens := range subjectThemeSegments(subject) {
			addCandidates(unigrams, buildNGramCandidates(tokens, 1))
			addCandidates(phrases, buildNGramCandidates(tokens, 2))
			addCandidates(phrases, buildNGramCandidates(tokens, 3))
		}
	}

	phraseList := sortedThemeCandidates(phrases)
	if hasStrongPhraseCandidates(phraseList) {
		return toSubjectTerms(phraseList, limit)
	}
	return toSubjectTerms(sortedThemeCandidates(unigrams), limit)
}

func subjectThemeSegments(subject string) [][]string {
	raw := strings.FieldsFunc(strings.ToLower(subject), subjectTokenSplitter)
	for len(raw) > 0 && isReplyPrefix(raw[0]) {
		raw = raw[1:]
	}

	segments := make([][]string, 0, 1)
	current := make([]string, 0, len(raw))
	for _, token := range raw {
		if isUsefulThemeToken(token) {
			current = append(current, token)
			continue
		}
		if len(current) > 0 {
			segments = append(segments, current)
			current = make([]string, 0, len(raw))
		}
	}
	if len(current) > 0 {
		segments = append(segments, current)
	}
	return segments
}

func subjectTokenSplitter(r rune) bool {
	return unicode.IsSpace(r) || strings.ContainsRune(",.;:!?()[]{}<>\"'`|_/\\-:", r)
}

func isReplyPrefix(token string) bool {
	return token == "re" || token == "fw" || token == "fwd"
}

func isUsefulThemeToken(token string) bool {
	switch {
	case len(token) < 2:
		return false
	case subjectNoiseTokens[token]:
		return false
	case isNumericToken(token):
		return false
	case isLikelyYearToken(token):
		return false
	default:
		return true
	}
}

func isNumericToken(token string) bool {
	for _, r := range token {
		if !unicode.IsDigit(r) {
			return false
		}
	}
	return token != ""
}

func isLikelyYearToken(token string) bool {
	if len(token) != 4 || !isNumericToken(token) {
		return false
	}
	year, err := strconv.Atoi(token)
	if err != nil {
		return false
	}
	return year >= 1900 && year <= 2099
}

func buildNGramCandidates(tokens []string, size int) []string {
	if size <= 0 || len(tokens) < size {
		return nil
	}

	out := make([]string, 0, len(tokens)-size+1)
	for i := 0; i <= len(tokens)-size; i++ {
		candidate := strings.Join(tokens[i:i+size], " ")
		if isUsefulThemeCandidate(candidate) {
			out = append(out, candidate)
		}
	}
	return out
}

func isUsefulThemeCandidate(candidate string) bool {
	parts := strings.Fields(candidate)
	if len(parts) == 0 {
		return false
	}
	numericParts := 0
	for _, part := range parts {
		if !isUsefulThemeToken(part) {
			return false
		}
		if isNumericToken(part) || isLikelyYearToken(part) {
			numericParts++
		}
	}
	return numericParts*2 < len(parts)
}

func addCandidates(dst map[string]*themeCandidate, terms []string) {
	seen := make(map[string]bool, len(terms))
	for _, term := range terms {
		candidate := dst[term]
		if candidate == nil {
			candidate = &themeCandidate{
				Term:       term,
				TokenCount: len(strings.Fields(term)),
			}
			dst[term] = candidate
		}
		candidate.Count++
		if !seen[term] {
			candidate.DocFreq++
			seen[term] = true
		}
	}
}

func sortedThemeCandidates(src map[string]*themeCandidate) []themeCandidate {
	out := make([]themeCandidate, 0, len(src))
	for _, candidate := range src {
		candidate.Score = scoreThemeCandidate(*candidate)
		out = append(out, *candidate)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		if out[i].DocFreq != out[j].DocFreq {
			return out[i].DocFreq > out[j].DocFreq
		}
		if out[i].TokenCount != out[j].TokenCount {
			return out[i].TokenCount > out[j].TokenCount
		}
		return out[i].Term < out[j].Term
	})
	return out
}

func scoreThemeCandidate(candidate themeCandidate) int {
	phraseBonus := 0
	if candidate.TokenCount >= 2 {
		phraseBonus = 25 + (candidate.TokenCount-2)*10
	}
	return candidate.Count*100 + candidate.DocFreq*20 + phraseBonus
}

func hasStrongPhraseCandidates(candidates []themeCandidate) bool {
	for _, candidate := range candidates {
		if candidate.TokenCount >= 2 && (candidate.Count >= 2 || candidate.DocFreq >= 2) {
			return true
		}
	}
	return false
}

func toSubjectTerms(candidates []themeCandidate, limit int) []SubjectTerm {
	if limit > 0 && len(candidates) > limit {
		candidates = candidates[:limit]
	}
	out := make([]SubjectTerm, 0, len(candidates))
	for _, candidate := range candidates {
		out = append(out, SubjectTerm{Term: candidate.Term, Count: candidate.Count})
	}
	return out
}

// RenderDomains writes domain rows to w in the requested format.
func RenderDomains(w io.Writer, rows []DomainRow, f Format) error {
	switch f {
	case FormatTable:
		tw := tabwriter.NewWriter(w, 0, 0, 3, ' ', 0)
		_, _ = fmt.Fprintln(tw, "DOMAIN\tCOUNT")
		for _, r := range rows {
			_, _ = fmt.Fprintf(tw, "%s\t%d\n", r.Domain, r.Count)
		}
		return tw.Flush()
	case FormatCSV:
		cw := csv.NewWriter(w)
		_ = cw.Write([]string{"domain", "count"})
		for _, r := range rows {
			_ = cw.Write([]string{r.Domain, fmt.Sprintf("%d", r.Count)})
		}
		cw.Flush()
		return cw.Error()
	case FormatJSON:
		data, err := json.MarshalIndent(rows, "", "  ")
		if err != nil {
			return err
		}
		_, err = w.Write(data)
		return err
	default:
		return fmt.Errorf("unknown format %q", f)
	}
}

// RenderSenders writes sender rows to w in the requested format.
func RenderSenders(w io.Writer, rows []SenderRow, f Format) error {
	switch f {
	case FormatTable:
		tw := tabwriter.NewWriter(w, 0, 0, 3, ' ', 0)
		_, _ = fmt.Fprintln(tw, "EMAIL\tNAME\tDOMAIN\tCOUNT")
		for _, r := range rows {
			_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%d\n", r.Email, r.Name, r.Domain, r.Count)
		}
		return tw.Flush()
	case FormatCSV:
		cw := csv.NewWriter(w)
		_ = cw.Write([]string{"email", "name", "domain", "count"})
		for _, r := range rows {
			_ = cw.Write([]string{r.Email, r.Name, r.Domain, fmt.Sprintf("%d", r.Count)})
		}
		cw.Flush()
		return cw.Error()
	case FormatJSON:
		data, err := json.MarshalIndent(rows, "", "  ")
		if err != nil {
			return err
		}
		_, err = w.Write(data)
		return err
	default:
		return fmt.Errorf("unknown format %q", f)
	}
}

// RenderSubjectTerms writes subject term rows to w in the requested format.
func RenderSubjectTerms(w io.Writer, rows []SubjectTerm, f Format) error {
	switch f {
	case FormatTable:
		tw := tabwriter.NewWriter(w, 0, 0, 3, ' ', 0)
		_, _ = fmt.Fprintln(tw, "TERM\tCOUNT")
		for _, r := range rows {
			_, _ = fmt.Fprintf(tw, "%s\t%d\n", r.Term, r.Count)
		}
		return tw.Flush()
	case FormatCSV:
		cw := csv.NewWriter(w)
		_ = cw.Write([]string{"term", "count"})
		for _, r := range rows {
			_ = cw.Write([]string{r.Term, fmt.Sprintf("%d", r.Count)})
		}
		cw.Flush()
		return cw.Error()
	case FormatJSON:
		data, err := json.MarshalIndent(rows, "", "  ")
		if err != nil {
			return err
		}
		_, err = w.Write(data)
		return err
	default:
		return fmt.Errorf("unknown format %q", f)
	}
}

// RenderVolume writes volume rows to w in the requested format.
func RenderVolume(w io.Writer, rows []VolumeRow, f Format) error {
	switch f {
	case FormatTable:
		tw := tabwriter.NewWriter(w, 0, 0, 3, ' ', 0)
		_, _ = fmt.Fprintln(tw, "PERIOD\tCOUNT")
		for _, r := range rows {
			_, _ = fmt.Fprintf(tw, "%s\t%d\n", r.Period, r.Count)
		}
		return tw.Flush()
	case FormatCSV:
		cw := csv.NewWriter(w)
		_ = cw.Write([]string{"period", "count"})
		for _, r := range rows {
			_ = cw.Write([]string{r.Period, fmt.Sprintf("%d", r.Count)})
		}
		cw.Flush()
		return cw.Error()
	case FormatJSON:
		data, err := json.MarshalIndent(rows, "", "  ")
		if err != nil {
			return err
		}
		_, err = w.Write(data)
		return err
	default:
		return fmt.Errorf("unknown format %q", f)
	}
}
