// Package analysis computes domain, sender, subject, and volume insights from stored messages.
package analysis

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"sort"
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

// stopWords is the set of tokens filtered out by TokenizeSubjects.
var stopWords = map[string]bool{
	"the": true, "a": true, "an": true, "re": true, "fw": true, "fwd": true,
	"is": true, "in": true, "of": true, "to": true, "and": true, "for": true,
	"on": true, "at": true, "with": true, "be": true, "as": true, "by": true,
	"or": true, "external": true,
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

// TokenizeSubjects splits subjects into tokens, filters stop words, and returns
// the top limit terms by frequency. Terms are lowercased; tokens shorter than
// 2 characters are discarded.
func TokenizeSubjects(subjects []string, limit int) []SubjectTerm {
	freq := make(map[string]int)
	splitter := func(r rune) bool {
		return unicode.IsSpace(r) || strings.ContainsRune(",.;:!?()[]\"'-", r)
	}
	for _, s := range subjects {
		for _, token := range strings.FieldsFunc(s, splitter) {
			t := strings.ToLower(token)
			if len(t) < 2 {
				continue
			}
			if stopWords[t] {
				continue
			}
			freq[t]++
		}
	}

	terms := make([]SubjectTerm, 0, len(freq))
	for term, count := range freq {
		terms = append(terms, SubjectTerm{Term: term, Count: count})
	}
	sort.Slice(terms, func(i, j int) bool {
		if terms[i].Count != terms[j].Count {
			return terms[i].Count > terms[j].Count
		}
		return terms[i].Term < terms[j].Term
	})

	if limit > 0 && len(terms) > limit {
		terms = terms[:limit]
	}
	return terms
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
