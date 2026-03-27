// Package export parses reports-directory inputs into a normalized export model
// for later workbook and snapshot renderers.
package export

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	domainsFilename  = "domains.csv"
	sendersFilename  = "senders.csv"
	subjectsFilename = "subjects.csv"
	volumeFilename   = "volume.csv"
)

// Owner describes the inbox owner context used to filter internal rows out of
// external rankings.
type Owner struct {
	Email  string
	Domain string
}

// Options configures reports-directory parsing and aggregation.
type Options struct {
	ReportsDir  string
	OwnerEmail  string
	OwnerDomain string
}

// Model is the normalized export model shared by later workbook and snapshot
// renderers.
type Model struct {
	Owner              Owner
	Summary            Summary
	Senders            []SenderMetric
	Domains            []DomainMetric
	Subjects           []SubjectMetric
	Volume             []VolumeMetric
	ExternalTopSenders []SenderMetric
	ExternalTopDomains []DomainMetric
}

// Summary contains derived report-wide metrics for later renderers.
type Summary struct {
	TotalMessages        int
	ReportingPeriodStart string
	ReportingPeriodEnd   string
	TopExternalSender    string
	TopExternalDomain    string
}

// SenderMetric is a normalized sender aggregate row.
type SenderMetric struct {
	Email          string
	Name           string
	Domain         string
	Count          int
	PercentOfTotal float64
	Internal       bool
}

// DomainMetric is a normalized domain aggregate row.
type DomainMetric struct {
	Domain         string
	Count          int
	PercentOfTotal float64
	Internal       bool
}

// SubjectMetric is a normalized subject-theme aggregate row.
type SubjectMetric struct {
	Term           string
	Count          int
	PercentOfTotal float64
}

// VolumeMetric is a normalized monthly volume row with derived deltas.
type VolumeMetric struct {
	Period         string
	Count          int
	PercentOfTotal float64
	MoMChange      int
	MoMPercent     float64
}

// ParseReportsDir loads the required report CSV files from opts.ReportsDir,
// validates them, and returns a normalized export model.
func ParseReportsDir(opts Options) (*Model, error) {
	if strings.TrimSpace(opts.ReportsDir) == "" {
		return nil, errors.New("reports dir is required")
	}
	info, err := os.Stat(opts.ReportsDir)
	if err != nil {
		return nil, fmt.Errorf("stat reports dir: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("reports dir %q is not a directory", opts.ReportsDir)
	}

	owner := normalizeOwner(opts.OwnerEmail, opts.OwnerDomain)

	domains, err := parseDomains(filepath.Join(opts.ReportsDir, domainsFilename))
	if err != nil {
		return nil, err
	}
	senders, err := parseSenders(filepath.Join(opts.ReportsDir, sendersFilename))
	if err != nil {
		return nil, err
	}
	subjects, err := parseSubjects(filepath.Join(opts.ReportsDir, subjectsFilename))
	if err != nil {
		return nil, err
	}
	volume, err := parseVolume(filepath.Join(opts.ReportsDir, volumeFilename))
	if err != nil {
		return nil, err
	}

	model := &Model{Owner: owner}
	model.Volume = buildVolumeMetrics(volume)
	model.Summary.TotalMessages = totalMessages(model.Volume)
	if len(model.Volume) > 0 {
		model.Summary.ReportingPeriodStart = model.Volume[0].Period
		model.Summary.ReportingPeriodEnd = model.Volume[len(model.Volume)-1].Period
	}
	model.Senders = buildSenderMetrics(senders, owner, model.Summary.TotalMessages)
	model.Domains = buildDomainMetrics(domains, owner, model.Summary.TotalMessages)
	model.Subjects = buildSubjectMetrics(subjects, model.Summary.TotalMessages)
	model.ExternalTopSenders = externalSenders(model.Senders)
	model.ExternalTopDomains = externalDomains(model.Domains)
	if len(model.ExternalTopSenders) > 0 {
		model.Summary.TopExternalSender = model.ExternalTopSenders[0].Email
	}
	if len(model.ExternalTopDomains) > 0 {
		model.Summary.TopExternalDomain = model.ExternalTopDomains[0].Domain
	}
	return model, nil
}

type domainRow struct {
	Domain string
	Count  int
}

type senderRow struct {
	Email  string
	Name   string
	Domain string
	Count  int
}

type subjectRow struct {
	Term  string
	Count int
}

type volumeRow struct {
	Period string
	Count  int
}

func normalizeOwner(email, domain string) Owner {
	email = strings.TrimSpace(strings.ToLower(email))
	domain = strings.TrimSpace(strings.ToLower(domain))
	if domain == "" && strings.Contains(email, "@") {
		domain = email[strings.LastIndex(email, "@")+1:]
	}
	return Owner{Email: email, Domain: domain}
}

func parseDomains(path string) ([]domainRow, error) {
	records, err := readCSV(path)
	if err != nil {
		return nil, err
	}
	if err := requireHeader(path, records[0], []string{"domain", "count"}); err != nil {
		return nil, err
	}
	out := make([]domainRow, 0, len(records)-1)
	for i, rec := range records[1:] {
		if isBlankRecord(rec) {
			continue
		}
		if len(rec) != 2 {
			return nil, fmt.Errorf("%s row %d: expected 2 columns, got %d", path, i+2, len(rec))
		}
		count, err := strconv.Atoi(strings.TrimSpace(rec[1]))
		if err != nil {
			return nil, fmt.Errorf("%s row %d: parse count: %w", path, i+2, err)
		}
		out = append(out, domainRow{Domain: strings.TrimSpace(rec[0]), Count: count})
	}
	return out, nil
}

func parseSenders(path string) ([]senderRow, error) {
	records, err := readCSV(path)
	if err != nil {
		return nil, err
	}
	if err := requireHeader(path, records[0], []string{"email", "name", "domain", "count"}); err != nil {
		return nil, err
	}
	out := make([]senderRow, 0, len(records)-1)
	for i, rec := range records[1:] {
		if isBlankRecord(rec) {
			continue
		}
		if len(rec) != 4 {
			return nil, fmt.Errorf("%s row %d: expected 4 columns, got %d", path, i+2, len(rec))
		}
		count, err := strconv.Atoi(strings.TrimSpace(rec[3]))
		if err != nil {
			return nil, fmt.Errorf("%s row %d: parse count: %w", path, i+2, err)
		}
		out = append(out, senderRow{
			Email:  strings.TrimSpace(rec[0]),
			Name:   strings.TrimSpace(rec[1]),
			Domain: strings.TrimSpace(rec[2]),
			Count:  count,
		})
	}
	return out, nil
}

func parseSubjects(path string) ([]subjectRow, error) {
	records, err := readCSV(path)
	if err != nil {
		return nil, err
	}
	if err := requireHeader(path, records[0], []string{"term", "count"}); err != nil {
		return nil, err
	}
	out := make([]subjectRow, 0, len(records)-1)
	for i, rec := range records[1:] {
		if isBlankRecord(rec) {
			continue
		}
		if len(rec) != 2 {
			return nil, fmt.Errorf("%s row %d: expected 2 columns, got %d", path, i+2, len(rec))
		}
		count, err := strconv.Atoi(strings.TrimSpace(rec[1]))
		if err != nil {
			return nil, fmt.Errorf("%s row %d: parse count: %w", path, i+2, err)
		}
		out = append(out, subjectRow{Term: strings.TrimSpace(rec[0]), Count: count})
	}
	return out, nil
}

func parseVolume(path string) ([]volumeRow, error) {
	records, err := readCSV(path)
	if err != nil {
		return nil, err
	}
	if err := requireHeader(path, records[0], []string{"period", "count"}); err != nil {
		return nil, err
	}
	out := make([]volumeRow, 0, len(records)-1)
	for i, rec := range records[1:] {
		if isBlankRecord(rec) {
			continue
		}
		if len(rec) != 2 {
			return nil, fmt.Errorf("%s row %d: expected 2 columns, got %d", path, i+2, len(rec))
		}
		period := strings.TrimSpace(rec[0])
		if _, err := time.Parse("2006-01", period); err != nil {
			return nil, fmt.Errorf("%s row %d: parse period: %w", path, i+2, err)
		}
		count, err := strconv.Atoi(strings.TrimSpace(rec[1]))
		if err != nil {
			return nil, fmt.Errorf("%s row %d: parse count: %w", path, i+2, err)
		}
		out = append(out, volumeRow{Period: period, Count: count})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Period < out[j].Period })
	return out, nil
}

func readCSV(path string) ([][]string, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("required report file missing: %s", filepath.Base(path))
		}
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	records, err := csv.NewReader(f).ReadAll()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("%s is empty", filepath.Base(path))
		}
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("%s is empty", filepath.Base(path))
	}
	return records, nil
}

func requireHeader(path string, got, want []string) error {
	if len(got) != len(want) {
		return fmt.Errorf("%s header: expected %v, got %v", filepath.Base(path), want, got)
	}
	for i := range want {
		if strings.TrimSpace(strings.ToLower(got[i])) != want[i] {
			return fmt.Errorf("%s header: expected %v, got %v", filepath.Base(path), want, got)
		}
	}
	return nil
}

func isBlankRecord(rec []string) bool {
	for _, field := range rec {
		if strings.TrimSpace(field) != "" {
			return false
		}
	}
	return true
}

func totalMessages(rows []VolumeMetric) int {
	total := 0
	for _, row := range rows {
		total += row.Count
	}
	return total
}

func buildSenderMetrics(rows []senderRow, owner Owner, total int) []SenderMetric {
	out := make([]SenderMetric, 0, len(rows))
	for _, row := range rows {
		out = append(out, SenderMetric{
			Email:          row.Email,
			Name:           row.Name,
			Domain:         strings.ToLower(row.Domain),
			Count:          row.Count,
			PercentOfTotal: percent(row.Count, total),
			Internal:       isInternalSender(row.Email, row.Domain, owner),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return out[i].Email < out[j].Email
	})
	return out
}

func buildDomainMetrics(rows []domainRow, owner Owner, total int) []DomainMetric {
	out := make([]DomainMetric, 0, len(rows))
	for _, row := range rows {
		domain := strings.ToLower(row.Domain)
		out = append(out, DomainMetric{
			Domain:         domain,
			Count:          row.Count,
			PercentOfTotal: percent(row.Count, total),
			Internal:       owner.Domain != "" && domain == owner.Domain,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return out[i].Domain < out[j].Domain
	})
	return out
}

func buildSubjectMetrics(rows []subjectRow, total int) []SubjectMetric {
	out := make([]SubjectMetric, 0, len(rows))
	for _, row := range rows {
		out = append(out, SubjectMetric{
			Term:           row.Term,
			Count:          row.Count,
			PercentOfTotal: percent(row.Count, total),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return out[i].Term < out[j].Term
	})
	return out
}

func buildVolumeMetrics(rows []volumeRow) []VolumeMetric {
	out := make([]VolumeMetric, 0, len(rows))
	total := 0
	for _, row := range rows {
		total += row.Count
	}
	for i, row := range rows {
		metric := VolumeMetric{
			Period:         row.Period,
			Count:          row.Count,
			PercentOfTotal: percent(row.Count, total),
		}
		if i > 0 {
			metric.MoMChange = row.Count - rows[i-1].Count
			if rows[i-1].Count > 0 {
				metric.MoMPercent = float64(metric.MoMChange) / float64(rows[i-1].Count) * 100
			}
		}
		out = append(out, metric)
	}
	return out
}

func externalSenders(rows []SenderMetric) []SenderMetric {
	out := make([]SenderMetric, 0, len(rows))
	for _, row := range rows {
		if !row.Internal {
			out = append(out, row)
		}
	}
	return out
}

func externalDomains(rows []DomainMetric) []DomainMetric {
	out := make([]DomainMetric, 0, len(rows))
	for _, row := range rows {
		if !row.Internal {
			out = append(out, row)
		}
	}
	return out
}

func isInternalSender(email, domain string, owner Owner) bool {
	email = strings.ToLower(strings.TrimSpace(email))
	domain = strings.ToLower(strings.TrimSpace(domain))
	return (owner.Email != "" && email == owner.Email) || (owner.Domain != "" && domain == owner.Domain)
}

func percent(count, total int) float64 {
	if total <= 0 {
		return 0
	}
	return float64(count) / float64(total) * 100
}
