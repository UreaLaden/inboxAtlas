package export

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

const (
	maxSummaryBullets = 5
	summaryTopLimit   = 5
)

var (
	// ErrSummaryInputRequired is returned when AI summary input cannot be built
	// because the normalized export model is missing.
	ErrSummaryInputRequired = errors.New("summary input is required")

	// ErrSummaryOutputInvalid is returned when structured AI summary output fails
	// schema or fact validation and therefore cannot be adapted into a snapshot
	// narrative.
	ErrSummaryOutputInvalid = errors.New("summary output is invalid")

	emailPattern  = regexp.MustCompile(`(?i)\b[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}\b`)
	domainPattern = regexp.MustCompile(`(?i)\b(?:[A-Z0-9](?:[A-Z0-9\-]*[A-Z0-9])?\.)+[A-Z]{2,}\b`)
	numberPattern = regexp.MustCompile(`\b(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?%?\b`)
)

// SummaryInput is the deterministic payload supplied to the AI summary stage.
// It contains only facts derived from the normalized export model.
type SummaryInput struct {
	Owner              Owner                   `json:"owner"`
	ReportingPeriod    SummaryReportingPeriod  `json:"reporting_period"`
	TotalMessages      int                     `json:"total_messages"`
	TopExternalSenders []SummarySenderFact     `json:"top_external_senders"`
	TopExternalDomains []SummaryDomainFact     `json:"top_external_domains"`
	TopSubjectThemes   []SummarySubjectFact    `json:"top_subject_themes"`
	VolumeHighlights   SummaryVolumeHighlights `json:"volume_highlights"`
	DerivedMetrics     SummaryDerivedMetrics   `json:"derived_metrics"`
}

// SummaryReportingPeriod describes the reporting window for the export model.
type SummaryReportingPeriod struct {
	Start string `json:"start"`
	End   string `json:"end"`
	Label string `json:"label"`
}

// SummarySenderFact is a deterministic external-sender ranking row for the AI
// summary payload.
type SummarySenderFact struct {
	Email          string  `json:"email"`
	Name           string  `json:"name"`
	Domain         string  `json:"domain"`
	Count          int     `json:"count"`
	PercentOfTotal float64 `json:"percent_of_total"`
}

// SummaryDomainFact is a deterministic external-domain ranking row for the AI
// summary payload.
type SummaryDomainFact struct {
	Domain         string  `json:"domain"`
	Count          int     `json:"count"`
	PercentOfTotal float64 `json:"percent_of_total"`
}

// SummarySubjectFact is a deterministic subject-theme ranking row for the AI
// summary payload.
type SummarySubjectFact struct {
	Term           string  `json:"term"`
	Count          int     `json:"count"`
	PercentOfTotal float64 `json:"percent_of_total"`
}

// SummaryVolumeHighlights captures deterministic volume trend facts for the AI
// summary payload.
type SummaryVolumeHighlights struct {
	FirstPeriod    string  `json:"first_period"`
	FirstCount     int     `json:"first_count"`
	LastPeriod     string  `json:"last_period"`
	LastCount      int     `json:"last_count"`
	AbsoluteChange int     `json:"absolute_change"`
	PercentChange  float64 `json:"percent_change"`
	PeakPeriod     string  `json:"peak_period"`
	PeakCount      int     `json:"peak_count"`
}

// SummaryDerivedMetrics carries additional deterministic facts already
// available from the normalized export model.
type SummaryDerivedMetrics struct {
	TopExternalSender      string `json:"top_external_sender"`
	TopExternalSenderCount int    `json:"top_external_sender_count"`
	TopExternalDomain      string `json:"top_external_domain"`
	TopExternalDomainCount int    `json:"top_external_domain_count"`
}

// SummaryOutput is the strict structured result expected from the AI summary
// stage before adaptation into SnapshotNarrative.
type SummaryOutput struct {
	Title                string   `json:"title"`
	Subtitle             string   `json:"subtitle"`
	Headline             string   `json:"headline"`
	SecondaryHeadline    string   `json:"secondary_headline"`
	SnapshotBullets      []string `json:"snapshot_bullets"`
	WhatThisMeansBullets []string `json:"what_this_means_bullets"`
	OpportunitiesBullets []string `json:"opportunities_bullets"`
	BottomLine           string   `json:"bottom_line"`
}

// BuildSummaryInput derives the deterministic AI summary payload from the
// normalized export model.
func BuildSummaryInput(model *Model) (SummaryInput, error) {
	if model == nil {
		return SummaryInput{}, ErrSummaryInputRequired
	}

	input := SummaryInput{
		Owner:         model.Owner,
		TotalMessages: model.Summary.TotalMessages,
		ReportingPeriod: SummaryReportingPeriod{
			Start: model.Summary.ReportingPeriodStart,
			End:   model.Summary.ReportingPeriodEnd,
			Label: summaryReportingLabel(model.Summary.ReportingPeriodStart, model.Summary.ReportingPeriodEnd),
		},
		TopExternalSenders: buildSummarySenderFacts(model.ExternalTopSenders),
		TopExternalDomains: buildSummaryDomainFacts(model.ExternalTopDomains),
		TopSubjectThemes:   buildSummarySubjectFacts(model.Subjects),
		VolumeHighlights:   buildVolumeHighlights(model.Volume),
	}

	if len(model.ExternalTopSenders) > 0 {
		input.DerivedMetrics.TopExternalSender = model.ExternalTopSenders[0].Email
		input.DerivedMetrics.TopExternalSenderCount = model.ExternalTopSenders[0].Count
	}
	if len(model.ExternalTopDomains) > 0 {
		input.DerivedMetrics.TopExternalDomain = model.ExternalTopDomains[0].Domain
		input.DerivedMetrics.TopExternalDomainCount = model.ExternalTopDomains[0].Count
	}

	return input, nil
}

// ParseSummaryOutputJSON decodes strict JSON output from the AI summary stage.
func ParseSummaryOutputJSON(data []byte) (SummaryOutput, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()

	var output SummaryOutput
	if err := dec.Decode(&output); err != nil {
		return SummaryOutput{}, fmt.Errorf("%w: parse json: %v", ErrSummaryOutputInvalid, err)
	}
	if err := dec.Decode(new(struct{})); err == nil {
		return SummaryOutput{}, fmt.Errorf("%w: extra trailing content", ErrSummaryOutputInvalid)
	}

	output = normalizeSummaryOutput(output)
	return output, nil
}

// AdaptSummaryOutput validates structured AI summary output against the
// deterministic input payload and converts valid output into SnapshotNarrative.
func AdaptSummaryOutput(input SummaryInput, output SummaryOutput) (SnapshotNarrative, error) {
	output = normalizeSummaryOutput(output)
	if err := ValidateSummaryOutput(input, output); err != nil {
		return SnapshotNarrative{}, err
	}

	narrative := SnapshotNarrative{
		Title:             defaultString(output.Title, "Inbox Snapshot"),
		Subtitle:          defaultString(output.Subtitle, defaultSummarySubtitle(input)),
		Headline:          output.Headline,
		SecondaryHeadline: output.SecondaryHeadline,
		Snapshot:          append([]string(nil), output.SnapshotBullets...),
		WhatThisMeans:     append([]string(nil), output.WhatThisMeansBullets...),
		Opportunities:     append([]string(nil), output.OpportunitiesBullets...),
		BottomLine:        output.BottomLine,
	}
	return narrative, nil
}

// ValidateSummaryOutput enforces the structured AI summary schema and fact
// guardrails before any snapshot rendering occurs.
func ValidateSummaryOutput(input SummaryInput, output SummaryOutput) error {
	if err := validateSummaryInput(input); err != nil {
		return err
	}

	switch {
	case strings.TrimSpace(output.Headline) == "":
		return fmt.Errorf("%w: headline is required", ErrSummaryOutputInvalid)
	case strings.TrimSpace(output.SecondaryHeadline) == "":
		return fmt.Errorf("%w: secondary_headline is required", ErrSummaryOutputInvalid)
	case strings.TrimSpace(output.BottomLine) == "":
		return fmt.Errorf("%w: bottom_line is required", ErrSummaryOutputInvalid)
	}

	if err := validateSummaryBullets("snapshot_bullets", output.SnapshotBullets); err != nil {
		return err
	}
	if err := validateSummaryBullets("what_this_means_bullets", output.WhatThisMeansBullets); err != nil {
		return err
	}
	if err := validateSummaryBullets("opportunities_bullets", output.OpportunitiesBullets); err != nil {
		return err
	}

	allowedFacts := newAllowedFacts(input)
	for _, text := range summaryOutputTexts(output) {
		if err := validateOutputTextFacts(text, allowedFacts); err != nil {
			return err
		}
	}

	return nil
}

// FormatSnapshotNarrativeMarkdown renders canonical summary.md content from a
// validated SnapshotNarrative.
func FormatSnapshotNarrativeMarkdown(narrative SnapshotNarrative) ([]byte, error) {
	if err := validateNarrative(narrative); err != nil {
		return nil, err
	}

	var buf strings.Builder
	buf.WriteString("# ")
	buf.WriteString(defaultString(narrative.Title, "Inbox Snapshot"))
	buf.WriteString("\n")

	if subtitle := strings.TrimSpace(narrative.Subtitle); subtitle != "" {
		buf.WriteString(subtitle)
		buf.WriteString("\n")
	}

	writeMarkdownSection(&buf, "Key Takeaway", []string{narrative.Headline})
	writeMarkdownSection(&buf, "Secondary Takeaway", []string{narrative.SecondaryHeadline})
	writeMarkdownSection(&buf, "Snapshot", narrative.Snapshot)
	writeMarkdownSection(&buf, "What This Means", narrative.WhatThisMeans)
	writeMarkdownSection(&buf, "Opportunities to Improve", narrative.Opportunities)
	writeMarkdownSection(&buf, "Bottom Line", []string{narrative.BottomLine})

	return []byte(buf.String()), nil
}

func validateSummaryInput(input SummaryInput) error {
	if input.TotalMessages < 0 {
		return fmt.Errorf("%w: total_messages must be non-negative", ErrSummaryOutputInvalid)
	}
	return nil
}

func normalizeSummaryOutput(output SummaryOutput) SummaryOutput {
	output.Title = strings.TrimSpace(output.Title)
	output.Subtitle = strings.TrimSpace(output.Subtitle)
	output.Headline = strings.TrimSpace(output.Headline)
	output.SecondaryHeadline = strings.TrimSpace(output.SecondaryHeadline)
	output.BottomLine = strings.TrimSpace(output.BottomLine)
	output.SnapshotBullets = normalizeBullets(output.SnapshotBullets)
	output.WhatThisMeansBullets = normalizeBullets(output.WhatThisMeansBullets)
	output.OpportunitiesBullets = normalizeBullets(output.OpportunitiesBullets)
	return output
}

func normalizeBullets(items []string) []string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		if trimmed := strings.TrimSpace(item); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func validateSummaryBullets(name string, bullets []string) error {
	switch {
	case len(bullets) == 0:
		return fmt.Errorf("%w: %s must contain at least one bullet", ErrSummaryOutputInvalid, name)
	case len(bullets) > maxSummaryBullets:
		return fmt.Errorf("%w: %s may contain at most %d bullets", ErrSummaryOutputInvalid, name, maxSummaryBullets)
	default:
		return nil
	}
}

func summaryOutputTexts(output SummaryOutput) []string {
	return append([]string{
		output.Title,
		output.Subtitle,
		output.Headline,
		output.SecondaryHeadline,
		output.BottomLine,
	}, append(append([]string{}, output.SnapshotBullets...), append(output.WhatThisMeansBullets, output.OpportunitiesBullets...)...)...)
}

type allowedFacts struct {
	emails  map[string]struct{}
	domains map[string]struct{}
	numbers map[string]struct{}
}

func newAllowedFacts(input SummaryInput) allowedFacts {
	facts := allowedFacts{
		emails:  map[string]struct{}{},
		domains: map[string]struct{}{},
		numbers: map[string]struct{}{},
	}

	addTextFacts := func(text string) {
		for _, match := range emailPattern.FindAllString(strings.ToLower(text), -1) {
			facts.emails[match] = struct{}{}
			if at := strings.LastIndex(match, "@"); at >= 0 && at+1 < len(match) {
				facts.domains[match[at+1:]] = struct{}{}
			}
		}
		for _, match := range domainPattern.FindAllString(strings.ToLower(text), -1) {
			facts.domains[match] = struct{}{}
		}
		for _, match := range numberPattern.FindAllString(text, -1) {
			facts.numbers[normalizeNumericFact(match)] = struct{}{}
		}
	}
	addCount := func(value int) {
		addTextFacts(strconv.Itoa(value))
	}
	addPercent := func(value float64) {
		addTextFacts(strconv.FormatFloat(value, 'f', -1, 64))
		addTextFacts(strconv.FormatFloat(value, 'f', -1, 64) + "%")
		addTextFacts(fmt.Sprintf("%.2f", value))
		addTextFacts(fmt.Sprintf("%.2f%%", value))
		addTextFacts(fmt.Sprintf("%.1f", value))
		addTextFacts(fmt.Sprintf("%.1f%%", value))
		addTextFacts(fmt.Sprintf("%.0f", value))
		addTextFacts(fmt.Sprintf("%.0f%%", value))
	}

	addTextFacts(input.Owner.Email)
	addTextFacts(input.Owner.Domain)
	addTextFacts(input.ReportingPeriod.Start)
	addTextFacts(input.ReportingPeriod.End)
	addTextFacts(input.ReportingPeriod.Label)
	addCount(input.TotalMessages)

	for _, sender := range input.TopExternalSenders {
		addTextFacts(sender.Email)
		addTextFacts(sender.Name)
		addTextFacts(sender.Domain)
		addCount(sender.Count)
		addPercent(sender.PercentOfTotal)
	}
	for _, domain := range input.TopExternalDomains {
		addTextFacts(domain.Domain)
		addCount(domain.Count)
		addPercent(domain.PercentOfTotal)
	}
	for _, subject := range input.TopSubjectThemes {
		addTextFacts(subject.Term)
		addCount(subject.Count)
		addPercent(subject.PercentOfTotal)
	}

	addTextFacts(input.VolumeHighlights.FirstPeriod)
	addTextFacts(input.VolumeHighlights.LastPeriod)
	addTextFacts(input.VolumeHighlights.PeakPeriod)
	addCount(input.VolumeHighlights.FirstCount)
	addCount(input.VolumeHighlights.LastCount)
	addCount(input.VolumeHighlights.AbsoluteChange)
	addCount(input.VolumeHighlights.PeakCount)
	addPercent(input.VolumeHighlights.PercentChange)

	addTextFacts(input.DerivedMetrics.TopExternalSender)
	addTextFacts(input.DerivedMetrics.TopExternalDomain)
	addCount(input.DerivedMetrics.TopExternalSenderCount)
	addCount(input.DerivedMetrics.TopExternalDomainCount)

	return facts
}

func validateOutputTextFacts(text string, facts allowedFacts) error {
	lower := strings.ToLower(text)

	for _, match := range emailPattern.FindAllString(lower, -1) {
		if _, ok := facts.emails[match]; !ok {
			return fmt.Errorf("%w: unsupported email reference %q", ErrSummaryOutputInvalid, match)
		}
	}
	for _, match := range domainPattern.FindAllString(lower, -1) {
		if _, ok := facts.domains[match]; !ok {
			return fmt.Errorf("%w: unsupported domain reference %q", ErrSummaryOutputInvalid, match)
		}
	}
	for _, match := range numberPattern.FindAllString(text, -1) {
		if _, ok := facts.numbers[normalizeNumericFact(match)]; !ok {
			return fmt.Errorf("%w: unsupported numeric fact %q in %q", ErrSummaryOutputInvalid, match, text)
		}
	}
	return nil
}

func normalizeNumericFact(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}

	hasPercent := strings.HasSuffix(trimmed, "%")
	if hasPercent {
		trimmed = strings.TrimSuffix(trimmed, "%")
	}
	trimmed = strings.ReplaceAll(trimmed, ",", "")

	if strings.Contains(trimmed, ".") {
		parsed, err := strconv.ParseFloat(trimmed, 64)
		if err != nil {
			return value
		}
		normalized := strconv.FormatFloat(parsed, 'f', -1, 64)
		if hasPercent {
			return normalized + "%"
		}
		return normalized
	}

	parsed, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil {
		return value
	}
	normalized := strconv.FormatInt(parsed, 10)
	if hasPercent {
		return normalized + "%"
	}
	return normalized
}

func buildSummarySenderFacts(rows []SenderMetric) []SummarySenderFact {
	limit := min(summaryTopLimit, len(rows))
	out := make([]SummarySenderFact, 0, limit)
	for _, row := range rows[:limit] {
		out = append(out, SummarySenderFact{
			Email:          row.Email,
			Name:           row.Name,
			Domain:         row.Domain,
			Count:          row.Count,
			PercentOfTotal: row.PercentOfTotal,
		})
	}
	return out
}

func buildSummaryDomainFacts(rows []DomainMetric) []SummaryDomainFact {
	limit := min(summaryTopLimit, len(rows))
	out := make([]SummaryDomainFact, 0, limit)
	for _, row := range rows[:limit] {
		out = append(out, SummaryDomainFact{
			Domain:         row.Domain,
			Count:          row.Count,
			PercentOfTotal: row.PercentOfTotal,
		})
	}
	return out
}

func buildSummarySubjectFacts(rows []SubjectMetric) []SummarySubjectFact {
	limit := min(summaryTopLimit, len(rows))
	out := make([]SummarySubjectFact, 0, limit)
	for _, row := range rows[:limit] {
		out = append(out, SummarySubjectFact(row))
	}
	return out
}

func buildVolumeHighlights(rows []VolumeMetric) SummaryVolumeHighlights {
	if len(rows) == 0 {
		return SummaryVolumeHighlights{}
	}

	peak := rows[0]
	for _, row := range rows[1:] {
		if row.Count > peak.Count || (row.Count == peak.Count && row.Period < peak.Period) {
			peak = row
		}
	}

	first := rows[0]
	last := rows[len(rows)-1]
	highlights := SummaryVolumeHighlights{
		FirstPeriod:    first.Period,
		FirstCount:     first.Count,
		LastPeriod:     last.Period,
		LastCount:      last.Count,
		AbsoluteChange: last.Count - first.Count,
		PeakPeriod:     peak.Period,
		PeakCount:      peak.Count,
	}
	if first.Count > 0 {
		highlights.PercentChange = float64(highlights.AbsoluteChange) / float64(first.Count) * 100
	}
	return highlights
}

func summaryReportingLabel(start, end string) string {
	switch {
	case start != "" && end != "" && start != end:
		return start + " to " + end
	case start != "":
		return start
	case end != "":
		return end
	default:
		return ""
	}
}

func defaultSummarySubtitle(input SummaryInput) string {
	switch {
	case strings.TrimSpace(input.Owner.Email) != "":
		return input.Owner.Email
	case strings.TrimSpace(input.ReportingPeriod.Label) != "":
		return input.ReportingPeriod.Label
	default:
		return "Unscoped reports"
	}
}

func writeMarkdownSection(buf *strings.Builder, heading string, lines []string) {
	buf.WriteString("\n## ")
	buf.WriteString(heading)
	buf.WriteString("\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if slices.Contains([]string{"Snapshot", "What This Means", "Opportunities to Improve"}, heading) {
			buf.WriteString("- ")
		}
		buf.WriteString(line)
		buf.WriteString("\n")
	}
}
