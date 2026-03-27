package export

import (
	"bytes"
	"errors"
	"fmt"
	"html/template"
	"os"
	"strings"
	"time"
)

var (
	// ErrSnapshotNarrativeRequired is returned when snapshot rendering is asked to
	// run without the required narrative sections.
	ErrSnapshotNarrativeRequired = errors.New("snapshot narrative is required")

	// ErrPDFRendererUnavailable is returned when PDF rendering cannot proceed
	// because no PDF-capable renderer has been configured.
	ErrPDFRendererUnavailable = errors.New("pdf renderer unavailable")
)

// SnapshotNarrative carries the explicit summary sections required for HTML and
// PDF-capable snapshot rendering.
type SnapshotNarrative struct {
	Title             string
	Subtitle          string
	Headline          string
	SecondaryHeadline string
	Snapshot          []string
	WhatThisMeans     []string
	Opportunities     []string
	BottomLine        string
}

// SnapshotOptions configures HTML snapshot rendering.
type SnapshotOptions struct {
	GeneratedAt time.Time
}

// PDFRenderer converts rendered HTML snapshot bytes into PDF bytes.
type PDFRenderer interface {
	RenderPDF(html []byte) ([]byte, error)
}

// LoadSnapshotNarrative reads a summary markdown file and parses the required
// snapshot narrative sections from it.
func LoadSnapshotNarrative(path string) (SnapshotNarrative, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return SnapshotNarrative{}, fmt.Errorf("read snapshot narrative: %w", err)
	}
	return ParseSnapshotNarrative(body)
}

// ParseSnapshotNarrative parses the required snapshot narrative sections from a
// markdown summary document.
func ParseSnapshotNarrative(markdown []byte) (SnapshotNarrative, error) {
	sections := splitMarkdownSections(string(markdown))
	titleLines := nonEmptyLines(sections["title"])

	narrative := SnapshotNarrative{
		Title:             firstValue(titleLines, 0),
		Headline:          firstNonEmptyLine(sections["key takeaway"]),
		SecondaryHeadline: firstNonEmptyLine(sections["secondary takeaway"]),
		BottomLine:        firstNonEmptyLine(sections["bottom line"]),
		Snapshot:          parseBulletSection(sections["snapshot"]),
		WhatThisMeans:     parseBulletSection(sections["what this means"]),
		Opportunities:     parseBulletSection(sections["opportunities to improve"]),
	}

	if subtitle := firstNonEmptyLine(sections["subtitle"]); subtitle != "" {
		narrative.Subtitle = subtitle
	} else if len(titleLines) > 1 {
		narrative.Subtitle = titleLines[1]
	}
	if narrative.Subtitle == "" {
		narrative.Subtitle = firstNonEmptyLine(sections["reporting period"])
	}
	if narrative.Title == "" {
		narrative.Title = "Inbox Snapshot"
	}

	if err := validateNarrative(narrative); err != nil {
		return SnapshotNarrative{}, err
	}
	return narrative, nil
}

// BuildSnapshotHTML renders a deterministic HTML snapshot from the normalized
// export model and explicit narrative sections.
func BuildSnapshotHTML(model *Model, narrative SnapshotNarrative, opts SnapshotOptions) ([]byte, error) {
	if model == nil {
		return nil, errors.New("model is required")
	}
	if err := validateNarrative(narrative); err != nil {
		return nil, err
	}

	generatedAt := opts.GeneratedAt
	if generatedAt.IsZero() {
		generatedAt = time.Now().UTC()
	}
	generatedAt = generatedAt.UTC()

	view := snapshotView{
		Title:             defaultString(narrative.Title, "Inbox Snapshot"),
		Subtitle:          defaultString(narrative.Subtitle, defaultSubtitle(model)),
		ReportingPeriod:   reportingPeriod(model),
		GeneratedDate:     generatedAt.Format("2006-01-02"),
		Headline:          narrative.Headline,
		SecondaryHeadline: narrative.SecondaryHeadline,
		Snapshot:          narrative.Snapshot,
		WhatThisMeans:     narrative.WhatThisMeans,
		Opportunities:     narrative.Opportunities,
		BottomLine:        narrative.BottomLine,
		TopSenders:        topSenders(model, 5),
		TopDomains:        topDomains(model, 5),
		TopSubjects:       topSubjects(model, 5),
	}

	var buf bytes.Buffer
	if err := snapshotHTMLTemplate.Execute(&buf, view); err != nil {
		return nil, fmt.Errorf("render snapshot html: %w", err)
	}
	return buf.Bytes(), nil
}

// BuildSnapshotPDF renders a snapshot PDF through the configured PDF renderer.
func BuildSnapshotPDF(model *Model, narrative SnapshotNarrative, opts SnapshotOptions, renderer PDFRenderer) ([]byte, error) {
	if renderer == nil {
		return nil, fmt.Errorf("%w: no renderer configured", ErrPDFRendererUnavailable)
	}
	html, err := BuildSnapshotHTML(model, narrative, opts)
	if err != nil {
		return nil, err
	}
	pdf, err := renderer.RenderPDF(html)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrPDFRendererUnavailable, err)
	}
	return pdf, nil
}

type snapshotView struct {
	Title             string
	Subtitle          string
	ReportingPeriod   string
	GeneratedDate     string
	Headline          string
	SecondaryHeadline string
	Snapshot          []string
	WhatThisMeans     []string
	Opportunities     []string
	BottomLine        string
	TopSenders        []SenderMetric
	TopDomains        []DomainMetric
	TopSubjects       []SubjectMetric
}

var snapshotHTMLTemplate = template.Must(template.New("snapshot").Parse(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{{.Title}}</title>
  <style>
    :root { color-scheme: light; }
    body { margin: 0; padding: 40px; background: #f6f5f2; color: #1f2328; font: 16px/1.5 Georgia, "Times New Roman", serif; }
    .page { max-width: 860px; margin: 0 auto; background: #ffffff; padding: 48px 56px; box-shadow: 0 20px 60px rgba(31,35,40,0.08); }
    h1, h2, h3, p { margin: 0; }
    .eyebrow { font: 600 12px/1.2 "Aptos", "Segoe UI", sans-serif; letter-spacing: 0.18em; text-transform: uppercase; color: #5c6773; }
    .header { margin-bottom: 28px; }
    .title { margin-top: 10px; font-size: 34px; line-height: 1.15; }
    .meta { margin-top: 12px; color: #5c6773; font: 500 14px/1.5 "Aptos", "Segoe UI", sans-serif; }
    .hero { display: grid; grid-template-columns: 1fr 1fr; gap: 18px; margin: 28px 0 32px; }
    .band { border-top: 3px solid #4c6a92; padding-top: 14px; }
    .band strong { display: block; margin-bottom: 8px; font: 600 12px/1.2 "Aptos", "Segoe UI", sans-serif; letter-spacing: 0.12em; text-transform: uppercase; color: #4c6a92; }
    .band p { font-size: 22px; line-height: 1.3; }
    section { margin-top: 30px; }
    section h2 { margin-bottom: 12px; padding-bottom: 8px; border-bottom: 1px solid #d7dde4; font: 600 14px/1.2 "Aptos", "Segoe UI", sans-serif; letter-spacing: 0.12em; text-transform: uppercase; color: #38424c; }
    ul { margin: 0; padding-left: 20px; }
    li + li { margin-top: 8px; }
    .summary-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 18px; margin-top: 18px; }
    .summary-card { background: #f8fafc; border: 1px solid #d7dde4; padding: 14px 16px; }
    .summary-card h3 { margin-bottom: 10px; font: 600 12px/1.2 "Aptos", "Segoe UI", sans-serif; letter-spacing: 0.1em; text-transform: uppercase; color: #4c6a92; }
    .summary-card ul { padding-left: 18px; }
    .bottom-line { margin-top: 30px; padding: 18px 22px; background: #eef3f8; border-left: 4px solid #4c6a92; }
    .bottom-line h2 { border: 0; margin-bottom: 8px; padding: 0; }
  </style>
</head>
<body>
  <main class="page">
    <header class="header">
      <div class="eyebrow">InboxAtlas Snapshot</div>
      <h1 class="title">{{.Title}}</h1>
      <p class="meta">{{.Subtitle}}</p>
      <p class="meta">Reporting Period: {{.ReportingPeriod}} | Generated: {{.GeneratedDate}}</p>
    </header>

    <section class="hero">
      <div class="band">
        <strong>Key Takeaway</strong>
        <p>{{.Headline}}</p>
      </div>
      <div class="band">
        <strong>Secondary Takeaway</strong>
        <p>{{.SecondaryHeadline}}</p>
      </div>
    </section>

    <section>
      <h2>Snapshot</h2>
      <ul>{{range .Snapshot}}<li>{{.}}</li>{{end}}</ul>
      <div class="summary-grid">
        <article class="summary-card">
          <h3>Top External Senders</h3>
          <ul>{{range .TopSenders}}<li>{{.Email}} ({{.Count}})</li>{{end}}</ul>
        </article>
        <article class="summary-card">
          <h3>Top External Domains</h3>
          <ul>{{range .TopDomains}}<li>{{.Domain}} ({{.Count}})</li>{{end}}</ul>
        </article>
        <article class="summary-card">
          <h3>Subject Themes</h3>
          <ul>{{range .TopSubjects}}<li>{{.Term}} ({{.Count}})</li>{{end}}</ul>
        </article>
      </div>
    </section>

    <section>
      <h2>What This Means</h2>
      <ul>{{range .WhatThisMeans}}<li>{{.}}</li>{{end}}</ul>
    </section>

    <section>
      <h2>Opportunities to Improve</h2>
      <ul>{{range .Opportunities}}<li>{{.}}</li>{{end}}</ul>
    </section>

    <section class="bottom-line">
      <h2>Bottom Line</h2>
      <p>{{.BottomLine}}</p>
    </section>
  </main>
</body>
</html>
`))

func splitMarkdownSections(markdown string) map[string]string {
	sections := map[string]string{"title": "", "subtitle": ""}
	current := "title"
	var builder strings.Builder

	flush := func() {
		sections[current] = strings.TrimSpace(builder.String())
		builder.Reset()
	}

	lines := strings.Split(markdown, "\n")
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "## ") {
			flush()
			current = strings.ToLower(strings.TrimSpace(strings.TrimPrefix(trimmed, "## ")))
			if _, ok := sections[current]; !ok {
				sections[current] = ""
			}
			continue
		}
		if strings.HasPrefix(trimmed, "# ") && current == "title" && builder.Len() == 0 {
			builder.WriteString(strings.TrimSpace(strings.TrimPrefix(trimmed, "# ")))
			builder.WriteByte('\n')
			continue
		}
		builder.WriteString(line)
		builder.WriteByte('\n')
	}
	flush()
	return sections
}

func parseBulletSection(body string) []string {
	items := make([]string, 0)
	for _, line := range strings.Split(body, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "- ") || strings.HasPrefix(trimmed, "* ") {
			items = append(items, strings.TrimSpace(trimmed[2:]))
		}
	}
	return items
}

func firstNonEmptyLine(body string) string {
	lines := nonEmptyLines(body)
	if len(lines) > 0 {
		return lines[0]
	}
	return ""
}

func nonEmptyLines(body string) []string {
	lines := make([]string, 0)
	for _, line := range strings.Split(body, "\n") {
		if trimmed := strings.TrimSpace(line); trimmed != "" {
			lines = append(lines, trimmed)
		}
	}
	return lines
}

func firstValue(lines []string, index int) string {
	if index >= 0 && index < len(lines) {
		return lines[index]
	}
	return ""
}

func validateNarrative(narrative SnapshotNarrative) error {
	switch {
	case strings.TrimSpace(narrative.Headline) == "":
		return fmt.Errorf("%w: missing key takeaway section", ErrSnapshotNarrativeRequired)
	case strings.TrimSpace(narrative.SecondaryHeadline) == "":
		return fmt.Errorf("%w: missing secondary takeaway section", ErrSnapshotNarrativeRequired)
	case len(narrative.Snapshot) == 0:
		return fmt.Errorf("%w: missing snapshot bullets", ErrSnapshotNarrativeRequired)
	case len(narrative.WhatThisMeans) == 0:
		return fmt.Errorf("%w: missing what this means section", ErrSnapshotNarrativeRequired)
	case len(narrative.Opportunities) == 0:
		return fmt.Errorf("%w: missing opportunities section", ErrSnapshotNarrativeRequired)
	case strings.TrimSpace(narrative.BottomLine) == "":
		return fmt.Errorf("%w: missing bottom line section", ErrSnapshotNarrativeRequired)
	default:
		return nil
	}
}

func defaultSubtitle(model *Model) string {
	if model.Owner.Email != "" {
		return model.Owner.Email
	}
	return "Unscoped reports"
}

func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
