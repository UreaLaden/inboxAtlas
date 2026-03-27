package export

import (
	"archive/zip"
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	styleDefault = iota
	styleHeader
	styleTitle
	styleLabel
	styleInteger
	stylePercent
	styleWrapped
)

// WorkbookOptions configures workbook generation for a normalized export model.
type WorkbookOptions struct {
	GeneratedAt time.Time
}

// BuildWorkbook renders model into a deterministic XLSX workbook byte slice.
func BuildWorkbook(model *Model, opts WorkbookOptions) ([]byte, error) {
	if model == nil {
		return nil, errors.New("model is required")
	}

	generatedAt := opts.GeneratedAt
	if generatedAt.IsZero() {
		generatedAt = time.Now().UTC()
	}
	generatedAt = generatedAt.UTC()

	sheets := buildWorkbookSheets(model, generatedAt)

	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	files := []workbookFile{
		{name: "[Content_Types].xml", data: contentTypesXML(len(sheets))},
		{name: "_rels/.rels", data: rootRelsXML()},
		{name: "docProps/app.xml", data: appPropsXML(sheets)},
		{name: "docProps/core.xml", data: corePropsXML(generatedAt)},
		{name: "xl/workbook.xml", data: workbookXML(sheets)},
		{name: "xl/_rels/workbook.xml.rels", data: workbookRelsXML(sheets)},
		{name: "xl/styles.xml", data: stylesXML()},
	}
	for i, sheet := range sheets {
		files = append(files, workbookFile{
			name: fmt.Sprintf("xl/worksheets/sheet%d.xml", i+1),
			data: worksheetXML(sheet),
		})
	}

	for _, file := range files {
		w, err := zw.Create(file.name)
		if err != nil {
			return nil, fmt.Errorf("create workbook part %s: %w", file.name, err)
		}
		if _, err := w.Write([]byte(file.data)); err != nil {
			return nil, fmt.Errorf("write workbook part %s: %w", file.name, err)
		}
	}
	if err := zw.Close(); err != nil {
		return nil, fmt.Errorf("close workbook: %w", err)
	}
	return buf.Bytes(), nil
}

type workbookFile struct {
	name string
	data string
}

type workbookSheet struct {
	name         string
	columns      []float64
	rows         []worksheetRow
	autoFilter   string
	freezeTopRow bool
}

type worksheetRow struct {
	index int
	cells []worksheetCell
}

type worksheetCell struct {
	column int
	kind   cellKind
	value  string
	style  int
}

type cellKind int

const (
	cellString cellKind = iota
	cellNumber
)

func buildWorkbookSheets(model *Model, generatedAt time.Time) []workbookSheet {
	return []workbookSheet{
		buildOverviewSheet(model, generatedAt),
		buildVolumeSheet(model),
		buildSendersSheet(model),
		buildDomainsSheet(model),
		buildSubjectsSheet(model),
		buildSourceDataSheet(model),
	}
}

func buildOverviewSheet(model *Model, generatedAt time.Time) workbookSheet {
	rows := []worksheetRow{
		row(1, strCell(1, "Inbox Snapshot", styleTitle)),
		row(2,
			strCell(1, "Owner", styleLabel),
			strCell(2, nonEmpty(model.Owner.Email, "unscoped reports"), styleDefault),
			strCell(4, "Generated", styleLabel),
			strCell(5, generatedAt.Format("2006-01-02 15:04 UTC"), styleDefault),
		),
		row(3,
			strCell(1, "Reporting Period", styleLabel),
			strCell(2, reportingPeriod(model), styleDefault),
			strCell(4, "Total Messages", styleLabel),
			numCell(5, model.Summary.TotalMessages, styleInteger),
		),
		row(5, strCell(1, "Summary", styleLabel)),
		row(6, strCell(1, "Metric", styleHeader), strCell(2, "Value", styleHeader)),
		row(7, strCell(1, "Top External Sender", styleDefault), strCell(2, nonEmpty(model.Summary.TopExternalSender, "n/a"), styleDefault)),
		row(8, strCell(1, "Top External Domain", styleDefault), strCell(2, nonEmpty(model.Summary.TopExternalDomain, "n/a"), styleDefault)),
		row(9, strCell(1, "Top Subject Theme", styleDefault), strCell(2, topSubject(model), styleDefault)),
		row(11, strCell(1, "Key Findings", styleLabel)),
	}

	findings := buildKeyFindings(model)
	for i, finding := range findings {
		rows = append(rows, row(12+i, strCell(1, "- "+finding, styleWrapped)))
	}

	rows = append(rows,
		row(17, strCell(1, "Top 5 External Senders", styleLabel), strCell(5, "Top 5 External Domains", styleLabel)),
		row(18,
			strCell(1, "Sender", styleHeader),
			strCell(2, "Domain", styleHeader),
			strCell(3, "Count", styleHeader),
			strCell(5, "Domain", styleHeader),
			strCell(6, "Count", styleHeader),
		),
	)
	for i, sender := range topSenders(model, 5) {
		rows = append(rows, row(19+i,
			strCell(1, sender.Email, styleDefault),
			strCell(2, sender.Domain, styleDefault),
			numCell(3, sender.Count, styleInteger),
		))
	}
	for i, domain := range topDomains(model, 5) {
		rows = append(rows, row(19+i,
			strCell(5, domain.Domain, styleDefault),
			numCell(6, domain.Count, styleInteger),
		))
	}

	rows = append(rows,
		row(26, strCell(1, "Top Subject Themes", styleLabel)),
		row(27, strCell(1, "Theme", styleHeader), strCell(2, "Count", styleHeader), strCell(3, "Percent of Total", styleHeader)),
	)
	for i, subject := range topSubjects(model, 5) {
		rows = append(rows, row(28+i,
			strCell(1, subject.Term, styleDefault),
			numCell(2, subject.Count, styleInteger),
			percentCell(3, subject.PercentOfTotal, stylePercent),
		))
	}

	return workbookSheet{
		name:    "Overview",
		columns: []float64{28, 20, 14, 4, 24, 14},
		rows:    rows,
	}
}

func buildVolumeSheet(model *Model) workbookSheet {
	rows := []worksheetRow{
		row(1,
			strCell(1, "Month", styleHeader),
			strCell(2, "Email Count", styleHeader),
			strCell(3, "Month-over-Month Change", styleHeader),
			strCell(4, "Percent Change", styleHeader),
			strCell(5, "Share of Total", styleHeader),
		),
	}
	for i, metric := range model.Volume {
		rows = append(rows, row(2+i,
			strCell(1, metric.Period, styleDefault),
			numCell(2, metric.Count, styleInteger),
			numCell(3, metric.MoMChange, styleInteger),
			percentCell(4, metric.MoMPercent, stylePercent),
			percentCell(5, metric.PercentOfTotal, stylePercent),
		))
	}
	return workbookSheet{
		name:         "Volume Trends",
		columns:      []float64{14, 14, 24, 16, 16},
		rows:         rows,
		autoFilter:   fmt.Sprintf("A1:E%d", max(1, len(model.Volume)+1)),
		freezeTopRow: true,
	}
}

func buildSendersSheet(model *Model) workbookSheet {
	rows := []worksheetRow{
		row(1,
			strCell(1, "Sender Email", styleHeader),
			strCell(2, "Sender Name", styleHeader),
			strCell(3, "Sender Domain", styleHeader),
			strCell(4, "Message Count", styleHeader),
			strCell(5, "Percent of Total", styleHeader),
			strCell(6, "Scope", styleHeader),
		),
	}
	for i, metric := range model.Senders {
		rows = append(rows, row(2+i,
			strCell(1, metric.Email, styleDefault),
			strCell(2, metric.Name, styleDefault),
			strCell(3, metric.Domain, styleDefault),
			numCell(4, metric.Count, styleInteger),
			percentCell(5, metric.PercentOfTotal, stylePercent),
			strCell(6, scopeLabel(metric.Internal), styleDefault),
		))
	}
	return workbookSheet{
		name:         "Top Senders",
		columns:      []float64{30, 22, 20, 14, 16, 12},
		rows:         rows,
		autoFilter:   fmt.Sprintf("A1:F%d", max(1, len(model.Senders)+1)),
		freezeTopRow: true,
	}
}

func buildDomainsSheet(model *Model) workbookSheet {
	rows := []worksheetRow{
		row(1,
			strCell(1, "Domain", styleHeader),
			strCell(2, "Message Count", styleHeader),
			strCell(3, "Percent of Total", styleHeader),
			strCell(4, "Scope", styleHeader),
		),
	}
	for i, metric := range model.Domains {
		rows = append(rows, row(2+i,
			strCell(1, metric.Domain, styleDefault),
			numCell(2, metric.Count, styleInteger),
			percentCell(3, metric.PercentOfTotal, stylePercent),
			strCell(4, scopeLabel(metric.Internal), styleDefault),
		))
	}
	return workbookSheet{
		name:         "Top Domains",
		columns:      []float64{26, 14, 16, 12},
		rows:         rows,
		autoFilter:   fmt.Sprintf("A1:D%d", max(1, len(model.Domains)+1)),
		freezeTopRow: true,
	}
}

func buildSubjectsSheet(model *Model) workbookSheet {
	rows := []worksheetRow{
		row(1,
			strCell(1, "Theme", styleHeader),
			strCell(2, "Frequency", styleHeader),
			strCell(3, "Percent of Total", styleHeader),
		),
	}
	for i, metric := range model.Subjects {
		rows = append(rows, row(2+i,
			strCell(1, metric.Term, styleDefault),
			numCell(2, metric.Count, styleInteger),
			percentCell(3, metric.PercentOfTotal, stylePercent),
		))
	}
	return workbookSheet{
		name:         "Subject Themes",
		columns:      []float64{26, 14, 16},
		rows:         rows,
		autoFilter:   fmt.Sprintf("A1:C%d", max(1, len(model.Subjects)+1)),
		freezeTopRow: true,
	}
}

func buildSourceDataSheet(model *Model) workbookSheet {
	rows := []worksheetRow{
		row(1,
			strCell(1, "Dataset", styleHeader),
			strCell(2, "Primary", styleHeader),
			strCell(3, "Secondary", styleHeader),
			strCell(4, "Tertiary", styleHeader),
			strCell(5, "Count", styleHeader),
			strCell(6, "Percent of Total", styleHeader),
			strCell(7, "Internal", styleHeader),
			strCell(8, "Month-over-Month Change", styleHeader),
			strCell(9, "Percent Change", styleHeader),
		),
	}
	rowIndex := 2
	for _, metric := range model.Senders {
		rows = append(rows, row(rowIndex,
			strCell(1, "sender", styleDefault),
			strCell(2, metric.Email, styleDefault),
			strCell(3, metric.Name, styleDefault),
			strCell(4, metric.Domain, styleDefault),
			numCell(5, metric.Count, styleInteger),
			percentCell(6, metric.PercentOfTotal, stylePercent),
			strCell(7, scopeLabel(metric.Internal), styleDefault),
		))
		rowIndex++
	}
	for _, metric := range model.Domains {
		rows = append(rows, row(rowIndex,
			strCell(1, "domain", styleDefault),
			strCell(2, metric.Domain, styleDefault),
			numCell(5, metric.Count, styleInteger),
			percentCell(6, metric.PercentOfTotal, stylePercent),
			strCell(7, scopeLabel(metric.Internal), styleDefault),
		))
		rowIndex++
	}
	for _, metric := range model.Subjects {
		rows = append(rows, row(rowIndex,
			strCell(1, "subject", styleDefault),
			strCell(2, metric.Term, styleDefault),
			numCell(5, metric.Count, styleInteger),
			percentCell(6, metric.PercentOfTotal, stylePercent),
		))
		rowIndex++
	}
	for _, metric := range model.Volume {
		rows = append(rows, row(rowIndex,
			strCell(1, "volume", styleDefault),
			strCell(2, metric.Period, styleDefault),
			numCell(5, metric.Count, styleInteger),
			percentCell(6, metric.PercentOfTotal, stylePercent),
			numCell(8, metric.MoMChange, styleInteger),
			percentCell(9, metric.MoMPercent, stylePercent),
		))
		rowIndex++
	}
	return workbookSheet{
		name:         "Source Data",
		columns:      []float64{14, 26, 22, 20, 14, 16, 12, 24, 16},
		rows:         rows,
		autoFilter:   fmt.Sprintf("A1:I%d", max(1, rowIndex-1)),
		freezeTopRow: true,
	}
}

func buildKeyFindings(model *Model) []string {
	findings := make([]string, 0, 4)
	if len(model.Volume) > 0 {
		findings = append(findings, fmt.Sprintf(
			"Volume spans %d months from %s to %s.",
			len(model.Volume),
			model.Summary.ReportingPeriodStart,
			model.Summary.ReportingPeriodEnd,
		))
	}
	if len(model.ExternalTopSenders) > 0 {
		top := model.ExternalTopSenders[0]
		findings = append(findings, fmt.Sprintf(
			"Top external sender %s accounts for %d messages.",
			top.Email,
			top.Count,
		))
	}
	if len(model.ExternalTopDomains) > 0 {
		top := model.ExternalTopDomains[0]
		findings = append(findings, fmt.Sprintf(
			"Top external domain %s accounts for %d messages.",
			top.Domain,
			top.Count,
		))
	}
	if len(model.Subjects) > 0 {
		top := model.Subjects[0]
		findings = append(findings, fmt.Sprintf(
			"Recurring subject theme %q appears %d times.",
			top.Term,
			top.Count,
		))
	}
	if len(findings) == 0 {
		findings = append(findings, "No report rows were available for workbook generation.")
	}
	return findings
}

func topSenders(model *Model, limit int) []SenderMetric {
	if len(model.ExternalTopSenders) < limit {
		limit = len(model.ExternalTopSenders)
	}
	return model.ExternalTopSenders[:limit]
}

func topDomains(model *Model, limit int) []DomainMetric {
	if len(model.ExternalTopDomains) < limit {
		limit = len(model.ExternalTopDomains)
	}
	return model.ExternalTopDomains[:limit]
}

func topSubjects(model *Model, limit int) []SubjectMetric {
	if len(model.Subjects) < limit {
		limit = len(model.Subjects)
	}
	return model.Subjects[:limit]
}

func topSubject(model *Model) string {
	if len(model.Subjects) == 0 {
		return "n/a"
	}
	return model.Subjects[0].Term
}

func reportingPeriod(model *Model) string {
	if model.Summary.ReportingPeriodStart == "" && model.Summary.ReportingPeriodEnd == "" {
		return "n/a"
	}
	if model.Summary.ReportingPeriodStart == model.Summary.ReportingPeriodEnd {
		return model.Summary.ReportingPeriodStart
	}
	return model.Summary.ReportingPeriodStart + " to " + model.Summary.ReportingPeriodEnd
}

func nonEmpty(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func scopeLabel(internal bool) string {
	if internal {
		return "internal"
	}
	return "external"
}

func row(index int, cells ...worksheetCell) worksheetRow {
	return worksheetRow{index: index, cells: cells}
}

func strCell(column int, value string, style int) worksheetCell {
	return worksheetCell{column: column, kind: cellString, value: value, style: style}
}

func numCell(column, value, style int) worksheetCell {
	return worksheetCell{column: column, kind: cellNumber, value: strconv.Itoa(value), style: style}
}

func percentCell(column int, value float64, style int) worksheetCell {
	return worksheetCell{
		column: column,
		kind:   cellNumber,
		value:  strconv.FormatFloat(value/100, 'f', -1, 64),
		style:  style,
	}
}

func contentTypesXML(sheetCount int) string {
	var sheets strings.Builder
	for i := 0; i < sheetCount; i++ {
		fmt.Fprintf(
			&sheets,
			`<Override PartName="/xl/worksheets/sheet%d.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>`,
			i+1,
		)
	}
	return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>` +
		`<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">` +
		`<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>` +
		`<Default Extension="xml" ContentType="application/xml"/>` +
		`<Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>` +
		`<Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>` +
		`<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>` +
		`<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>` +
		sheets.String() +
		`</Types>`
}

func rootRelsXML() string {
	return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>` +
		`<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">` +
		`<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>` +
		`<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>` +
		`<Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>` +
		`</Relationships>`
}

func appPropsXML(sheets []workbookSheet) string {
	var titles strings.Builder
	for _, sheet := range sheets {
		titles.WriteString(`<vt:lpstr>` + xmlEscaped(sheet.name) + `</vt:lpstr>`)
	}
	return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>` +
		`<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties" xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes">` +
		`<Application>InboxAtlas</Application>` +
		`<DocSecurity>0</DocSecurity>` +
		`<ScaleCrop>false</ScaleCrop>` +
		`<HeadingPairs><vt:vector size="2" baseType="variant"><vt:variant><vt:lpstr>Worksheets</vt:lpstr></vt:variant><vt:variant><vt:i4>` + strconv.Itoa(len(sheets)) + `</vt:i4></vt:variant></vt:vector></HeadingPairs>` +
		`<TitlesOfParts><vt:vector size="` + strconv.Itoa(len(sheets)) + `" baseType="lpstr">` + titles.String() + `</vt:vector></TitlesOfParts>` +
		`</Properties>`
}

func corePropsXML(generatedAt time.Time) string {
	timestamp := generatedAt.Format(time.RFC3339)
	return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>` +
		`<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:dcmitype="http://purl.org/dc/dcmitype/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">` +
		`<dc:creator>InboxAtlas</dc:creator>` +
		`<cp:lastModifiedBy>InboxAtlas</cp:lastModifiedBy>` +
		`<dcterms:created xsi:type="dcterms:W3CDTF">` + timestamp + `</dcterms:created>` +
		`<dcterms:modified xsi:type="dcterms:W3CDTF">` + timestamp + `</dcterms:modified>` +
		`</cp:coreProperties>`
}

func workbookXML(sheets []workbookSheet) string {
	var entries strings.Builder
	for i, sheet := range sheets {
		fmt.Fprintf(&entries,
			`<sheet name="%s" sheetId="%d" r:id="rId%d"/>`,
			xmlEscaped(sheet.name),
			i+1,
			i+1,
		)
	}
	return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>` +
		`<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">` +
		`<bookViews><workbookView xWindow="0" yWindow="0" windowWidth="16000" windowHeight="9000"/></bookViews>` +
		`<sheets>` + entries.String() + `</sheets>` +
		`</workbook>`
}

func workbookRelsXML(sheets []workbookSheet) string {
	var rels strings.Builder
	for i := range sheets {
		fmt.Fprintf(&rels,
			`<Relationship Id="rId%d" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet%d.xml"/>`,
			i+1,
			i+1,
		)
	}
	fmt.Fprintf(&rels,
		`<Relationship Id="rId%d" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>`,
		len(sheets)+1,
	)
	return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>` +
		`<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">` +
		rels.String() +
		`</Relationships>`
}

func stylesXML() string {
	return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>` +
		`<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">` +
		`<numFmts count="2"><numFmt numFmtId="164" formatCode="#,##0"/><numFmt numFmtId="165" formatCode="0.0%"/></numFmts>` +
		`<fonts count="3">` +
		`<font><sz val="11"/><color theme="1"/><name val="Aptos"/></font>` +
		`<font><b/><sz val="11"/><color theme="1"/><name val="Aptos"/></font>` +
		`<font><b/><sz val="16"/><color theme="1"/><name val="Aptos"/></font>` +
		`</fonts>` +
		`<fills count="3">` +
		`<fill><patternFill patternType="none"/></fill>` +
		`<fill><patternFill patternType="gray125"/></fill>` +
		`<fill><patternFill patternType="solid"><fgColor rgb="FFD9EAF7"/><bgColor indexed="64"/></patternFill></fill>` +
		`</fills>` +
		`<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>` +
		`<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>` +
		`<cellXfs count="7">` +
		`<xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>` +
		`<xf numFmtId="0" fontId="1" fillId="2" borderId="0" xfId="0" applyFont="1" applyFill="1"><alignment horizontal="center" vertical="center"/></xf>` +
		`<xf numFmtId="0" fontId="2" fillId="0" borderId="0" xfId="0" applyFont="1"/>` +
		`<xf numFmtId="0" fontId="1" fillId="0" borderId="0" xfId="0" applyFont="1"/>` +
		`<xf numFmtId="164" fontId="0" fillId="0" borderId="0" xfId="0" applyNumberFormat="1"><alignment horizontal="right"/></xf>` +
		`<xf numFmtId="165" fontId="0" fillId="0" borderId="0" xfId="0" applyNumberFormat="1"><alignment horizontal="right"/></xf>` +
		`<xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0" applyAlignment="1"><alignment wrapText="1" vertical="top"/></xf>` +
		`</cellXfs>` +
		`<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>` +
		`</styleSheet>`
}

func worksheetXML(sheet workbookSheet) string {
	var out strings.Builder
	out.WriteString(`<?xml version="1.0" encoding="UTF-8" standalone="yes"?>`)
	out.WriteString(`<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">`)
	if sheet.freezeTopRow {
		out.WriteString(`<sheetViews><sheetView workbookViewId="0"><pane ySplit="1" topLeftCell="A2" activePane="bottomLeft" state="frozen"/></sheetView></sheetViews>`)
	}
	if len(sheet.columns) > 0 {
		out.WriteString(`<cols>`)
		for i, width := range sheet.columns {
			fmt.Fprintf(&out, `<col min="%d" max="%d" width="%g" customWidth="1"/>`, i+1, i+1, width)
		}
		out.WriteString(`</cols>`)
	}
	out.WriteString(`<sheetData>`)
	for _, row := range sheet.rows {
		fmt.Fprintf(&out, `<row r="%d">`, row.index)
		for _, cell := range row.cells {
			out.WriteString(cellXML(row.index, cell))
		}
		out.WriteString(`</row>`)
	}
	out.WriteString(`</sheetData>`)
	if sheet.autoFilter != "" {
		out.WriteString(`<autoFilter ref="` + sheet.autoFilter + `"/>`)
	}
	out.WriteString(`</worksheet>`)
	return out.String()
}

func cellXML(rowIndex int, cell worksheetCell) string {
	ref := cellRef(cell.column, rowIndex)
	style := ""
	if cell.style > 0 {
		style = ` s="` + strconv.Itoa(cell.style) + `"`
	}
	if cell.kind == cellNumber {
		return `<c r="` + ref + `"` + style + `><v>` + cell.value + `</v></c>`
	}
	return `<c r="` + ref + `"` + style + ` t="inlineStr"><is><t xml:space="preserve">` + xmlEscaped(cell.value) + `</t></is></c>`
}

func cellRef(column, row int) string {
	return columnName(column) + strconv.Itoa(row)
}

func columnName(column int) string {
	name := ""
	for column > 0 {
		column--
		name = string(rune('A'+(column%26))) + name
		column /= 26
	}
	return name
}

func xmlEscaped(value string) string {
	var buf bytes.Buffer
	for _, r := range value {
		switch r {
		case '&':
			buf.WriteString("&amp;")
		case '<':
			buf.WriteString("&lt;")
		case '>':
			buf.WriteString("&gt;")
		case '"':
			buf.WriteString("&quot;")
		case '\'':
			buf.WriteString("&apos;")
		default:
			buf.WriteRune(r)
		}
	}
	return buf.String()
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
