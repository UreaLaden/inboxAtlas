// Package engine provides CLI-safe orchestration seams over lower-level
// InboxAtlas packages. It keeps Cobra handlers thin while preserving package
// boundaries.
package engine

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/UreaLaden/inboxatlas/internal/classification"
	"github.com/UreaLaden/inboxatlas/internal/config"
	"github.com/UreaLaden/inboxatlas/internal/storage"
	"github.com/UreaLaden/inboxatlas/pkg/models"
)

// ClassifyRunSummary describes one mailbox-scoped classification run.
type ClassifyRunSummary struct {
	MailboxID         string
	MessagesProcessed int
	Breakdown         []storage.ClassificationCount
	UnknownPct        float64
}

// ClassifySuggestion is a mailbox-scoped candidate classification seed shown
// to operators for review.
type ClassifySuggestion struct {
	ID           int64  `json:"id,omitempty"`
	MailboxID    string `json:"mailbox_id"`
	PatternType  string `json:"pattern_type"`
	PatternValue string `json:"pattern_value"`
	Category     string `json:"category"`
	Source       string `json:"source"`
	Priority     int    `json:"priority"`
}

// ClassifySuggestionsSummary describes the suggestion set for one mailbox.
type ClassifySuggestionsSummary struct {
	MailboxID   string
	Suggestions []ClassifySuggestion
}

// InferenceRunSummary reports the outcome of one mailbox-scoped inference run.
type InferenceRunSummary struct {
	MailboxID string `json:"mailbox_id"`
	Submitted int    `json:"submitted"`
	Persisted int    `json:"persisted"`
	High      int    `json:"high"`
	Medium    int    `json:"medium"`
	Low       int    `json:"low"`
	Rejected  int    `json:"rejected"`
}

// InferenceSuggestionsSummary describes the persisted AI inference suggestions
// available for operator review for one mailbox.
type InferenceSuggestionsSummary struct {
	MailboxID   string                `json:"mailbox_id"`
	Suggestions []InferenceSuggestion `json:"suggestions"`
}

// InferenceSuggestion is one persisted AI inference candidate rendered for CLI
// review and optional promotion.
type InferenceSuggestion struct {
	MessageID      string                    `json:"message_id"`
	PatternType    string                    `json:"pattern_type"`
	PatternValue   string                    `json:"pattern_value"`
	Category       string                    `json:"category"`
	Confidence     float64                   `json:"confidence"`
	ConfidenceBand string                    `json:"confidence_band"`
	ReviewRequired bool                      `json:"review_required"`
	Evidence       storage.InferenceEvidence `json:"evidence"`
}

// ClassificationSummary describes mailbox-scoped classification results for
// operator review.
type ClassificationSummary struct {
	MailboxID  string                        `json:"mailbox_id"`
	Total      int                           `json:"total"`
	Breakdown  []storage.ClassificationCount `json:"breakdown"`
	UnknownPct float64                       `json:"unknown_pct"`
}

// LabelStatsSummary describes mailbox-scoped Gmail label frequencies for
// manual operator review.
type LabelStatsSummary struct {
	MailboxID string               `json:"mailbox_id"`
	Labels    []storage.LabelCount `json:"labels"`
}

// LabelDomainEntry is one domain aggregate nested under a Gmail label in
// label-analysis output.
type LabelDomainEntry struct {
	Domain       string `json:"domain"`
	MessageCount int    `json:"message_count"`
}

// LabelDomainRow is one Gmail label with nested top-domain aggregates for
// operator review.
type LabelDomainRow struct {
	Label   string             `json:"label"`
	Name    string             `json:"name"`
	Domains []LabelDomainEntry `json:"domains"`
}

// LabelDomainSummary describes mailbox-scoped Gmail labels expanded into top
// domains for manual review.
type LabelDomainSummary struct {
	MailboxID string           `json:"mailbox_id"`
	Labels    []LabelDomainRow `json:"labels"`
}

// ClassifiedMessagesFilter constrains ListClassifiedMessages results.
type ClassifiedMessagesFilter struct {
	Category string     `json:"category,omitempty"`
	Intent   string     `json:"intent,omitempty"`
	Limit    int        `json:"limit,omitempty"`
	Since    *time.Time `json:"since,omitempty"`
}

// ClassifiedMessageRow is one per-message classification result for operator
// review or automation consumption.
type ClassifiedMessageRow struct {
	MessageID     string    `json:"message_id"`
	FromEmail     string    `json:"from_email"`
	Domain        string    `json:"domain"`
	Subject       string    `json:"subject"`
	ReceivedAt    time.Time `json:"received_at"`
	HasAttachment bool      `json:"has_attachment"`
	Category      string    `json:"category"`
	Intent        string    `json:"intent"`
	MatchedRule   string    `json:"matched_rule"`
}

// ClassifiedMessagesSummary is the result of ListClassifiedMessages.
type ClassifiedMessagesSummary struct {
	MailboxID string                   `json:"mailbox_id"`
	Filter    ClassifiedMessagesFilter `json:"filter,omitempty"`
	Messages  []ClassifiedMessageRow   `json:"messages"`
}

// PaymentBucketResult is one read-only payment bucket evaluation row for a
// classified message.
type PaymentBucketResult struct {
	MessageID   string    `json:"message_id"`
	ThreadID    string    `json:"thread_id"`
	FromEmail   string    `json:"from_email"`
	Domain      string    `json:"domain"`
	Subject     string    `json:"subject"`
	ReceivedAt  time.Time `json:"received_at"`
	Category    string    `json:"category"`
	Intent      string    `json:"intent"`
	Bucket      string    `json:"bucket"`
	MatchedRule string    `json:"matched_rule"`
}

// PaymentBucketOptions controls optional filtering and deduplication behaviour
// for EvaluatePaymentBuckets.
type PaymentBucketOptions struct {
	ExcludeCategories []string
	DedupeThreads     bool
}

// PaymentBucketEvalSummary is the result of EvaluatePaymentBuckets.
type PaymentBucketEvalSummary struct {
	MailboxID     string                `json:"mailbox_id"`
	TotalMessages int                   `json:"total_messages"`
	BucketedCount int                   `json:"bucketed_count"`
	Results       []PaymentBucketResult `json:"results"`
}

// PromoteSuggestionRequest identifies a mailbox bootstrap suggestion to promote
// into the active mailbox-scoped seed set.
type PromoteSuggestionRequest struct {
	PatternType  string
	PatternValue string
	Category     string
	Priority     int
	HasPriority  bool
}

// PromoteSuggestionResult reports the outcome of promoting a mailbox bootstrap
// suggestion into active mailbox-scoped seeds.
type PromoteSuggestionResult struct {
	MailboxID    string
	PatternType  string
	PatternValue string
	Category     string
	Source       string
	Priority     int
	Created      bool
}

// ListMailboxSeeds returns the active mailbox-scoped operator seeds for one
// mailbox and excludes global defaults from the result.
func ListMailboxSeeds(ctx context.Context, cfg config.Config, account string) ([]ClassifySuggestion, error) {
	st, mb, err := openResolvedStore(ctx, cfg, account)
	if err != nil {
		return nil, err
	}
	defer func() { _ = st.Close() }()

	seeds, err := st.ListSeeds(ctx, mb.ID)
	if err != nil {
		return nil, fmt.Errorf("list mailbox seeds: %w", err)
	}

	out := make([]ClassifySuggestion, 0, len(seeds))
	for _, seed := range seeds {
		if seed.MailboxID != mb.ID {
			continue
		}
		out = append(out, ClassifySuggestion{
			ID:           seed.ID,
			MailboxID:    seed.MailboxID,
			PatternType:  seed.PatternType,
			PatternValue: seed.PatternValue,
			Category:     seed.Category,
			Source:       seed.Source,
			Priority:     seed.Priority,
		})
	}
	return out, nil
}

// DeleteMailboxSeed deletes one active mailbox-scoped seed by ID and refuses
// to delete global default seeds through the mailbox operator path.
func DeleteMailboxSeed(ctx context.Context, cfg config.Config, account string, seedID int64) error {
	st, mb, err := openResolvedStore(ctx, cfg, account)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	seeds, err := st.ListSeeds(ctx, mb.ID)
	if err != nil {
		return fmt.Errorf("list mailbox seeds: %w", err)
	}

	for _, seed := range seeds {
		if seed.ID != seedID {
			continue
		}
		if seed.MailboxID == "" {
			return fmt.Errorf("seed %d is global and cannot be deleted through mailbox seed management", seedID)
		}
		if seed.MailboxID != mb.ID {
			break
		}
		if err := st.DeleteSeed(ctx, seedID); err != nil {
			return fmt.Errorf("delete mailbox seed: %w", err)
		}
		return nil
	}

	return fmt.Errorf("seed %d not found for mailbox %s", seedID, mb.ID)
}

// ClassificationCategories returns the supported deterministic taxonomy names
// in CLI display order.
func ClassificationCategories() []string {
	return []string{
		classification.CategoryInternal,
		classification.CategoryClient,
		classification.CategoryVendor,
		classification.CategoryGovernment,
		classification.CategorySystemGenerated,
		classification.CategoryNewsletterMarketing,
		classification.CategorySocial,
		classification.CategoryUnknown,
	}
}

// ClassificationIntents returns the supported deterministic intent names in
// CLI display order.
func ClassificationIntents() []string {
	return []string{
		classification.IntentInvoice,
		classification.IntentRequestForInformation,
	}
}

// ClassificationPatternTypes returns the supported deterministic pattern types
// in CLI display order.
func ClassificationPatternTypes() []string {
	return []string{
		classification.PatternDomain,
		classification.PatternSenderEmail,
		classification.PatternSenderPrefix,
		classification.PatternLabel,
		classification.PatternHasAttachment,
		classification.PatternSubjectTerm,
	}
}

// RunClassify executes mailbox-scoped classification for account using the
// existing classification runner.
func RunClassify(ctx context.Context, cfg config.Config, account string) (ClassifyRunSummary, error) {
	st, mb, err := openResolvedStore(ctx, cfg, account)
	if err != nil {
		return ClassifyRunSummary{}, err
	}
	defer func() { _ = st.Close() }()

	if err := ensureDefaultSeeds(ctx, st); err != nil {
		return ClassifyRunSummary{}, err
	}

	messages, err := st.ListMessageMetaByMailbox(ctx, mb.ID)
	if err != nil {
		return ClassifyRunSummary{}, err
	}
	if len(messages) == 0 {
		return ClassifyRunSummary{}, fmt.Errorf("no synced messages found for %s — run 'inboxatlas sync gmail --account %s' first", mb.ID, mb.ID)
	}

	if err := classification.RunMailboxClassification(ctx, st, mb.ID, messages); err != nil {
		return ClassifyRunSummary{}, err
	}

	breakdown, err := st.QueryClassificationsByMailbox(ctx, mb.ID)
	if err != nil {
		return ClassifyRunSummary{}, fmt.Errorf("query classifications by mailbox: %w", err)
	}

	total := 0
	unknown := 0
	for _, row := range breakdown {
		total += row.Count
		if row.Category == classification.CategoryUnknown {
			unknown += row.Count
		}
	}

	unknownPct := 0.0
	if total > 0 {
		unknownPct = float64(unknown) * 100 / float64(total)
	}

	return ClassifyRunSummary{
		MailboxID:         mb.ID,
		MessagesProcessed: len(messages),
		Breakdown:         breakdown,
		UnknownPct:        unknownPct,
	}, nil
}

// ListClassifySuggestions returns the mailbox bootstrap suggestions for account.
func ListClassifySuggestions(ctx context.Context, cfg config.Config, account string) (ClassifySuggestionsSummary, error) {
	st, mb, err := openResolvedStore(ctx, cfg, account)
	if err != nil {
		return ClassifySuggestionsSummary{}, err
	}
	defer func() { _ = st.Close() }()

	suggestions, err := classification.MailboxBootstrapSuggestions(ctx, st, mb.ID, 5)
	if err != nil {
		return ClassifySuggestionsSummary{}, err
	}

	return ClassifySuggestionsSummary{
		MailboxID:   mb.ID,
		Suggestions: toEngineSuggestions(suggestions),
	}, nil
}

// GetClassificationSummary returns mailbox-scoped classification totals,
// per-category counts, and the unknown percentage for operator review.
func GetClassificationSummary(ctx context.Context, cfg config.Config, account string) (ClassificationSummary, error) {
	st, mb, err := openResolvedStore(ctx, cfg, account)
	if err != nil {
		return ClassificationSummary{}, err
	}
	defer func() { _ = st.Close() }()

	breakdown, err := st.QueryClassificationsByMailbox(ctx, mb.ID)
	if err != nil {
		return ClassificationSummary{}, err
	}

	total := 0
	unknown := 0
	for _, row := range breakdown {
		total += row.Count
		if row.Category == classification.CategoryUnknown {
			unknown += row.Count
		}
	}

	unknownPct := 0.0
	if total > 0 {
		unknownPct = float64(unknown) * 100 / float64(total)
	}

	return ClassificationSummary{
		MailboxID:  mb.ID,
		Total:      total,
		Breakdown:  breakdown,
		UnknownPct: unknownPct,
	}, nil
}

// ListLabelStats returns mailbox-scoped Gmail label frequency rows to guide
// manual seed authoring.
func ListLabelStats(ctx context.Context, cfg config.Config, account string, minCount int) (LabelStatsSummary, error) {
	st, mb, err := openResolvedStore(ctx, cfg, account)
	if err != nil {
		return LabelStatsSummary{}, err
	}
	defer func() { _ = st.Close() }()

	labels, err := st.QueryLabelStatsByMailbox(ctx, mb.ID, minCount)
	if err != nil {
		return LabelStatsSummary{}, fmt.Errorf("query label stats: %w", err)
	}

	return LabelStatsSummary{
		MailboxID: mb.ID,
		Labels:    labels,
	}, nil
}

// ListLabelDomainStats returns mailbox-scoped Gmail label rows expanded into
// top domains to guide manual seed authoring.
func ListLabelDomainStats(ctx context.Context, cfg config.Config, account string, minCount, topN int) (LabelDomainSummary, error) {
	st, mb, err := openResolvedStore(ctx, cfg, account)
	if err != nil {
		return LabelDomainSummary{}, err
	}
	defer func() { _ = st.Close() }()

	rows, err := st.QueryLabelDomainStatsByMailbox(ctx, mb.ID, minCount, topN)
	if err != nil {
		return LabelDomainSummary{}, fmt.Errorf("query label domain stats: %w", err)
	}

	labels := make([]LabelDomainRow, 0)
	indexByLabel := make(map[string]int, len(rows))
	for _, row := range rows {
		idx, ok := indexByLabel[row.Label]
		if !ok {
			idx = len(labels)
			indexByLabel[row.Label] = idx
			labels = append(labels, LabelDomainRow{
				Label: row.Label,
				Name:  row.DisplayName,
			})
		}
		labels[idx].Domains = append(labels[idx].Domains, LabelDomainEntry{
			Domain:       row.Domain,
			MessageCount: row.MessageCount,
		})
	}

	return LabelDomainSummary{
		MailboxID: mb.ID,
		Labels:    labels,
	}, nil
}

// ListClassifiedMessages returns mailbox-scoped per-message classification rows
// with optional category and intent filtering for operator review and automation.
func ListClassifiedMessages(ctx context.Context, cfg config.Config, account string, filter ClassifiedMessagesFilter) (ClassifiedMessagesSummary, error) {
	st, mb, err := openResolvedStore(ctx, cfg, account)
	if err != nil {
		return ClassifiedMessagesSummary{}, err
	}
	defer func() { _ = st.Close() }()

	rows, err := st.QueryClassifiedMessages(ctx, mb.ID, storage.ClassifiedMessagesFilter{
		Category: filter.Category,
		Intent:   filter.Intent,
		Limit:    filter.Limit,
		Since:    derefTime(filter.Since),
	})
	if err != nil {
		return ClassifiedMessagesSummary{}, fmt.Errorf("query classified messages: %w", err)
	}

	out := make([]ClassifiedMessageRow, len(rows))
	for i, row := range rows {
		out[i] = ClassifiedMessageRow{
			MessageID:     row.MessageID,
			FromEmail:     row.FromEmail,
			Domain:        row.Domain,
			Subject:       row.Subject,
			ReceivedAt:    row.ReceivedAt,
			HasAttachment: row.HasAttachment,
			Category:      row.Category,
			Intent:        row.Intent,
			MatchedRule:   row.MatchedRule,
		}
	}

	return ClassifiedMessagesSummary{
		MailboxID: mb.ID,
		Filter:    filter,
		Messages:  out,
	}, nil
}

// EvaluatePaymentBuckets runs the deterministic payment bucket router against
// persisted classified-message rows for one mailbox without persisting any
// bucket output.
func EvaluatePaymentBuckets(ctx context.Context, cfg config.Config, account string, matchedOnly bool, opts PaymentBucketOptions) (PaymentBucketEvalSummary, error) {
	st, mb, err := openResolvedStore(ctx, cfg, account)
	if err != nil {
		return PaymentBucketEvalSummary{}, err
	}
	defer func() { _ = st.Close() }()

	rows, err := st.QueryClassifiedMessages(ctx, mb.ID, storage.ClassifiedMessagesFilter{
		Limit:             0,
		ExcludeCategories: opts.ExcludeCategories,
	})
	if err != nil {
		return PaymentBucketEvalSummary{}, fmt.Errorf("query classified messages: %w", err)
	}

	if opts.DedupeThreads {
		seen := make(map[string]struct{}, len(rows))
		deduped := rows[:0]
		for _, row := range rows {
			if row.ThreadID == "" {
				deduped = append(deduped, row)
				continue
			}
			if _, ok := seen[row.ThreadID]; !ok {
				seen[row.ThreadID] = struct{}{}
				deduped = append(deduped, row)
			}
		}
		rows = deduped
	}

	router := classification.NewRuleBasedPaymentBucketRouter(classification.DefaultPaymentBucketRules())
	results := make([]PaymentBucketResult, 0, len(rows))
	bucketed := 0

	for _, row := range rows {
		msg := models.MessageMeta{
			FromEmail: row.FromEmail,
			Domain:    row.Domain,
			Subject:   row.Subject,
		}
		bucket, matchedRule, err := router.AssignBucket(ctx, msg, row.Category, row.Intent)
		if err != nil {
			return PaymentBucketEvalSummary{}, fmt.Errorf("assign payment bucket for message %q: %w", row.MessageID, err)
		}
		if bucket != classification.BucketNone {
			bucketed++
		}
		if matchedOnly && bucket == classification.BucketNone {
			continue
		}
		results = append(results, PaymentBucketResult{
			MessageID:   row.MessageID,
			ThreadID:    row.ThreadID,
			FromEmail:   row.FromEmail,
			Domain:      row.Domain,
			Subject:     row.Subject,
			ReceivedAt:  row.ReceivedAt,
			Category:    row.Category,
			Intent:      row.Intent,
			Bucket:      bucket,
			MatchedRule: matchedRule,
		})
	}

	return PaymentBucketEvalSummary{
		MailboxID:     mb.ID,
		TotalMessages: len(rows),
		BucketedCount: bucketed,
		Results:       results,
	}, nil
}

func derefTime(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}

// RunInference executes mailbox-scoped AI inference over messages that remain
// unknown after deterministic classification and stages valid medium/high
// confidence candidates for operator review.
//
// batchSize controls how many messages are sent to the provider in each call.
// A value of 0 or less sends all unknown messages in a single call.
func RunInference(ctx context.Context, cfg config.Config, account, command string, args []string, batchSize int) (InferenceRunSummary, error) {
	st, mb, err := openResolvedStore(ctx, cfg, account)
	if err != nil {
		return InferenceRunSummary{}, err
	}
	defer func() { _ = st.Close() }()

	if err := ensureDefaultSeeds(ctx, st); err != nil {
		return InferenceRunSummary{}, err
	}

	messages, err := st.ListMessageMetaByMailbox(ctx, mb.ID)
	if err != nil {
		return InferenceRunSummary{}, fmt.Errorf("list mailbox messages: %w", err)
	}
	if len(messages) == 0 {
		return InferenceRunSummary{}, fmt.Errorf("no synced messages found for %s — run 'inboxatlas sync gmail --account %s' first", mb.ID, mb.ID)
	}

	if err := classification.RunMailboxClassification(ctx, st, mb.ID, messages); err != nil {
		return InferenceRunSummary{}, fmt.Errorf("run deterministic classification: %w", err)
	}

	deterministicCategories := make(map[string]string, len(messages))
	for _, msg := range messages {
		got, err := st.GetClassification(ctx, msg.ProviderID, mb.ID)
		if err != nil {
			return InferenceRunSummary{}, fmt.Errorf("get classification %s: %w", msg.ProviderID, err)
		}
		if got == nil {
			deterministicCategories[msg.ProviderID] = classification.CategoryUnknown
			continue
		}
		deterministicCategories[msg.ProviderID] = got.Category
	}

	senderCounts, err := st.QuerySenderStatsByMailbox(ctx, mb.ID, 1)
	if err != nil {
		return InferenceRunSummary{}, fmt.Errorf("query sender stats: %w", err)
	}
	domainCounts, err := st.QueryDomainStatsByMailbox(ctx, mb.ID, 1)
	if err != nil {
		return InferenceRunSummary{}, fmt.Errorf("query domain stats: %w", err)
	}

	senderCountByEmail := make(map[string]int, len(senderCounts))
	for _, row := range senderCounts {
		senderCountByEmail[strings.ToLower(row.Email)] = row.Count
	}
	domainCountByDomain := make(map[string]int, len(domainCounts))
	for _, row := range domainCounts {
		domainCountByDomain[strings.ToLower(row.Domain)] = row.Count
	}

	requests := make([]classification.InferenceRequest, 0, len(messages))
	requestByID := make(map[string]classification.InferenceRequest, len(messages))
	messageByID := make(map[string]models.MessageMeta, len(messages))
	for _, msg := range messages {
		messageByID[msg.ProviderID] = msg
		if deterministicCategories[msg.ProviderID] != classification.CategoryUnknown {
			continue
		}

		req := classification.InferenceRequest{
			MessageID:             msg.ProviderID,
			MailboxID:             mb.ID,
			FromEmail:             msg.FromEmail,
			FromName:              msg.FromName,
			Domain:                msg.Domain,
			Subject:               msg.Subject,
			Snippet:               msg.Snippet,
			Labels:                append([]string(nil), msg.Labels...),
			ReceivedAt:            msg.ReceivedAt.UTC().Format(time.RFC3339),
			SenderCount:           senderCountByEmail[strings.ToLower(msg.FromEmail)],
			DomainCount:           domainCountByDomain[strings.ToLower(msg.Domain)],
			DeterministicCategory: classification.CategoryUnknown,
		}
		requests = append(requests, req)
		requestByID[req.MessageID] = req
	}

	summary := InferenceRunSummary{
		MailboxID: mb.ID,
		Submitted: len(requests),
	}
	if len(requests) == 0 {
		return summary, nil
	}

	provider := classification.CommandInferenceProvider{Command: command, Args: args}

	effective := batchSize
	if effective <= 0 {
		effective = len(requests)
	}

	var allCandidates []classification.InferenceCandidate
	for i := 0; i < len(requests); i += effective {
		end := i + effective
		if end > len(requests) {
			end = len(requests)
		}
		batch := requests[i:end]
		got, err := provider.Infer(ctx, batch)
		if err != nil {
			return InferenceRunSummary{}, err
		}
		allCandidates = append(allCandidates, got...)
	}

	for _, candidate := range allCandidates {
		if err := classification.ValidateInferenceCandidate(candidate, requestByID); err != nil {
			summary.Rejected++
			continue
		}

		switch candidate.ConfidenceBand {
		case "high":
			summary.High++
		case "medium":
			summary.Medium++
		default:
			summary.Low++
			continue
		}

		msg := messageByID[candidate.MessageID]
		patternType, patternValue, ok := inferencePatternForMessage(msg)
		if !ok {
			summary.Rejected++
			continue
		}

		if err := st.SaveInferenceCandidate(ctx, storage.InferenceSuggestion{
			MailboxID:      mb.ID,
			MessageID:      candidate.MessageID,
			PatternType:    patternType,
			PatternValue:   patternValue,
			Category:       candidate.Category,
			Confidence:     candidate.Confidence,
			ConfidenceBand: candidate.ConfidenceBand,
			Evidence:       toStorageEvidence(candidate.Evidence),
			ReviewRequired: candidate.ReviewRequired,
		}); err != nil {
			return InferenceRunSummary{}, fmt.Errorf("save inference candidate %s: %w", candidate.MessageID, err)
		}
		summary.Persisted++
	}

	return summary, nil
}

// ListInferenceSuggestions returns persisted AI inference suggestions for one
// mailbox in operator review order.
func ListInferenceSuggestions(ctx context.Context, cfg config.Config, account string) (InferenceSuggestionsSummary, error) {
	st, mb, err := openResolvedStore(ctx, cfg, account)
	if err != nil {
		return InferenceSuggestionsSummary{}, err
	}
	defer func() { _ = st.Close() }()

	suggestions, err := st.ListInferenceSuggestions(ctx, mb.ID)
	if err != nil {
		return InferenceSuggestionsSummary{}, fmt.Errorf("list inference suggestions: %w", err)
	}

	out := make([]InferenceSuggestion, len(suggestions))
	for i, suggestion := range suggestions {
		out[i] = InferenceSuggestion{
			MessageID:      suggestion.MessageID,
			PatternType:    suggestion.PatternType,
			PatternValue:   suggestion.PatternValue,
			Category:       suggestion.Category,
			Confidence:     suggestion.Confidence,
			ConfidenceBand: suggestion.ConfidenceBand,
			ReviewRequired: suggestion.ReviewRequired,
			Evidence:       suggestion.Evidence,
		}
	}

	return InferenceSuggestionsSummary{MailboxID: mb.ID, Suggestions: out}, nil
}

// PromoteClassifySuggestion validates a mailbox bootstrap suggestion and
// persists it as an active mailbox-scoped operator seed.
func PromoteClassifySuggestion(ctx context.Context, cfg config.Config, account string, req PromoteSuggestionRequest) (PromoteSuggestionResult, error) {
	if req.PatternType == "" || req.PatternValue == "" || req.Category == "" {
		return PromoteSuggestionResult{}, fmt.Errorf("pattern type, pattern value, and category are required")
	}

	st, mb, err := openResolvedStore(ctx, cfg, account)
	if err != nil {
		return PromoteSuggestionResult{}, err
	}
	defer func() { _ = st.Close() }()

	suggestion, ok, err := findSuggestion(ctx, st, mb.ID, req.PatternType, req.PatternValue)
	if err != nil {
		return PromoteSuggestionResult{}, err
	}
	if !ok {
		return PromoteSuggestionResult{}, fmt.Errorf("suggestion not found for mailbox %s: %s:%s", mb.ID, req.PatternType, req.PatternValue)
	}

	priority := suggestion.Priority
	if req.HasPriority {
		priority = req.Priority
	}

	activeSeeds, err := st.ListSeeds(ctx, mb.ID)
	if err != nil {
		return PromoteSuggestionResult{}, fmt.Errorf("list active seeds: %w", err)
	}

	for _, seed := range activeSeeds {
		if seed.MailboxID != mb.ID {
			continue
		}
		if seed.PatternType != req.PatternType || seed.PatternValue != req.PatternValue {
			continue
		}

		if seed.Category == req.Category && seed.Source == classification.SourceOperator && seed.Priority == priority {
			return PromoteSuggestionResult{
				MailboxID:    mb.ID,
				PatternType:  seed.PatternType,
				PatternValue: seed.PatternValue,
				Category:     seed.Category,
				Source:       seed.Source,
				Priority:     seed.Priority,
				Created:      false,
			}, nil
		}

		return PromoteSuggestionResult{}, fmt.Errorf("mailbox seed already exists for %s:%s with different attributes", req.PatternType, req.PatternValue)
	}

	if err := st.InsertSeed(ctx, storage.ClassificationSeed{
		MailboxID:    mb.ID,
		PatternType:  req.PatternType,
		PatternValue: req.PatternValue,
		Category:     req.Category,
		Source:       classification.SourceOperator,
		Priority:     priority,
	}); err != nil {
		return PromoteSuggestionResult{}, fmt.Errorf("promote suggestion: %w", err)
	}

	return PromoteSuggestionResult{
		MailboxID:    mb.ID,
		PatternType:  req.PatternType,
		PatternValue: req.PatternValue,
		Category:     req.Category,
		Source:       classification.SourceOperator,
		Priority:     priority,
		Created:      true,
	}, nil
}

// SubjectEvalRule is the engine-layer representation of a subject keyword
// classification rule. It mirrors classification.SubjectRule but is defined
// here so that callers (including the CLI) do not need to import the
// classification package directly.
type SubjectEvalRule struct {
	// IncludeKeywords specifies keywords of which at least one must appear in
	// the normalized subject (OR semantics, case-insensitive, whole-token).
	IncludeKeywords []string
	// ExcludeKeywords specifies keywords none of which may appear in the
	// normalized subject (case-insensitive, whole-token).
	ExcludeKeywords []string
	// Category is the taxonomy constant assigned when this rule matches.
	Category string
	// Priority controls evaluation order (lower = evaluated first).
	Priority int
}

// SubjectEvalResult is one message evaluation row from EvaluateSubjectRules.
type SubjectEvalResult struct {
	MessageID      string `json:"message_id"`
	Subject        string `json:"subject"`
	Normalized     string `json:"normalized"`
	Matched        bool   `json:"matched"`
	Category       string `json:"category"`
	MatchedRule    string `json:"matched_rule"`
	Excluded       bool   `json:"excluded"`
	ExcludedReason string `json:"excluded_reason,omitempty"`
}

// SubjectEvalSummary is the result of EvaluateSubjectRules.
type SubjectEvalSummary struct {
	MailboxID     string              `json:"mailbox_id"`
	TotalMessages int                 `json:"total_messages"`
	MatchedCount  int                 `json:"matched_count"`
	ExcludedCount int                 `json:"excluded_count"`
	Results       []SubjectEvalResult `json:"results"`
}

// EvaluateSubjectRules runs a dry-run subject-rule evaluation against all
// synced messages for account. No classifications are persisted — this is a
// read-only operator tool. Rules are passed directly by the caller rather than
// loaded from storage.
//
// excludeCategories is an optional list of persisted category values. Messages
// whose persisted classification matches any entry are marked excluded
// (Excluded: true, ExcludedReason: "category:<name>") and do not count toward
// MatchedCount. Messages with no persisted classification row are never excluded.
//
// Note on output volume: ListMessageMetaByMailbox returns all synced messages.
// For large mailboxes this may produce a large in-memory result set. This is
// acceptable for a dry-run operator tool in v1.
//
// If both a PatternSubjectTerm seed (via SeedRuleClassifier) and a
// SubjectRuleClassifier rule are active in a future classify run, the
// ChainClassifier priority/chain order governs which result wins.
func EvaluateSubjectRules(
	ctx context.Context,
	cfg config.Config,
	account string,
	rules []SubjectEvalRule,
	excludeCategories []string,
) (SubjectEvalSummary, error) {
	st, mb, err := openResolvedStore(ctx, cfg, account)
	if err != nil {
		return SubjectEvalSummary{}, err
	}
	defer func() { _ = st.Close() }()

	messages, err := st.ListMessageMetaByMailbox(ctx, mb.ID)
	if err != nil {
		return SubjectEvalSummary{}, fmt.Errorf("list messages: %w", err)
	}

	// Load persisted classifications for category-exclusion checks.
	var persistedCategories map[string]string
	if len(excludeCategories) > 0 {
		persistedCategories, err = st.ListMessageClassificationsByMailbox(ctx, mb.ID)
		if err != nil {
			return SubjectEvalSummary{}, fmt.Errorf("load persisted classifications: %w", err)
		}
	}

	// Build a set of excluded category values for O(1) lookup.
	excludeSet := make(map[string]struct{}, len(excludeCategories))
	for _, cat := range excludeCategories {
		excludeSet[cat] = struct{}{}
	}

	classRules := make([]classification.SubjectRule, len(rules))
	for i, r := range rules {
		classRules[i] = classification.SubjectRule{
			IncludeKeywords: append([]string(nil), r.IncludeKeywords...),
			ExcludeKeywords: append([]string(nil), r.ExcludeKeywords...),
			Category:        r.Category,
			Priority:        r.Priority,
		}
	}
	classifier := classification.NewSubjectRuleClassifier(classRules)
	results := make([]SubjectEvalResult, 0, len(messages))
	matched := 0
	excluded := 0

	for _, msg := range messages {
		norm := classification.NormalizeSubject(msg.Subject)

		// Check category exclusion before running the subject rule.
		var isExcluded bool
		var excludedReason string
		if len(excludeSet) > 0 {
			if persistedCat, ok := persistedCategories[msg.ProviderID]; ok {
				if _, excluded := excludeSet[persistedCat]; excluded {
					isExcluded = true
					excludedReason = "category:" + persistedCat
				}
			}
		}

		result, err := classifier.Classify(ctx, msg)
		if err != nil {
			return SubjectEvalSummary{}, fmt.Errorf("evaluate message %q: %w", msg.ProviderID, err)
		}
		isMatch := result.Category != classification.CategoryUnknown
		if isMatch && !isExcluded {
			matched++
		}
		if isExcluded {
			excluded++
		}
		results = append(results, SubjectEvalResult{
			MessageID:      msg.ProviderID,
			Subject:        msg.Subject,
			Normalized:     norm,
			Matched:        isMatch,
			Category:       result.Category,
			MatchedRule:    result.MatchedRule,
			Excluded:       isExcluded,
			ExcludedReason: excludedReason,
		})
	}

	return SubjectEvalSummary{
		MailboxID:     mb.ID,
		TotalMessages: len(messages),
		MatchedCount:  matched,
		ExcludedCount: excluded,
		Results:       results,
	}, nil
}

func openResolvedStore(ctx context.Context, cfg config.Config, account string) (*storage.Store, *models.Mailbox, error) {
	st, err := storage.Open(cfg.StoragePath)
	if err != nil {
		return nil, nil, fmt.Errorf("open storage: %w", err)
	}

	mb, err := storage.ResolveMailbox(ctx, st, account)
	if err != nil {
		_ = st.Close()
		return nil, nil, err
	}
	return st, mb, nil
}

func ensureDefaultSeeds(ctx context.Context, st *storage.Store) error {
	for _, seed := range classification.DefaultSeeds() {
		if err := st.UpsertSeed(ctx, storage.ClassificationSeed{
			PatternType:  seed.PatternType,
			PatternValue: seed.PatternValue,
			Category:     seed.Category,
			Source:       seed.Source,
			Priority:     seed.Priority,
		}); err != nil {
			return fmt.Errorf("upsert default seed %s:%s: %w", seed.PatternType, seed.PatternValue, err)
		}
	}
	return nil
}

func toEngineSuggestions(seeds []classification.ClassificationSeed) []ClassifySuggestion {
	out := make([]ClassifySuggestion, len(seeds))
	for i, seed := range seeds {
		out[i] = ClassifySuggestion{
			MailboxID:    seed.MailboxID,
			PatternType:  seed.PatternType,
			PatternValue: seed.PatternValue,
			Category:     seed.Category,
			Source:       seed.Source,
			Priority:     seed.Priority,
		}
	}
	return out
}

func findSuggestion(ctx context.Context, st *storage.Store, mailboxID, patternType, patternValue string) (classification.ClassificationSeed, bool, error) {
	suggestions, err := classification.MailboxBootstrapSuggestions(ctx, st, mailboxID, 5)
	if err != nil {
		return classification.ClassificationSeed{}, false, fmt.Errorf("list mailbox suggestions: %w", err)
	}
	for _, suggestion := range suggestions {
		if suggestion.PatternType == patternType && suggestion.PatternValue == patternValue {
			return suggestion, true, nil
		}
	}
	inferenceSuggestions, err := st.ListInferenceSuggestions(ctx, mailboxID)
	if err != nil {
		return classification.ClassificationSeed{}, false, fmt.Errorf("list inference suggestions: %w", err)
	}
	for _, suggestion := range inferenceSuggestions {
		if suggestion.PatternType == patternType && suggestion.PatternValue == patternValue {
			return classification.ClassificationSeed{
				MailboxID:    suggestion.MailboxID,
				PatternType:  suggestion.PatternType,
				PatternValue: suggestion.PatternValue,
				Category:     suggestion.Category,
				Source:       classification.SourceAI,
				Priority:     100,
			}, true, nil
		}
	}
	return classification.ClassificationSeed{}, false, nil
}

func inferencePatternForMessage(msg models.MessageMeta) (string, string, bool) {
	if value := strings.TrimSpace(strings.ToLower(msg.Domain)); value != "" {
		return classification.PatternDomain, value, true
	}
	if value := strings.TrimSpace(strings.ToLower(msg.FromEmail)); value != "" {
		return classification.PatternSenderEmail, value, true
	}
	return "", "", false
}

func toStorageEvidence(e classification.InferenceEvidence) storage.InferenceEvidence {
	return storage.InferenceEvidence{
		SubjectPhrases: append([]string(nil), e.SubjectPhrases...),
		SnippetPhrases: append([]string(nil), e.SnippetPhrases...),
		SenderSignal:   e.SenderSignal,
		DomainSignal:   e.DomainSignal,
		LabelSignals:   append([]string(nil), e.LabelSignals...),
	}
}
