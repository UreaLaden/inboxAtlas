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

// ClassifiedMessagesFilter constrains ListClassifiedMessages results.
type ClassifiedMessagesFilter struct {
	Category string `json:"category,omitempty"`
	Intent   string `json:"intent,omitempty"`
	Limit    int    `json:"limit,omitempty"`
}

// ClassifiedMessageRow is one per-message classification result for operator
// review or automation consumption.
type ClassifiedMessageRow struct {
	MessageID   string    `json:"message_id"`
	FromEmail   string    `json:"from_email"`
	Domain      string    `json:"domain"`
	Subject     string    `json:"subject"`
	ReceivedAt  time.Time `json:"received_at"`
	Category    string    `json:"category"`
	Intent      string    `json:"intent"`
	MatchedRule string    `json:"matched_rule"`
}

// ClassifiedMessagesSummary is the result of ListClassifiedMessages.
type ClassifiedMessagesSummary struct {
	MailboxID string                   `json:"mailbox_id"`
	Filter    ClassifiedMessagesFilter `json:"filter,omitempty"`
	Messages  []ClassifiedMessageRow   `json:"messages"`
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
	})
	if err != nil {
		return ClassifiedMessagesSummary{}, fmt.Errorf("query classified messages: %w", err)
	}

	out := make([]ClassifiedMessageRow, len(rows))
	for i, row := range rows {
		out[i] = ClassifiedMessageRow{
			MessageID:   row.MessageID,
			FromEmail:   row.FromEmail,
			Domain:      row.Domain,
			Subject:     row.Subject,
			ReceivedAt:  row.ReceivedAt,
			Category:    row.Category,
			Intent:      row.Intent,
			MatchedRule: row.MatchedRule,
		}
	}

	return ClassifiedMessagesSummary{
		MailboxID: mb.ID,
		Filter:    filter,
		Messages:  out,
	}, nil
}

// RunInference executes mailbox-scoped AI inference over messages that remain
// unknown after deterministic classification and stages valid medium/high
// confidence candidates for operator review.
func RunInference(ctx context.Context, cfg config.Config, account, command string, args []string) (InferenceRunSummary, error) {
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
	candidates, err := provider.Infer(ctx, requests)
	if err != nil {
		return InferenceRunSummary{}, err
	}

	for _, candidate := range candidates {
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
