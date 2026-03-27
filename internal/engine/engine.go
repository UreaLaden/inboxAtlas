// Package engine provides CLI-safe orchestration seams over lower-level
// InboxAtlas packages. It keeps Cobra handlers thin while preserving package
// boundaries.
package engine

import (
	"context"
	"fmt"

	"github.com/UreaLaden/inboxatlas/internal/classification"
	"github.com/UreaLaden/inboxatlas/internal/config"
	"github.com/UreaLaden/inboxatlas/internal/storage"
	"github.com/UreaLaden/inboxatlas/pkg/models"
)

// ClassifyRunSummary describes one mailbox-scoped classification run.
type ClassifyRunSummary struct {
	MailboxID         string
	MessagesProcessed int
}

// ClassifySuggestion is a mailbox-scoped candidate classification seed shown
// to operators for review.
type ClassifySuggestion struct {
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

	return ClassifyRunSummary{
		MailboxID:         mb.ID,
		MessagesProcessed: len(messages),
	}, nil
}

// ListClassifySuggestions returns the mailbox bootstrap suggestions for account.
func ListClassifySuggestions(ctx context.Context, cfg config.Config, account string) (ClassifySuggestionsSummary, error) {
	st, mb, err := openResolvedStore(ctx, cfg, account)
	if err != nil {
		return ClassifySuggestionsSummary{}, err
	}
	defer func() { _ = st.Close() }()

	return ClassifySuggestionsSummary{
		MailboxID:   mb.ID,
		Suggestions: toEngineSuggestions(classification.MailboxBootstrapSuggestions(mb.ID)),
	}, nil
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

	suggestion, ok := findSuggestion(mb.ID, req.PatternType, req.PatternValue, req.Category)
	if !ok {
		return PromoteSuggestionResult{}, fmt.Errorf("suggestion not found for mailbox %s: %s:%s (%s)", mb.ID, req.PatternType, req.PatternValue, req.Category)
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
		if err := st.InsertSeed(ctx, storage.ClassificationSeed{
			PatternType:  seed.PatternType,
			PatternValue: seed.PatternValue,
			Category:     seed.Category,
			Source:       seed.Source,
			Priority:     seed.Priority,
		}); err != nil {
			return fmt.Errorf("insert default seed %s:%s: %w", seed.PatternType, seed.PatternValue, err)
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

func findSuggestion(mailboxID, patternType, patternValue, category string) (classification.ClassificationSeed, bool) {
	for _, suggestion := range classification.MailboxBootstrapSuggestions(mailboxID) {
		if suggestion.PatternType == patternType && suggestion.PatternValue == patternValue && suggestion.Category == category {
			return suggestion, true
		}
	}
	return classification.ClassificationSeed{}, false
}
