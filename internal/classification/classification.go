// Package classification provides the deterministic rules-based message classification
// engine for InboxAtlas. All classification logic is delivered through the Classifier
// interface, which is designed to be swappable so that future AI-backed classifiers
// can slot in without changing callsites.
package classification

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/UreaLaden/inboxatlas/internal/storage"
	"github.com/UreaLaden/inboxatlas/pkg/models"
)

// Category constants define the supported message taxonomy.
const (
	// CategoryInternal covers messages from within the same organization.
	CategoryInternal = "internal"
	// CategoryClient covers messages from known client contacts or domains.
	CategoryClient = "client"
	// CategoryVendor covers messages from service providers and vendors.
	CategoryVendor = "vendor"
	// CategoryGovernment covers messages from government agencies.
	CategoryGovernment = "government"
	// CategorySystemGenerated covers automated system notifications.
	CategorySystemGenerated = "system-generated"
	// CategoryNewsletterMarketing covers newsletters, marketing, and promotional mail.
	CategoryNewsletterMarketing = "newsletter/marketing"
	// CategorySocial covers social network notifications (LinkedIn, Facebook, etc.).
	// Added based on corpus analysis — not in spec §16.1 but clearly warranted.
	CategorySocial = "social"
	// CategoryUnknown is the fallback for messages that match no rule.
	CategoryUnknown = "unknown"
)

// Pattern type constants define how a ClassificationSeed's PatternValue is matched.
const (
	// PatternDomain matches when msg.Domain equals the seed value (case-insensitive).
	PatternDomain = "domain"
	// PatternSenderEmail matches when msg.FromEmail equals the seed value (case-insensitive).
	PatternSenderEmail = "sender_email"
	// PatternSenderPrefix matches when the local part of msg.FromEmail has the seed value as prefix.
	PatternSenderPrefix = "sender_prefix"
	// PatternSubjectTerm matches when the lowercased tokenized subject contains the seed value.
	PatternSubjectTerm = "subject_term"
)

// Source constants identify the origin of a classification seed.
const (
	// SourceSeed identifies a built-in corpus-grounded seed from DefaultSeeds().
	SourceSeed = "seed"
	// SourceOperator identifies an operator-added seed.
	SourceOperator = "operator"
	// SourceAI identifies an AI-suggested seed. AI seeds require operator promotion
	// (source → "operator") before governing production classification.
	SourceAI = "ai"
)

// ClassificationResult holds the output of a single Classifier.Classify call.
type ClassificationResult struct {
	// Category is the assigned taxonomy category. "unknown" when no rule matches.
	Category string
	// MatchedRule is a human-readable description of the rule that produced this result
	// (§4.5 explainability). Non-empty on every non-unknown result.
	// Format: "<pattern_type>:<pattern_value>".
	MatchedRule string
	// Source identifies which classification system produced this result.
	Source string
}

// ClassificationSeed is a single classification rule evaluated by SeedRuleClassifier.
// This type mirrors storage.ClassificationSeed for package independence.
type ClassificationSeed struct {
	ID           int64
	MailboxID    string // empty = global (applies to all mailboxes)
	PatternType  string // PatternDomain, PatternSenderEmail, PatternSenderPrefix, PatternSubjectTerm
	PatternValue string // e.g. "facebookmail.com", "noreply"
	Category     string // taxonomy constant
	Source       string // SourceSeed, SourceOperator, SourceAI
	Priority     int    // lower = evaluated first; default 100
	CreatedAt    time.Time
}

// Classifier is the interface all classification backends must implement. It is
// the primary extensibility seam for plugging in different classification strategies
// (seed rules, AI, operator overrides) without changing call sites.
type Classifier interface {
	// Classify returns a ClassificationResult for msg. An unknown category result
	// (Category == CategoryUnknown) signals that this classifier could not match msg.
	Classify(ctx context.Context, msg models.MessageMeta) (ClassificationResult, error)
}

// InferenceRequest is the mailbox-scoped deterministic payload supplied to an
// AI inference provider for one still-unknown message.
type InferenceRequest struct {
	MessageID             string   `json:"message_id"`
	MailboxID             string   `json:"mailbox_id"`
	FromEmail             string   `json:"from_email"`
	FromName              string   `json:"from_name"`
	Domain                string   `json:"domain"`
	Subject               string   `json:"subject"`
	Snippet               string   `json:"snippet"`
	Labels                []string `json:"labels"`
	ReceivedAt            string   `json:"received_at"`
	SenderCount           int      `json:"sender_count"`
	DomainCount           int      `json:"domain_count"`
	DeterministicCategory string   `json:"deterministic_category"`
}

// InferenceEvidence captures the structured rationale returned by an AI
// inference provider for operator review.
type InferenceEvidence struct {
	SubjectPhrases []string `json:"subject_phrases"`
	SnippetPhrases []string `json:"snippet_phrases"`
	SenderSignal   string   `json:"sender_signal"`
	DomainSignal   string   `json:"domain_signal"`
	LabelSignals   []string `json:"label_signals"`
}

// InferenceCandidate is one provider-returned classification proposal for a
// message that remained unknown after deterministic classification.
type InferenceCandidate struct {
	MessageID      string            `json:"message_id"`
	Category       string            `json:"category"`
	Confidence     float64           `json:"confidence"`
	ConfidenceBand string            `json:"confidence_band"`
	Evidence       InferenceEvidence `json:"evidence"`
	ReviewRequired bool              `json:"review_required"`
}

// InferenceProvider returns structured inference candidates for a mailbox
// batch of still-unknown messages.
type InferenceProvider interface {
	Infer(ctx context.Context, requests []InferenceRequest) ([]InferenceCandidate, error)
}

// CommandInferenceProvider invokes an external command that accepts a JSON
// inference request payload on stdin and returns structured candidates on
// stdout.
type CommandInferenceProvider struct {
	Command string
	Args    []string
}

type inferenceProviderRequest struct {
	Requests []InferenceRequest `json:"requests"`
}

// Infer executes the configured command-backed provider and decodes structured
// inference candidates from stdout.
func (p CommandInferenceProvider) Infer(ctx context.Context, requests []InferenceRequest) ([]InferenceCandidate, error) {
	if strings.TrimSpace(p.Command) == "" {
		return nil, fmt.Errorf("inference provider command is required")
	}

	reqBody, err := json.Marshal(inferenceProviderRequest{Requests: requests})
	if err != nil {
		return nil, fmt.Errorf("marshal inference request: %w", err)
	}

	cmd := exec.CommandContext(ctx, p.Command, p.Args...)
	cmd.Stdin = bytes.NewReader(reqBody)
	var stderr bytes.Buffer
	cmd.Stderr = io.MultiWriter(os.Stderr, &stderr)
	output, err := cmd.Output()
	if err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			return nil, fmt.Errorf("run inference provider: %s", strings.TrimSpace(stderr.String()))
		}
		return nil, fmt.Errorf("run inference provider: %w", err)
	}

	var candidates []InferenceCandidate
	if err := json.Unmarshal(output, &candidates); err != nil {
		return nil, fmt.Errorf("parse inference output: %w", err)
	}
	return candidates, nil
}

// AIInferenceClassifier is a read-only adapter over a prevalidated candidate
// lookup computed for one mailbox inference run.
type AIInferenceClassifier struct {
	deterministicCategories map[string]string
	candidates              map[string]InferenceCandidate
}

// NewAIInferenceClassifier creates an AIInferenceClassifier from precomputed
// deterministic categories and validated inference candidates keyed by
// message ID.
func NewAIInferenceClassifier(deterministicCategories map[string]string, candidates map[string]InferenceCandidate) *AIInferenceClassifier {
	detCopy := make(map[string]string, len(deterministicCategories))
	for k, v := range deterministicCategories {
		detCopy[k] = v
	}
	candidateCopy := make(map[string]InferenceCandidate, len(candidates))
	for k, v := range candidates {
		candidateCopy[k] = v
	}
	return &AIInferenceClassifier{
		deterministicCategories: detCopy,
		candidates:              candidateCopy,
	}
}

// Classify returns an AI-backed classification result only when the message is
// still deterministic-unknown and a validated candidate exists for its message
// ID.
func (c *AIInferenceClassifier) Classify(_ context.Context, msg models.MessageMeta) (ClassificationResult, error) {
	if category, ok := c.deterministicCategories[msg.ProviderID]; ok && category != CategoryUnknown {
		return ClassificationResult{
			Category:    CategoryUnknown,
			MatchedRule: "deterministic classification already resolved",
			Source:      SourceAI,
		}, nil
	}

	candidate, ok := c.candidates[msg.ProviderID]
	if !ok {
		return ClassificationResult{
			Category:    CategoryUnknown,
			MatchedRule: "no inference candidate",
			Source:      SourceAI,
		}, nil
	}

	return ClassificationResult{
		Category:    candidate.Category,
		MatchedRule: "ai:" + candidate.ConfidenceBand,
		Source:      SourceAI,
	}, nil
}

// specificityRank returns a sort rank for a pattern type. Lower = more specific.
func specificityRank(patternType string) int {
	switch patternType {
	case PatternSenderEmail:
		return 1
	case PatternSenderPrefix:
		return 2
	case PatternDomain:
		return 3
	case PatternSubjectTerm:
		return 4
	default:
		return 5
	}
}

// localPart returns the lowercased local part of an email address (before "@").
// If there is no "@", the entire address is returned lowercased.
func localPart(email string) string {
	parts := strings.SplitN(strings.ToLower(email), "@", 2)
	return parts[0]
}

// SeedRuleClassifier implements Classifier using an ordered list of ClassificationSeeds.
// Seeds are sorted at construction time and evaluated in order; the first matching
// seed's category is returned.
type SeedRuleClassifier struct {
	seeds []ClassificationSeed
}

// NewSeedRuleClassifier creates a SeedRuleClassifier with seeds sorted by
// (Priority ASC, specificityRank(PatternType) ASC, ID ASC).
func NewSeedRuleClassifier(seeds []ClassificationSeed) *SeedRuleClassifier {
	sorted := make([]ClassificationSeed, len(seeds))
	copy(sorted, seeds)
	sort.Slice(sorted, func(i, j int) bool {
		a, b := sorted[i], sorted[j]
		if a.Priority != b.Priority {
			return a.Priority < b.Priority
		}
		ra, rb := specificityRank(a.PatternType), specificityRank(b.PatternType)
		if ra != rb {
			return ra < rb
		}
		return a.ID < b.ID
	})
	return &SeedRuleClassifier{seeds: sorted}
}

// Classify evaluates msg against the sorted seed list and returns the first match.
// Returns CategoryUnknown when no seed matches.
func (c *SeedRuleClassifier) Classify(_ context.Context, msg models.MessageMeta) (ClassificationResult, error) {
	for _, seed := range c.seeds {
		if c.matches(seed, msg) {
			return ClassificationResult{
				Category:    seed.Category,
				MatchedRule: seed.PatternType + ":" + seed.PatternValue,
				Source:      seed.Source,
			}, nil
		}
	}
	return ClassificationResult{
		Category:    CategoryUnknown,
		MatchedRule: "no matching rule",
		Source:      SourceSeed,
	}, nil
}

// matches returns true when msg satisfies seed's pattern.
func (c *SeedRuleClassifier) matches(seed ClassificationSeed, msg models.MessageMeta) bool {
	switch seed.PatternType {
	case PatternDomain:
		return strings.EqualFold(msg.Domain, seed.PatternValue)
	case PatternSenderEmail:
		return strings.EqualFold(msg.FromEmail, seed.PatternValue)
	case PatternSenderPrefix:
		return strings.HasPrefix(localPart(msg.FromEmail), strings.ToLower(seed.PatternValue))
	case PatternSubjectTerm:
		return subjectHasTerm(msg.Subject, seed.PatternValue)
	default:
		return false
	}
}

func subjectHasTerm(subject, term string) bool {
	for _, token := range tokenizeSubject(subject) {
		if token == strings.ToLower(term) {
			return true
		}
	}
	return false
}

func tokenizeSubject(subject string) []string {
	splitter := func(r rune) bool {
		return unicode.IsSpace(r) || strings.ContainsRune(",.;:!?()[]\"'-", r)
	}
	raw := strings.FieldsFunc(subject, splitter)
	tokens := make([]string, 0, len(raw))
	for _, token := range raw {
		tokens = append(tokens, strings.ToLower(token))
	}
	return tokens
}

// InferenceConfidenceBand derives the canonical confidence band for one
// confidence score.
func InferenceConfidenceBand(confidence float64) string {
	switch {
	case confidence >= 0.80:
		return "high"
	case confidence >= 0.55:
		return "medium"
	default:
		return "low"
	}
}

// ValidateInferenceCandidate enforces the accepted inference contract for a
// provider-returned candidate against the submitted request set.
func ValidateInferenceCandidate(candidate InferenceCandidate, requests map[string]InferenceRequest) error {
	if _, ok := requests[candidate.MessageID]; !ok {
		return fmt.Errorf("unknown inference message_id %q", candidate.MessageID)
	}
	if !isKnownCategory(candidate.Category) {
		return fmt.Errorf("unsupported inference category %q", candidate.Category)
	}
	if candidate.Confidence < 0 || candidate.Confidence > 1 {
		return fmt.Errorf("invalid inference confidence %.3f", candidate.Confidence)
	}
	expectedBand := InferenceConfidenceBand(candidate.Confidence)
	if candidate.ConfidenceBand != expectedBand {
		return fmt.Errorf("inference confidence band %q does not match confidence %.3f", candidate.ConfidenceBand, candidate.Confidence)
	}
	if candidate.ReviewRequired != (expectedBand == "medium") {
		return fmt.Errorf("inference review_required %t does not match confidence band %q", candidate.ReviewRequired, candidate.ConfidenceBand)
	}
	return nil
}

func isKnownCategory(category string) bool {
	switch category {
	case CategoryInternal,
		CategoryClient,
		CategoryVendor,
		CategoryGovernment,
		CategorySystemGenerated,
		CategoryNewsletterMarketing,
		CategorySocial,
		CategoryUnknown:
		return true
	default:
		return false
	}
}

// ChainClassifier implements Classifier by delegating to an ordered list of
// classifiers. It returns the first non-unknown result. If all classifiers
// return unknown, it returns unknown. ChainClassifier is the composition point
// for future AI backends (§16.2).
type ChainClassifier struct {
	classifiers []Classifier
}

// NewChainClassifier creates a ChainClassifier that delegates to classifiers in order.
func NewChainClassifier(classifiers ...Classifier) *ChainClassifier {
	return &ChainClassifier{classifiers: classifiers}
}

// Classify delegates to each classifier in order and returns the first non-unknown
// result. If all classifiers return unknown, returns unknown.
func (c *ChainClassifier) Classify(ctx context.Context, msg models.MessageMeta) (ClassificationResult, error) {
	for _, cl := range c.classifiers {
		result, err := cl.Classify(ctx, msg)
		if err != nil {
			return ClassificationResult{}, err
		}
		if result.Category != CategoryUnknown {
			return result, nil
		}
	}
	return ClassificationResult{
		Category:    CategoryUnknown,
		MatchedRule: "no matching rule",
		Source:      SourceSeed,
	}, nil
}

// RunMailboxClassification classifies the provided messages for mailboxID using
// the active global plus mailbox-scoped seeds loaded from storage, then
// persists the results through BulkSaveClassifications.
func RunMailboxClassification(ctx context.Context, st *storage.Store, mailboxID string, messages []models.MessageMeta) error {
	if mailboxID == "" {
		return fmt.Errorf("mailboxID is required")
	}

	storedSeeds, err := st.ListSeeds(ctx, mailboxID)
	if err != nil {
		return fmt.Errorf("list seeds: %w", err)
	}

	seeds := make([]ClassificationSeed, len(storedSeeds))
	for i, seed := range storedSeeds {
		seeds[i] = ClassificationSeed{
			ID:           seed.ID,
			MailboxID:    seed.MailboxID,
			PatternType:  seed.PatternType,
			PatternValue: seed.PatternValue,
			Category:     seed.Category,
			Source:       seed.Source,
			Priority:     seed.Priority,
			CreatedAt:    seed.CreatedAt,
		}
	}

	classifier := NewSeedRuleClassifier(seeds)
	classifications := make([]storage.Classification, 0, len(messages))
	classifiedAt := time.Now().UTC()

	for _, msg := range messages {
		if msg.MailboxID != mailboxID {
			return fmt.Errorf("message %q belongs to mailbox %q, want %q", msg.ProviderID, msg.MailboxID, mailboxID)
		}
		if msg.ProviderID == "" {
			return fmt.Errorf("message providerID is required for mailbox %q", mailboxID)
		}

		result, err := classifier.Classify(ctx, msg)
		if err != nil {
			return fmt.Errorf("classify message %q: %w", msg.ProviderID, err)
		}

		classifications = append(classifications, storage.Classification{
			MessageID:    msg.ProviderID,
			MailboxID:    mailboxID,
			Category:     result.Category,
			MatchedRule:  result.MatchedRule,
			Source:       result.Source,
			ClassifiedAt: classifiedAt,
		})
	}

	if err := st.BulkSaveClassifications(ctx, classifications); err != nil {
		return fmt.Errorf("persist classifications: %w", err)
	}
	return nil
}

// DefaultSeeds returns the inbox-agnostic built-in baseline classification
// seeds. These defaults are safe to apply globally for any mailbox and exclude
// tenant-specific sender or domain relationships. Only domain, sender_email,
// and sender_prefix patterns are included; subject_term seeds are excluded due
// to ambiguity.
func DefaultSeeds() []ClassificationSeed {
	return []ClassificationSeed{
		// High-specificity baseline sender_email seeds (priority 50)
		{PatternType: PatternSenderEmail, PatternValue: "calendar-notification@google.com", Category: CategorySystemGenerated, Source: SourceSeed, Priority: 50},

		// Reusable platform and delivery domains (priority 100)
		{PatternType: PatternDomain, PatternValue: "facebookmail.com", Category: CategorySocial, Source: SourceSeed, Priority: 100},
		{PatternType: PatternDomain, PatternValue: "linkedin.com", Category: CategorySocial, Source: SourceSeed, Priority: 100},
		{PatternType: PatternDomain, PatternValue: "service.govdelivery.com", Category: CategoryGovernment, Source: SourceSeed, Priority: 100},
		{PatternType: PatternDomain, PatternValue: "sent-via.netsuite.com", Category: CategorySystemGenerated, Source: SourceSeed, Priority: 100},
		{PatternType: PatternDomain, PatternValue: "sf-notifications.com", Category: CategorySystemGenerated, Source: SourceSeed, Priority: 100},
		{PatternType: PatternDomain, PatternValue: "paycomonline.com", Category: CategorySystemGenerated, Source: SourceSeed, Priority: 100},
		{PatternType: PatternDomain, PatternValue: "mail.momence.com", Category: CategorySystemGenerated, Source: SourceSeed, Priority: 100},
		{PatternType: PatternDomain, PatternValue: "mail.zapier.com", Category: CategorySystemGenerated, Source: SourceSeed, Priority: 100},
		{PatternType: PatternDomain, PatternValue: "joinhomebase.com", Category: CategorySystemGenerated, Source: SourceSeed, Priority: 100},
		{PatternType: PatternDomain, PatternValue: "onesaas.com", Category: CategorySystemGenerated, Source: SourceSeed, Priority: 100},

		// Sender prefix seeds (priority 150) — lower specificity than exact matches
		{PatternType: PatternSenderPrefix, PatternValue: "noreply", Category: CategorySystemGenerated, Source: SourceSeed, Priority: 150},
		{PatternType: PatternSenderPrefix, PatternValue: "no-reply", Category: CategorySystemGenerated, Source: SourceSeed, Priority: 150},
		{PatternType: PatternSenderPrefix, PatternValue: "donotreply", Category: CategorySystemGenerated, Source: SourceSeed, Priority: 150},
	}
}

// MailboxBootstrapSuggestions returns mailbox-scoped candidate seeds derived
// from mailbox-local discovery. These suggestions are intentionally separate
// from DefaultSeeds and from promoted active seeds; they require explicit
// operator review before influencing runtime classification.
func MailboxBootstrapSuggestions(ctx context.Context, st *storage.Store, mailboxID string, minCount int) ([]ClassificationSeed, error) {
	if mailboxID == "" {
		return nil, nil
	}
	if st == nil {
		return nil, fmt.Errorf("storage is required")
	}
	if minCount < 1 {
		minCount = 1
	}

	defaultCovered := make(map[string]struct{}, len(DefaultSeeds()))
	for _, seed := range DefaultSeeds() {
		defaultCovered[seed.PatternType+"\x00"+strings.ToLower(seed.PatternValue)] = struct{}{}
	}

	senderStats, err := st.QuerySenderStatsByMailbox(ctx, mailboxID, minCount)
	if err != nil {
		return nil, fmt.Errorf("query sender stats: %w", err)
	}
	domainStats, err := st.QueryDomainStatsByMailbox(ctx, mailboxID, minCount)
	if err != nil {
		return nil, fmt.Errorf("query domain stats: %w", err)
	}

	suggestions := make([]ClassificationSeed, 0, len(senderStats)+len(domainStats))
	seen := make(map[string]struct{}, len(senderStats)+len(domainStats))
	appendSuggestion := func(patternType, patternValue string) {
		patternValue = strings.TrimSpace(strings.ToLower(patternValue))
		if patternValue == "" {
			return
		}
		key := patternType + "\x00" + patternValue
		if _, ok := defaultCovered[key]; ok {
			return
		}
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		suggestions = append(suggestions, ClassificationSeed{
			MailboxID:    mailboxID,
			PatternType:  patternType,
			PatternValue: patternValue,
			Category:     CategoryUnknown,
			Source:       SourceSeed,
			Priority:     100,
		})
	}

	for _, sender := range senderStats {
		appendSuggestion(PatternSenderEmail, sender.Email)
	}
	for _, domain := range domainStats {
		appendSuggestion(PatternDomain, domain.Domain)
	}

	sort.Slice(suggestions, func(i, j int) bool {
		if suggestions[i].PatternType != suggestions[j].PatternType {
			return suggestions[i].PatternType < suggestions[j].PatternType
		}
		return suggestions[i].PatternValue < suggestions[j].PatternValue
	})
	return suggestions, nil
}
