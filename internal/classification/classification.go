// Package classification provides the deterministic rules-based message classification
// engine for InboxAtlas. All classification logic is delivered through the Classifier
// interface, which is designed to be swappable so that future AI-backed classifiers
// can slot in without changing callsites.
package classification

import (
	"context"
	"sort"
	"strings"
	"time"
	"unicode"

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

// DefaultSeeds returns the corpus-grounded built-in classification seeds.
// These seeds cover the major sender/domain clusters observed in the corpus.
// Only domain, sender_email, and sender_prefix patterns are included —
// subject_term seeds are excluded due to ambiguity.
// Operators can extend classification by inserting seeds with source = "operator".
func DefaultSeeds() []ClassificationSeed {
	return []ClassificationSeed{
		// High-specificity sender_email seeds (priority 50)
		{PatternType: PatternSenderEmail, PatternValue: "calendar-notification@google.com", Category: CategorySystemGenerated, Source: SourceSeed, Priority: 50},
		{PatternType: PatternSenderEmail, PatternValue: "acr@acrbookkeepingplus.com", Category: CategoryVendor, Source: SourceSeed, Priority: 50},

		// Domain seeds (priority 100)
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
		{PatternType: PatternDomain, PatternValue: "ealerts.bankofamerica.com", Category: CategoryVendor, Source: SourceSeed, Priority: 100},
		{PatternType: PatternDomain, PatternValue: "citynational.com", Category: CategoryVendor, Source: SourceSeed, Priority: 100},
		{PatternType: PatternDomain, PatternValue: "cardinalhealth.com", Category: CategoryVendor, Source: SourceSeed, Priority: 100},
		{PatternType: PatternDomain, PatternValue: "acrbookkeepingplus.com", Category: CategoryVendor, Source: SourceSeed, Priority: 100},
		{PatternType: PatternDomain, PatternValue: "healthymd.com", Category: CategoryClient, Source: SourceSeed, Priority: 100},
		{PatternType: PatternDomain, PatternValue: "law360.com", Category: CategoryNewsletterMarketing, Source: SourceSeed, Priority: 100},
		{PatternType: PatternDomain, PatternValue: "cpatrendlines.com", Category: CategoryNewsletterMarketing, Source: SourceSeed, Priority: 100},
		{PatternType: PatternDomain, PatternValue: "mails.mycareers.net", Category: CategoryNewsletterMarketing, Source: SourceSeed, Priority: 100},
		{PatternType: PatternDomain, PatternValue: "ktainstitute.com", Category: CategoryNewsletterMarketing, Source: SourceSeed, Priority: 100},
		{PatternType: PatternDomain, PatternValue: "email.bradfordtaxinstitute.com", Category: CategoryNewsletterMarketing, Source: SourceSeed, Priority: 100},
		{PatternType: PatternDomain, PatternValue: "woodard.com", Category: CategoryNewsletterMarketing, Source: SourceSeed, Priority: 100},

		// Sender prefix seeds (priority 150) — lower specificity than exact matches
		{PatternType: PatternSenderPrefix, PatternValue: "noreply", Category: CategorySystemGenerated, Source: SourceSeed, Priority: 150},
		{PatternType: PatternSenderPrefix, PatternValue: "no-reply", Category: CategorySystemGenerated, Source: SourceSeed, Priority: 150},
		{PatternType: PatternSenderPrefix, PatternValue: "donotreply", Category: CategorySystemGenerated, Source: SourceSeed, Priority: 150},
	}
}
