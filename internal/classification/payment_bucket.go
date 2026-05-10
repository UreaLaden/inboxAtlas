// Package classification provides the deterministic rules-based message classification
// engine for InboxAtlas. Payment bucket routing extends classification outputs
// with a separate deterministic payment-handling namespace.
package classification

import (
	"context"
	"sort"
	"strings"

	"github.com/UreaLaden/inboxatlas/pkg/models"
)

// Payment bucket constants define the supported payment-handling routing
// outputs for the initial deterministic v1 slice.
const (
	// BucketPayableInvoiceAutomated routes automated invoice senders.
	BucketPayableInvoiceAutomated = "payable-invoice-automated"
	// BucketPayableUrgentInsurance routes urgent insurance payment traffic.
	BucketPayableUrgentInsurance = "payable-urgent-insurance"
	// BucketPayableOverdueVendor routes overdue vendor payment subjects.
	BucketPayableOverdueVendor = "payable-overdue-vendor"
	// BucketStatementConfirmation routes statement-confirmation traffic.
	BucketStatementConfirmation = "statement-confirmation"
	// BucketPayableInvoiceVendor routes vendor invoice traffic based on
	// persisted category and intent.
	BucketPayableInvoiceVendor = "payable-invoice-vendor"
	// BucketNone indicates that no payment bucket rule matched.
	BucketNone = ""
)

// Payment bucket pattern constants define how a PaymentBucketRule is matched.
const (
	// BucketPatternSenderEmail matches the full sender email exactly.
	BucketPatternSenderEmail = "sender_email"
	// BucketPatternDomain matches the sender domain exactly.
	BucketPatternDomain = "domain"
	// BucketPatternSubjectKeywords matches if any configured keyword is present
	// in the normalized subject token set.
	BucketPatternSubjectKeywords = "subject_keywords"
	// BucketPatternCategoryIntent matches the persisted category plus optional
	// intent.
	BucketPatternCategoryIntent = "category_intent"
)

// PaymentBucketRule is one deterministic payment bucket routing rule.
type PaymentBucketRule struct {
	Bucket           string
	Priority         int
	PatternType      string
	PatternValue     string
	SubjectKeywords  []string
	RequiredCategory string
	RequiredIntent   string
}

// PaymentBucketRouter routes a classified message into a payment-handling
// bucket without persisting any result.
type PaymentBucketRouter interface {
	AssignBucket(ctx context.Context, msg models.MessageMeta, category, intent string) (bucket, matchedRule string, err error)
}

// RuleBasedPaymentBucketRouter routes messages using a fixed priority-sorted
// ruleset.
type RuleBasedPaymentBucketRouter struct {
	rules []PaymentBucketRule
}

// NewRuleBasedPaymentBucketRouter creates a RuleBasedPaymentBucketRouter with
// rules sorted by ascending priority while preserving declaration order for
// ties.
func NewRuleBasedPaymentBucketRouter(rules []PaymentBucketRule) *RuleBasedPaymentBucketRouter {
	sorted := make([]PaymentBucketRule, len(rules))
	copy(sorted, rules)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].Priority < sorted[j].Priority
	})
	return &RuleBasedPaymentBucketRouter{rules: sorted}
}

// AssignBucket evaluates msg plus its persisted category and intent against
// the configured routing rules and returns the first matched bucket.
func (r *RuleBasedPaymentBucketRouter) AssignBucket(_ context.Context, msg models.MessageMeta, category, intent string) (string, string, error) {
	for _, rule := range r.rules {
		if paymentBucketRuleMatches(rule, msg, category, intent) {
			return rule.Bucket, paymentBucketMatchedRule(rule), nil
		}
	}
	return BucketNone, "", nil
}

// DefaultPaymentBucketRules returns the analyzer-approved deterministic v1
// payment routing ruleset.
func DefaultPaymentBucketRules() []PaymentBucketRule {
	return []PaymentBucketRule{
		{Bucket: BucketPayableInvoiceAutomated, Priority: 1, PatternType: BucketPatternSenderEmail, PatternValue: "messenger@messaging.squareup.com"},
		{Bucket: BucketPayableInvoiceAutomated, Priority: 1, PatternType: BucketPatternSenderEmail, PatternValue: "account-services@inform.bill.com"},
		{Bucket: BucketPayableInvoiceAutomated, Priority: 1, PatternType: BucketPatternSenderEmail, PatternValue: "system@sent-via.netsuite.com"},
		{Bucket: BucketPayableInvoiceAutomated, Priority: 1, PatternType: BucketPatternSenderEmail, PatternValue: "quickbooks@notification.intuit.com"},
		{Bucket: BucketPayableInvoiceAutomated, Priority: 1, PatternType: BucketPatternSenderEmail, PatternValue: "ar@definiti.com"},
		{Bucket: BucketPayableUrgentInsurance, Priority: 2, PatternType: BucketPatternDomain, PatternValue: "avanteins.com"},
		{Bucket: BucketPayableUrgentInsurance, Priority: 2, PatternType: BucketPatternDomain, PatternValue: "roadreadyinsurance.com"},
		{Bucket: BucketPayableOverdueVendor, Priority: 3, PatternType: BucketPatternSubjectKeywords, SubjectKeywords: []string{"overdue", "obligation"}},
		{Bucket: BucketStatementConfirmation, Priority: 3, PatternType: BucketPatternSubjectKeywords, SubjectKeywords: []string{"statement"}},
		{Bucket: BucketPayableInvoiceVendor, Priority: 4, PatternType: BucketPatternCategoryIntent, RequiredCategory: CategoryVendor, RequiredIntent: IntentInvoice},
	}
}

func paymentBucketRuleMatches(rule PaymentBucketRule, msg models.MessageMeta, category, intent string) bool {
	switch rule.PatternType {
	case BucketPatternSenderEmail:
		return strings.EqualFold(msg.FromEmail, rule.PatternValue)
	case BucketPatternDomain:
		return strings.EqualFold(msg.Domain, rule.PatternValue)
	case BucketPatternSubjectKeywords:
		if len(rule.SubjectKeywords) == 0 {
			return false
		}
		tokens := tokenizeSubject(NormalizeSubject(msg.Subject))
		tokenSet := make(map[string]struct{}, len(tokens))
		for _, token := range tokens {
			tokenSet[token] = struct{}{}
		}
		for _, keyword := range rule.SubjectKeywords {
			if _, ok := tokenSet[strings.ToLower(keyword)]; ok {
				return true
			}
		}
		return false
	case BucketPatternCategoryIntent:
		if !strings.EqualFold(category, rule.RequiredCategory) {
			return false
		}
		if strings.TrimSpace(rule.RequiredIntent) == "" {
			return true
		}
		return strings.EqualFold(intent, rule.RequiredIntent)
	default:
		return false
	}
}

func paymentBucketMatchedRule(rule PaymentBucketRule) string {
	return "bucket_rule:" + rule.PatternType + ":" + paymentBucketRuleValue(rule)
}

func paymentBucketRuleValue(rule PaymentBucketRule) string {
	switch rule.PatternType {
	case BucketPatternSubjectKeywords:
		values := make([]string, len(rule.SubjectKeywords))
		for i, keyword := range rule.SubjectKeywords {
			values[i] = strings.ToLower(keyword)
		}
		return strings.Join(values, ",")
	case BucketPatternCategoryIntent:
		if strings.TrimSpace(rule.RequiredIntent) == "" {
			return strings.ToLower(rule.RequiredCategory)
		}
		return strings.ToLower(rule.RequiredCategory) + ":" + strings.ToLower(rule.RequiredIntent)
	default:
		return strings.ToLower(strings.TrimSpace(rule.PatternValue))
	}
}
