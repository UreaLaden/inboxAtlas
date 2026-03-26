package classification

import (
	"context"
	"testing"
	"time"

	"github.com/UreaLaden/inboxatlas/pkg/models"
)

// makeMsg returns a minimal MessageMeta for testing.
func makeMsg(fromEmail, domain, subject string) models.MessageMeta {
	return models.MessageMeta{
		FromEmail: fromEmail,
		Domain:    domain,
		Subject:   subject,
	}
}

func TestSeedRuleClassifier_DomainMatch(t *testing.T) {
	seeds := []ClassificationSeed{
		{ID: 1, PatternType: PatternDomain, PatternValue: "facebookmail.com", Category: CategorySocial, Source: SourceSeed, Priority: 100},
	}
	c := NewSeedRuleClassifier(seeds)
	result, err := c.Classify(context.Background(), makeMsg("groupupdates@facebookmail.com", "facebookmail.com", ""))
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if result.Category != CategorySocial {
		t.Errorf("Category: got %q, want %q", result.Category, CategorySocial)
	}
	if result.MatchedRule != "domain:facebookmail.com -> social" {
		t.Errorf("MatchedRule: got %q, want %q", result.MatchedRule, "domain:facebookmail.com -> social")
	}
}

func TestSeedRuleClassifier_DomainMatchCaseInsensitive(t *testing.T) {
	seeds := []ClassificationSeed{
		{ID: 1, PatternType: PatternDomain, PatternValue: "facebookmail.com", Category: CategorySocial, Source: SourceSeed, Priority: 100},
	}
	c := NewSeedRuleClassifier(seeds)
	result, err := c.Classify(context.Background(), makeMsg("user@FACEBOOKMAIL.COM", "FACEBOOKMAIL.COM", ""))
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if result.Category != CategorySocial {
		t.Errorf("Category: got %q, want %q", result.Category, CategorySocial)
	}
}

func TestSeedRuleClassifier_SenderEmailMatch(t *testing.T) {
	// sender_email at priority 50 should win over domain seed at priority 100
	seeds := []ClassificationSeed{
		{ID: 1, PatternType: PatternSenderEmail, PatternValue: "acr@acrbookkeepingplus.com", Category: CategoryVendor, Source: SourceSeed, Priority: 50},
		{ID: 2, PatternType: PatternDomain, PatternValue: "acrbookkeepingplus.com", Category: CategoryVendor, Source: SourceSeed, Priority: 100},
	}
	c := NewSeedRuleClassifier(seeds)
	result, err := c.Classify(context.Background(), makeMsg("acr@acrbookkeepingplus.com", "acrbookkeepingplus.com", ""))
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if result.Category != CategoryVendor {
		t.Errorf("Category: got %q, want %q", result.Category, CategoryVendor)
	}
	if result.MatchedRule != "sender_email:acr@acrbookkeepingplus.com -> vendor" {
		t.Errorf("MatchedRule: got %q, want %q", result.MatchedRule, "sender_email:acr@acrbookkeepingplus.com -> vendor")
	}
}

func TestSeedRuleClassifier_SenderPrefixMatch(t *testing.T) {
	seeds := []ClassificationSeed{
		{ID: 1, PatternType: PatternSenderPrefix, PatternValue: "noreply", Category: CategorySystemGenerated, Source: SourceSeed, Priority: 150},
	}
	c := NewSeedRuleClassifier(seeds)
	result, err := c.Classify(context.Background(), makeMsg("noreply@unknown-domain.com", "unknown-domain.com", ""))
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if result.Category != CategorySystemGenerated {
		t.Errorf("Category: got %q, want %q", result.Category, CategorySystemGenerated)
	}
	if result.MatchedRule != "sender_prefix:noreply -> system-generated" {
		t.Errorf("MatchedRule: got %q, want %q", result.MatchedRule, "sender_prefix:noreply -> system-generated")
	}
}

func TestSeedRuleClassifier_SenderPrefixMatchCaseInsensitive(t *testing.T) {
	seeds := []ClassificationSeed{
		{ID: 1, PatternType: PatternSenderPrefix, PatternValue: "noreply", Category: CategorySystemGenerated, Source: SourceSeed, Priority: 150},
	}
	c := NewSeedRuleClassifier(seeds)
	// Local part uppercase — prefix match should still work
	result, err := c.Classify(context.Background(), makeMsg("NOREPLY@example.com", "example.com", ""))
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if result.Category != CategorySystemGenerated {
		t.Errorf("Category: got %q, want %q", result.Category, CategorySystemGenerated)
	}
}

func TestSeedRuleClassifier_SenderPrefixVsDomain_LowerPriorityWins(t *testing.T) {
	// sender_email seed at priority 50 must be evaluated before prefix at priority 150
	seeds := []ClassificationSeed{
		{ID: 1, PatternType: PatternSenderEmail, PatternValue: "acr@acrbookkeepingplus.com", Category: CategoryVendor, Source: SourceSeed, Priority: 50},
		{ID: 2, PatternType: PatternSenderPrefix, PatternValue: "acr", Category: CategorySystemGenerated, Source: SourceSeed, Priority: 150},
	}
	c := NewSeedRuleClassifier(seeds)
	result, err := c.Classify(context.Background(), makeMsg("acr@acrbookkeepingplus.com", "acrbookkeepingplus.com", ""))
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	// sender_email (priority 50) beats prefix (priority 150)
	if result.Category != CategoryVendor {
		t.Errorf("Category: got %q, want %q", result.Category, CategoryVendor)
	}
}

func TestSeedRuleClassifier_NoMatch(t *testing.T) {
	seeds := []ClassificationSeed{
		{ID: 1, PatternType: PatternDomain, PatternValue: "example.com", Category: CategoryVendor, Source: SourceSeed, Priority: 100},
	}
	c := NewSeedRuleClassifier(seeds)
	result, err := c.Classify(context.Background(), makeMsg("unknown@unknown.xyz", "unknown.xyz", ""))
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if result.Category != CategoryUnknown {
		t.Errorf("Category: got %q, want %q", result.Category, CategoryUnknown)
	}
}

func TestSeedRuleClassifier_MatchedRuleNonEmpty(t *testing.T) {
	seeds := DefaultSeeds()
	c := NewSeedRuleClassifier(seeds)
	ctx := context.Background()

	tests := []struct {
		name   string
		email  string
		domain string
	}{
		{"domain match", "user@facebookmail.com", "facebookmail.com"},
		{"sender_email match", "acr@acrbookkeepingplus.com", "acrbookkeepingplus.com"},
		{"prefix match", "noreply@example.com", "example.com"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := c.Classify(ctx, makeMsg(tt.email, tt.domain, ""))
			if err != nil {
				t.Fatalf("Classify: %v", err)
			}
			if result.Category == CategoryUnknown {
				t.Errorf("expected non-unknown result for %q", tt.email)
			}
			if result.MatchedRule == "" {
				t.Errorf("MatchedRule must be non-empty for non-unknown result; email=%q", tt.email)
			}
		})
	}
}

func TestSeedRuleClassifier_UnknownPatternType(t *testing.T) {
	seeds := []ClassificationSeed{
		{ID: 1, PatternType: "unknown_type", PatternValue: "anything", Category: CategoryVendor, Source: SourceSeed, Priority: 100},
	}
	c := NewSeedRuleClassifier(seeds)
	result, err := c.Classify(context.Background(), makeMsg("user@example.com", "example.com", ""))
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	// Unknown pattern type never matches, so result should be unknown
	if result.Category != CategoryUnknown {
		t.Errorf("unknown pattern type should not match; got %q", result.Category)
	}
}

func TestSeedRuleClassifier_SubjectTermMatch(t *testing.T) {
	seeds := []ClassificationSeed{
		{ID: 1, PatternType: PatternSubjectTerm, PatternValue: "invoice", Category: CategoryVendor, Source: SourceSeed, Priority: 100},
	}
	c := NewSeedRuleClassifier(seeds)
	result, err := c.Classify(context.Background(), makeMsg("user@example.com", "example.com", "Invoice #12345 due"))
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if result.Category != CategoryVendor {
		t.Errorf("Category: got %q, want %q", result.Category, CategoryVendor)
	}
}

func TestSeedRuleClassifier_SubjectTermDoesNotMatchSubstring(t *testing.T) {
	seeds := []ClassificationSeed{
		{ID: 1, PatternType: PatternSubjectTerm, PatternValue: "pay", Category: CategoryVendor, Source: SourceSeed, Priority: 100},
	}
	c := NewSeedRuleClassifier(seeds)
	result, err := c.Classify(context.Background(), makeMsg("user@example.com", "example.com", "Payroll processed"))
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if result.Category != CategoryUnknown {
		t.Errorf("Category: got %q, want %q", result.Category, CategoryUnknown)
	}
}

func TestChainClassifier_FirstNonUnknown(t *testing.T) {
	// First classifier returns unknown; second returns client
	firstClassifier := NewSeedRuleClassifier([]ClassificationSeed{
		{ID: 1, PatternType: PatternDomain, PatternValue: "known.com", Category: CategoryVendor, Source: SourceSeed, Priority: 100},
	})
	stubClientClassifier := &stubClassifier{result: ClassificationResult{Category: CategoryClient, MatchedRule: "stub:client", Source: SourceSeed}}

	chain := NewChainClassifier(firstClassifier, stubClientClassifier)
	result, err := chain.Classify(context.Background(), makeMsg("user@unknown.xyz", "unknown.xyz", ""))
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if result.Category != CategoryClient {
		t.Errorf("Category: got %q, want %q", result.Category, CategoryClient)
	}
}

func TestChainClassifier_FirstMatchWins(t *testing.T) {
	// First classifier matches — second should not be called
	firstClassifier := NewSeedRuleClassifier([]ClassificationSeed{
		{ID: 1, PatternType: PatternDomain, PatternValue: "known.com", Category: CategoryVendor, Source: SourceSeed, Priority: 100},
	})
	stubClientClassifier := &stubClassifier{result: ClassificationResult{Category: CategoryClient, MatchedRule: "stub:client", Source: SourceSeed}}

	chain := NewChainClassifier(firstClassifier, stubClientClassifier)
	result, err := chain.Classify(context.Background(), makeMsg("user@known.com", "known.com", ""))
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if result.Category != CategoryVendor {
		t.Errorf("Category: got %q, want %q", result.Category, CategoryVendor)
	}
}

func TestChainClassifier_AllUnknown(t *testing.T) {
	c1 := NewSeedRuleClassifier(nil)
	c2 := NewSeedRuleClassifier(nil)
	chain := NewChainClassifier(c1, c2)
	result, err := chain.Classify(context.Background(), makeMsg("user@unknown.xyz", "unknown.xyz", ""))
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if result.Category != CategoryUnknown {
		t.Errorf("Category: got %q, want %q", result.Category, CategoryUnknown)
	}
}

func TestChainClassifier_Empty(t *testing.T) {
	chain := NewChainClassifier()
	result, err := chain.Classify(context.Background(), makeMsg("user@unknown.xyz", "unknown.xyz", ""))
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if result.Category != CategoryUnknown {
		t.Errorf("Category: got %q, want %q", result.Category, CategoryUnknown)
	}
}

func TestChainClassifier_PropagatesError(t *testing.T) {
	errClassifier := &errorClassifier{}
	chain := NewChainClassifier(errClassifier)
	_, err := chain.Classify(context.Background(), makeMsg("user@example.com", "example.com", ""))
	if err == nil {
		t.Fatal("expected error from chain classifier")
	}
}

func TestDefaultSeeds_Integrity(t *testing.T) {
	seeds := DefaultSeeds()

	if len(seeds) <= 20 {
		t.Errorf("DefaultSeeds() should return more than 20 seeds; got %d", len(seeds))
	}

	validCategories := map[string]bool{
		CategoryInternal: true, CategoryClient: true, CategoryVendor: true,
		CategoryGovernment: true, CategorySystemGenerated: true,
		CategoryNewsletterMarketing: true, CategorySocial: true, CategoryUnknown: true,
	}
	validPatternTypes := map[string]bool{
		PatternDomain: true, PatternSenderEmail: true,
		PatternSenderPrefix: true, PatternSubjectTerm: true,
	}

	for i, s := range seeds {
		if s.PatternType == "" {
			t.Errorf("seed[%d]: PatternType is empty", i)
		}
		if s.PatternValue == "" {
			t.Errorf("seed[%d]: PatternValue is empty", i)
		}
		if s.Category == "" {
			t.Errorf("seed[%d]: Category is empty", i)
		}
		if !validCategories[s.Category] {
			t.Errorf("seed[%d]: unknown Category %q", i, s.Category)
		}
		if !validPatternTypes[s.PatternType] {
			t.Errorf("seed[%d]: unknown PatternType %q", i, s.PatternType)
		}
		if s.PatternType == PatternSubjectTerm {
			t.Errorf("seed[%d]: DefaultSeeds() must not contain subject_term seeds", i)
		}
		if s.Source != SourceSeed {
			t.Errorf("seed[%d]: Source: got %q, want %q", i, s.Source, SourceSeed)
		}
	}
}

func TestDefaultSeeds_FacebookmailSocial(t *testing.T) {
	seeds := DefaultSeeds()
	c := NewSeedRuleClassifier(seeds)
	result, err := c.Classify(context.Background(), makeMsg("groupupdates@facebookmail.com", "facebookmail.com", ""))
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if result.Category != CategorySocial {
		t.Errorf("facebookmail.com: got %q, want %q", result.Category, CategorySocial)
	}
}

func TestDefaultSeeds_NoreplySystemGenerated(t *testing.T) {
	seeds := DefaultSeeds()
	c := NewSeedRuleClassifier(seeds)
	result, err := c.Classify(context.Background(), makeMsg("noreply@unknown-domain.com", "unknown-domain.com", ""))
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if result.Category != CategorySystemGenerated {
		t.Errorf("noreply@unknown-domain.com: got %q, want %q", result.Category, CategorySystemGenerated)
	}
}

func TestDefaultSeeds_UnknownSender(t *testing.T) {
	seeds := DefaultSeeds()
	c := NewSeedRuleClassifier(seeds)
	result, err := c.Classify(context.Background(), makeMsg("unknown@unknown.xyz", "unknown.xyz", ""))
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if result.Category != CategoryUnknown {
		t.Errorf("unknown@unknown.xyz: got %q, want %q", result.Category, CategoryUnknown)
	}
}

// stubClassifier is a test-only Classifier that always returns a fixed result.
type stubClassifier struct {
	result ClassificationResult
}

func (s *stubClassifier) Classify(_ context.Context, _ models.MessageMeta) (ClassificationResult, error) {
	return s.result, nil
}

// errorClassifier is a test-only Classifier that always returns an error.
type errorClassifier struct{}

func (e *errorClassifier) Classify(_ context.Context, _ models.MessageMeta) (ClassificationResult, error) {
	return ClassificationResult{}, &classifyError{"injected error"}
}

type classifyError struct{ msg string }

func (e *classifyError) Error() string { return e.msg }

// Ensure the package compiles with the correct time import.
var _ = time.Now
