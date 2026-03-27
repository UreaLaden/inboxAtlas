package classification

import (
	"context"
	"testing"
	"time"

	"github.com/UreaLaden/inboxatlas/internal/storage"
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
	if result.MatchedRule != "domain:facebookmail.com" {
		t.Errorf("MatchedRule: got %q, want %q", result.MatchedRule, "domain:facebookmail.com")
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
	if result.MatchedRule != "sender_email:acr@acrbookkeepingplus.com" {
		t.Errorf("MatchedRule: got %q, want %q", result.MatchedRule, "sender_email:acr@acrbookkeepingplus.com")
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
	if result.MatchedRule != "sender_prefix:noreply" {
		t.Errorf("MatchedRule: got %q, want %q", result.MatchedRule, "sender_prefix:noreply")
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
		{"sender_email match", "calendar-notification@google.com", "google.com"},
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

func TestSeedRuleClassifier_MatchedRuleFormat(t *testing.T) {
	seeds := []ClassificationSeed{
		{ID: 1, PatternType: PatternDomain, PatternValue: "facebookmail.com", Category: CategorySocial, Source: SourceSeed, Priority: 100},
	}
	c := NewSeedRuleClassifier(seeds)
	result, err := c.Classify(context.Background(), makeMsg("user@facebookmail.com", "facebookmail.com", ""))
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if result.MatchedRule != "domain:facebookmail.com" {
		t.Fatalf("MatchedRule: got %q, want %q", result.MatchedRule, "domain:facebookmail.com")
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

	if len(seeds) < 10 {
		t.Errorf("DefaultSeeds() should retain a meaningful reusable baseline; got %d seeds", len(seeds))
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

func TestDefaultSeeds_DoNotContainTenantSpecificSeeds(t *testing.T) {
	seeds := DefaultSeeds()
	disallowed := map[string]struct{}{
		"acr@acrbookkeepingplus.com":     {},
		"acrbookkeepingplus.com":         {},
		"healthymd.com":                  {},
		"cardinalhealth.com":             {},
		"citynational.com":               {},
		"ealerts.bankofamerica.com":      {},
		"law360.com":                     {},
		"cpatrendlines.com":              {},
		"mails.mycareers.net":            {},
		"ktainstitute.com":               {},
		"email.bradfordtaxinstitute.com": {},
		"woodard.com":                    {},
	}

	for _, seed := range seeds {
		if _, found := disallowed[seed.PatternValue]; found {
			t.Fatalf("tenant-specific seed leaked into DefaultSeeds: %+v", seed)
		}
		if seed.MailboxID != "" {
			t.Fatalf("DefaultSeeds must remain global; got mailbox-scoped seed %+v", seed)
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

func TestDefaultSeeds_DoNotClassifyFormerTenantSpecificDomain(t *testing.T) {
	seeds := DefaultSeeds()
	c := NewSeedRuleClassifier(seeds)
	result, err := c.Classify(context.Background(), makeMsg("owner@healthymd.com", "healthymd.com", ""))
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if result.Category != CategoryUnknown {
		t.Fatalf("healthymd.com should not be classified by baseline defaults; got %q", result.Category)
	}
}

func TestMailboxBootstrapSuggestions_AreMailboxScoped(t *testing.T) {
	suggestions := MailboxBootstrapSuggestions("owner@example.com")
	if len(suggestions) == 0 {
		t.Fatal("expected mailbox bootstrap suggestions")
	}

	foundTenantSeed := false
	for _, suggestion := range suggestions {
		if suggestion.MailboxID != "owner@example.com" {
			t.Fatalf("suggestion mailbox_id: got %q, want %q", suggestion.MailboxID, "owner@example.com")
		}
		if suggestion.PatternValue == "healthymd.com" {
			foundTenantSeed = true
		}
	}
	if !foundTenantSeed {
		t.Fatal("expected healthymd.com in mailbox bootstrap suggestions")
	}
}

func TestMailboxBootstrapSuggestions_EmptyMailboxID(t *testing.T) {
	suggestions := MailboxBootstrapSuggestions("")
	if len(suggestions) != 0 {
		t.Fatalf("expected no suggestions for empty mailbox id; got %d", len(suggestions))
	}
}

func TestRunMailboxClassification_LoadsGlobalAndMailboxScopedSeeds(t *testing.T) {
	st := newClassificationStore(t)
	ctx := context.Background()
	createClassificationMailbox(t, st, "alpha@example.com")
	createClassificationMailbox(t, st, "beta@example.com")

	mustInsertSeed(t, st, storage.ClassificationSeed{
		PatternType:  PatternDomain,
		PatternValue: "example.com",
		Category:     CategoryGovernment,
		Source:       SourceSeed,
		Priority:     100,
	})
	mustInsertSeed(t, st, storage.ClassificationSeed{
		MailboxID:    "alpha@example.com",
		PatternType:  PatternSenderEmail,
		PatternValue: "owner@example.com",
		Category:     CategoryClient,
		Source:       SourceOperator,
		Priority:     50,
	})
	mustInsertSeed(t, st, storage.ClassificationSeed{
		MailboxID:    "beta@example.com",
		PatternType:  PatternSenderEmail,
		PatternValue: "owner@example.com",
		Category:     CategoryVendor,
		Source:       SourceOperator,
		Priority:     50,
	})

	alphaMsg := mustStoreMessage(t, st, models.MessageMeta{
		ProviderID: "alpha-msg",
		MailboxID:  "alpha@example.com",
		Provider:   "gmail",
		FromEmail:  "owner@example.com",
		Domain:     "example.com",
		ReceivedAt: time.Now().UTC(),
	})
	betaMsg := mustStoreMessage(t, st, models.MessageMeta{
		ProviderID: "beta-msg",
		MailboxID:  "beta@example.com",
		Provider:   "gmail",
		FromEmail:  "owner@example.com",
		Domain:     "example.com",
		ReceivedAt: time.Now().UTC(),
	})

	if err := RunMailboxClassification(ctx, st, "alpha@example.com", []models.MessageMeta{alphaMsg}); err != nil {
		t.Fatalf("RunMailboxClassification alpha: %v", err)
	}
	if err := RunMailboxClassification(ctx, st, "beta@example.com", []models.MessageMeta{betaMsg}); err != nil {
		t.Fatalf("RunMailboxClassification beta: %v", err)
	}

	alphaClassification := mustGetClassification(t, st, "alpha-msg", "alpha@example.com")
	if alphaClassification.Category != CategoryClient {
		t.Fatalf("alpha category: got %q, want %q", alphaClassification.Category, CategoryClient)
	}
	if alphaClassification.MatchedRule != "sender_email:owner@example.com" {
		t.Fatalf("alpha matched rule: got %q", alphaClassification.MatchedRule)
	}

	betaClassification := mustGetClassification(t, st, "beta-msg", "beta@example.com")
	if betaClassification.Category != CategoryVendor {
		t.Fatalf("beta category: got %q, want %q", betaClassification.Category, CategoryVendor)
	}
}

func TestRunMailboxClassification_NewInboxGetsBaselineFromGlobalSeeds(t *testing.T) {
	st := newClassificationStore(t)
	ctx := context.Background()
	createClassificationMailbox(t, st, "new@example.com")

	mustInsertSeed(t, st, storage.ClassificationSeed{
		PatternType:  PatternDomain,
		PatternValue: "facebookmail.com",
		Category:     CategorySocial,
		Source:       SourceSeed,
		Priority:     100,
	})

	msg := mustStoreMessage(t, st, models.MessageMeta{
		ProviderID: "baseline-msg",
		MailboxID:  "new@example.com",
		Provider:   "gmail",
		FromEmail:  "groupupdates@facebookmail.com",
		Domain:     "facebookmail.com",
		ReceivedAt: time.Now().UTC(),
	})

	if err := RunMailboxClassification(ctx, st, "new@example.com", []models.MessageMeta{msg}); err != nil {
		t.Fatalf("RunMailboxClassification: %v", err)
	}

	classification := mustGetClassification(t, st, "baseline-msg", "new@example.com")
	if classification.Category != CategorySocial {
		t.Fatalf("category: got %q, want %q", classification.Category, CategorySocial)
	}
	if classification.MatchedRule != "domain:facebookmail.com" {
		t.Fatalf("matched rule: got %q", classification.MatchedRule)
	}
}

func TestRunMailboxClassification_RerunOverwritesPriorResults(t *testing.T) {
	st := newClassificationStore(t)
	ctx := context.Background()
	createClassificationMailbox(t, st, "alpha@example.com")

	mustInsertSeed(t, st, storage.ClassificationSeed{
		PatternType:  PatternDomain,
		PatternValue: "example.com",
		Category:     CategoryVendor,
		Source:       SourceSeed,
		Priority:     100,
	})

	msg := mustStoreMessage(t, st, models.MessageMeta{
		ProviderID: "rerun-msg",
		MailboxID:  "alpha@example.com",
		Provider:   "gmail",
		FromEmail:  "person@example.com",
		Domain:     "example.com",
		ReceivedAt: time.Now().UTC(),
	})

	if err := RunMailboxClassification(ctx, st, "alpha@example.com", []models.MessageMeta{msg}); err != nil {
		t.Fatalf("first RunMailboxClassification: %v", err)
	}

	mustInsertSeed(t, st, storage.ClassificationSeed{
		MailboxID:    "alpha@example.com",
		PatternType:  PatternSenderEmail,
		PatternValue: "person@example.com",
		Category:     CategoryClient,
		Source:       SourceOperator,
		Priority:     50,
	})

	if err := RunMailboxClassification(ctx, st, "alpha@example.com", []models.MessageMeta{msg}); err != nil {
		t.Fatalf("second RunMailboxClassification: %v", err)
	}

	classification := mustGetClassification(t, st, "rerun-msg", "alpha@example.com")
	if classification.Category != CategoryClient {
		t.Fatalf("category after rerun: got %q, want %q", classification.Category, CategoryClient)
	}
	if classification.MatchedRule != "sender_email:person@example.com" {
		t.Fatalf("matched rule after rerun: got %q", classification.MatchedRule)
	}
}

func TestRunMailboxClassification_MessageIDScopedByMailbox(t *testing.T) {
	st := newClassificationStore(t)
	ctx := context.Background()
	createClassificationMailbox(t, st, "alpha@example.com")
	createClassificationMailbox(t, st, "beta@example.com")

	mustInsertSeed(t, st, storage.ClassificationSeed{
		MailboxID:    "alpha@example.com",
		PatternType:  PatternSenderEmail,
		PatternValue: "alpha@example.com",
		Category:     CategoryClient,
		Source:       SourceOperator,
		Priority:     50,
	})
	mustInsertSeed(t, st, storage.ClassificationSeed{
		MailboxID:    "beta@example.com",
		PatternType:  PatternSenderEmail,
		PatternValue: "beta@example.com",
		Category:     CategoryVendor,
		Source:       SourceOperator,
		Priority:     50,
	})

	alphaMsg := mustStoreMessage(t, st, models.MessageMeta{
		ProviderID: "shared-id",
		MailboxID:  "alpha@example.com",
		Provider:   "gmail",
		FromEmail:  "alpha@example.com",
		Domain:     "example.com",
		ReceivedAt: time.Now().UTC(),
	})
	betaMsg := models.MessageMeta{
		ProviderID: "shared-id",
		MailboxID:  "beta@example.com",
		Provider:   "gmail",
		FromEmail:  "beta@example.com",
		Domain:     "example.com",
		ReceivedAt: time.Now().UTC(),
	}

	if err := RunMailboxClassification(ctx, st, "alpha@example.com", []models.MessageMeta{alphaMsg}); err != nil {
		t.Fatalf("RunMailboxClassification alpha: %v", err)
	}
	if err := RunMailboxClassification(ctx, st, "beta@example.com", []models.MessageMeta{betaMsg}); err != nil {
		t.Fatalf("RunMailboxClassification beta: %v", err)
	}

	alphaClassification := mustGetClassification(t, st, "shared-id", "alpha@example.com")
	if alphaClassification.Category != CategoryClient {
		t.Fatalf("alpha category: got %q, want %q", alphaClassification.Category, CategoryClient)
	}

	betaClassification := mustGetClassification(t, st, "shared-id", "beta@example.com")
	if betaClassification.Category != CategoryVendor {
		t.Fatalf("beta category: got %q, want %q", betaClassification.Category, CategoryVendor)
	}
}

func TestRunMailboxClassification_RejectsCrossMailboxMessages(t *testing.T) {
	st := newClassificationStore(t)
	ctx := context.Background()
	createClassificationMailbox(t, st, "alpha@example.com")
	createClassificationMailbox(t, st, "beta@example.com")

	msg := mustStoreMessage(t, st, models.MessageMeta{
		ProviderID: "cross-mailbox",
		MailboxID:  "beta@example.com",
		Provider:   "gmail",
		FromEmail:  "person@example.com",
		Domain:     "example.com",
		ReceivedAt: time.Now().UTC(),
	})

	err := RunMailboxClassification(ctx, st, "alpha@example.com", []models.MessageMeta{msg})
	if err == nil {
		t.Fatal("expected mailbox mismatch error")
	}

	got, getErr := st.GetClassification(ctx, "cross-mailbox", "alpha@example.com")
	if getErr != nil {
		t.Fatalf("GetClassification: %v", getErr)
	}
	if got != nil {
		t.Fatal("expected no persisted classification on mailbox mismatch")
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

func newClassificationStore(t *testing.T) *storage.Store {
	t.Helper()
	st, err := storage.Open(":memory:")
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func createClassificationMailbox(t *testing.T, st *storage.Store, id string) {
	t.Helper()
	if err := st.CreateMailbox(context.Background(), models.Mailbox{ID: id, Provider: "gmail"}); err != nil {
		t.Fatalf("CreateMailbox(%q): %v", id, err)
	}
}

func mustInsertSeed(t *testing.T, st *storage.Store, seed storage.ClassificationSeed) {
	t.Helper()
	if err := st.InsertSeed(context.Background(), seed); err != nil {
		t.Fatalf("InsertSeed(%q): %v", seed.PatternValue, err)
	}
}

func mustStoreMessage(t *testing.T, st *storage.Store, msg models.MessageMeta) models.MessageMeta {
	t.Helper()
	if err := st.UpsertMessage(context.Background(), msg); err != nil {
		t.Fatalf("UpsertMessage(%q): %v", msg.ProviderID, err)
	}
	return msg
}

func mustGetClassification(t *testing.T, st *storage.Store, messageID, mailboxID string) *storage.Classification {
	t.Helper()
	classification, err := st.GetClassification(context.Background(), messageID, mailboxID)
	if err != nil {
		t.Fatalf("GetClassification(%q, %q): %v", messageID, mailboxID, err)
	}
	if classification == nil {
		t.Fatalf("expected classification for %q / %q", messageID, mailboxID)
	}
	return classification
}

// Ensure the package compiles with the correct time import.
var _ = time.Now
