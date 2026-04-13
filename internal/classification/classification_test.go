package classification

import (
	"context"
	"os/exec"
	"strings"
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

func insertBootstrapDomainStat(t *testing.T, st *storage.Store, mailboxID, domain string, count int) {
	t.Helper()
	if err := st.UpsertDomainStat(context.Background(), mailboxID, domain, count); err != nil {
		t.Fatalf("insert domain stat %s/%s: %v", mailboxID, domain, err)
	}
}

func insertBootstrapSenderStat(t *testing.T, st *storage.Store, mailboxID, email, domain string, count int) {
	t.Helper()
	if err := st.UpsertSenderStat(context.Background(), mailboxID, email, "", domain, count); err != nil {
		t.Fatalf("insert sender stat %s/%s: %v", mailboxID, email, err)
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

func TestSeedRuleClassifier_HasAttachmentMatch(t *testing.T) {
	seeds := []ClassificationSeed{
		{ID: 1, PatternType: PatternHasAttachment, PatternValue: "true", Category: CategoryVendor, Source: SourceSeed, Priority: 100},
	}
	c := NewSeedRuleClassifier(seeds)
	result, err := c.Classify(context.Background(), models.MessageMeta{HasAttachment: true})
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if result.Category != CategoryVendor {
		t.Fatalf("Category: got %q, want %q", result.Category, CategoryVendor)
	}
}

func TestIntentRuleClassifier_SubjectTermMatch(t *testing.T) {
	seeds := []ClassificationSeed{
		{ID: 1, PatternType: PatternSubjectTerm, PatternValue: "invoice", Category: IntentInvoice, Source: SourceSeed, Priority: 100},
	}
	c := NewIntentRuleClassifier(seeds)
	result, err := c.Classify(context.Background(), makeMsg("user@example.com", "example.com", "Invoice #12345 due"))
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if result.Intent != IntentInvoice {
		t.Fatalf("Intent: got %q, want %q", result.Intent, IntentInvoice)
	}
	if result.Category != CategoryUnknown {
		t.Fatalf("Category: got %q, want %q", result.Category, CategoryUnknown)
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

func TestSeedRuleClassifier_LabelMatch(t *testing.T) {
	seeds := []ClassificationSeed{
		{ID: 1, PatternType: PatternLabel, PatternValue: "CATEGORY_PROMOTIONS", Category: CategoryNewsletterMarketing, Source: SourceSeed, Priority: 100},
	}
	c := NewSeedRuleClassifier(seeds)
	msg := makeMsg("user@example.com", "example.com", "")
	msg.Labels = []string{"CATEGORY_PROMOTIONS"}

	result, err := c.Classify(context.Background(), msg)
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if result.Category != CategoryNewsletterMarketing {
		t.Errorf("Category: got %q, want %q", result.Category, CategoryNewsletterMarketing)
	}
}

func TestSeedRuleClassifier_LabelMatchCaseInsensitive(t *testing.T) {
	seeds := []ClassificationSeed{
		{ID: 1, PatternType: PatternLabel, PatternValue: "category_promotions", Category: CategoryNewsletterMarketing, Source: SourceSeed, Priority: 100},
	}
	c := NewSeedRuleClassifier(seeds)
	msg := makeMsg("user@example.com", "example.com", "")
	msg.Labels = []string{"CATEGORY_PROMOTIONS"}

	result, err := c.Classify(context.Background(), msg)
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if result.Category != CategoryNewsletterMarketing {
		t.Errorf("Category: got %q, want %q", result.Category, CategoryNewsletterMarketing)
	}
}

func TestSeedRuleClassifier_LabelNoMatchWhenEmpty(t *testing.T) {
	seeds := []ClassificationSeed{
		{ID: 1, PatternType: PatternLabel, PatternValue: "INBOX", Category: CategoryInternal, Source: SourceSeed, Priority: 100},
	}
	c := NewSeedRuleClassifier(seeds)
	msg := makeMsg("user@example.com", "example.com", "")

	result, err := c.Classify(context.Background(), msg)
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if result.Category != CategoryUnknown {
		t.Errorf("Category: got %q, want %q", result.Category, CategoryUnknown)
	}
}

func TestSeedRuleClassifier_LabelWrongValueNoMatch(t *testing.T) {
	seeds := []ClassificationSeed{
		{ID: 1, PatternType: PatternLabel, PatternValue: "SENT", Category: CategoryInternal, Source: SourceSeed, Priority: 100},
	}
	c := NewSeedRuleClassifier(seeds)
	msg := makeMsg("user@example.com", "example.com", "")
	msg.Labels = []string{"INBOX"}

	result, err := c.Classify(context.Background(), msg)
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if result.Category != CategoryUnknown {
		t.Errorf("Category: got %q, want %q", result.Category, CategoryUnknown)
	}
}

func TestAIInferenceClassifier_SkipsDeterministicallyClassifiedMessage(t *testing.T) {
	classifier := NewAIInferenceClassifier(
		map[string]string{"m1": CategoryVendor},
		map[string]InferenceCandidate{"m1": {
			MessageID:      "m1",
			Category:       CategoryClient,
			Confidence:     0.91,
			ConfidenceBand: "high",
		}},
	)

	result, err := classifier.Classify(context.Background(), models.MessageMeta{ProviderID: "m1"})
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if result.Category != CategoryUnknown {
		t.Fatalf("expected deterministic shortcut to keep unknown AI result, got %+v", result)
	}
}

func TestAIInferenceClassifier_ReturnsCandidateForUnknownMessage(t *testing.T) {
	classifier := NewAIInferenceClassifier(
		map[string]string{"m1": CategoryUnknown},
		map[string]InferenceCandidate{"m1": {
			MessageID:      "m1",
			Category:       CategoryClient,
			Confidence:     0.76,
			ConfidenceBand: "medium",
		}},
	)

	result, err := classifier.Classify(context.Background(), models.MessageMeta{ProviderID: "m1"})
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if result.Category != CategoryClient || result.Source != SourceAI {
		t.Fatalf("unexpected AI classification result: %+v", result)
	}
}

func TestValidateInferenceCandidate(t *testing.T) {
	requests := map[string]InferenceRequest{
		"m1": {MessageID: "m1"},
	}

	valid := InferenceCandidate{
		MessageID:      "m1",
		Category:       CategoryClient,
		Confidence:     0.81,
		ConfidenceBand: "high",
		ReviewRequired: false,
	}
	if err := ValidateInferenceCandidate(valid, requests); err != nil {
		t.Fatalf("ValidateInferenceCandidate(valid): %v", err)
	}

	for _, tc := range []struct {
		name      string
		candidate InferenceCandidate
	}{
		{
			name: "unknown message",
			candidate: InferenceCandidate{
				MessageID:      "missing",
				Category:       CategoryClient,
				Confidence:     0.81,
				ConfidenceBand: "high",
			},
		},
		{
			name: "invalid category",
			candidate: InferenceCandidate{
				MessageID:      "m1",
				Category:       "not-valid",
				Confidence:     0.81,
				ConfidenceBand: "high",
			},
		},
		{
			name: "invalid confidence",
			candidate: InferenceCandidate{
				MessageID:      "m1",
				Category:       CategoryClient,
				Confidence:     1.2,
				ConfidenceBand: "high",
			},
		},
		{
			name: "inconsistent band",
			candidate: InferenceCandidate{
				MessageID:      "m1",
				Category:       CategoryClient,
				Confidence:     0.40,
				ConfidenceBand: "high",
			},
		},
		{
			name: "inconsistent review flag",
			candidate: InferenceCandidate{
				MessageID:      "m1",
				Category:       CategoryClient,
				Confidence:     0.60,
				ConfidenceBand: "medium",
				ReviewRequired: false,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateInferenceCandidate(tc.candidate, requests); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestInferenceConfidenceBand(t *testing.T) {
	if got := InferenceConfidenceBand(0.80); got != "high" {
		t.Fatalf("0.80 band: got %q", got)
	}
	if got := InferenceConfidenceBand(0.55); got != "medium" {
		t.Fatalf("0.55 band: got %q", got)
	}
	if got := InferenceConfidenceBand(0.54); got != "low" {
		t.Fatalf("0.54 band: got %q", got)
	}
}

func TestCommandInferenceProvider(t *testing.T) {
	provider := testCommandInferenceProvider("valid")
	candidates, err := provider.Infer(t.Context(), []InferenceRequest{{MessageID: "m1"}})
	if err != nil {
		t.Fatalf("Infer: %v", err)
	}
	if len(candidates) != 1 || candidates[0].MessageID != "m1" {
		t.Fatalf("unexpected candidates: %+v", candidates)
	}
}

func TestCommandInferenceProvider_InvalidJSON(t *testing.T) {
	provider := testCommandInferenceProvider("invalid-json")
	_, err := provider.Infer(t.Context(), []InferenceRequest{{MessageID: "m1"}})
	if err == nil {
		t.Fatal("expected invalid json error")
	}
	if !strings.Contains(err.Error(), "parse inference output") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCommandInferenceProvider_ExitErrorIncludesStderr(t *testing.T) {
	provider := testCommandInferenceProvider("stderr-exit")
	_, err := provider.Infer(t.Context(), []InferenceRequest{{MessageID: "m1"}})
	if err == nil {
		t.Fatal("expected provider failure")
	}
	if !strings.Contains(err.Error(), "debug trace") {
		t.Fatalf("expected stderr in error, got %v", err)
	}
}

func TestCommandInferenceProvider_RequiresCommand(t *testing.T) {
	_, err := (CommandInferenceProvider{}).Infer(t.Context(), nil)
	if err == nil {
		t.Fatal("expected missing command error")
	}
}

func testCommandInferenceProvider(mode string) CommandInferenceProvider {
	if _, err := exec.LookPath("sh"); err == nil {
		return CommandInferenceProvider{
			Command: "sh",
			Args:    []string{"-c", testInferenceProviderScript(mode)},
		}
	}
	return CommandInferenceProvider{
		Command: "cmd",
		Args:    []string{"/d", "/c", testInferenceProviderWindows(mode)},
	}
}

func testInferenceProviderScript(mode string) string {
	switch mode {
	case "valid":
		return `cat >/dev/null; cat <<'EOF'
[{"message_id":"m1","category":"client","confidence":0.81,"confidence_band":"high","review_required":false,"evidence":{"subject_phrases":["invoice"],"snippet_phrases":["payment"],"sender_signal":"known sender","domain_signal":"known domain","label_signals":["INBOX"]}}]
EOF`
	case "invalid-json":
		return `cat >/dev/null; printf 'not-json'`
	case "stderr-exit":
		return `cat >/dev/null; printf 'debug trace\n' >&2; exit 9`
	default:
		panic("unknown provider mode: " + mode)
	}
}

func testInferenceProviderWindows(mode string) string {
	switch mode {
	case "valid":
		return `more >nul & echo [{"message_id":"m1","category":"client","confidence":0.81,"confidence_band":"high","review_required":false,"evidence":{"subject_phrases":["invoice"],"snippet_phrases":["payment"],"sender_signal":"known sender","domain_signal":"known domain","label_signals":["INBOX"]}}]`
	case "invalid-json":
		return `more >nul & <nul set /p =not-json`
	case "stderr-exit":
		return `more >nul & >&2 echo debug trace & exit /b 9`
	default:
		panic("unknown provider mode: " + mode)
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
		PatternSenderPrefix: true, PatternLabel: true, PatternHasAttachment: true, PatternSubjectTerm: true,
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

func TestDefaultIntents_InvoiceSubjectTerm(t *testing.T) {
	seeds := DefaultIntents()
	c := NewIntentRuleClassifier(seeds)
	result, err := c.Classify(context.Background(), makeMsg("billing@vendor.example", "vendor.example", "Invoice #2041"))
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if result.Intent != IntentInvoice {
		t.Fatalf("Intent: got %q, want %q", result.Intent, IntentInvoice)
	}
}

func TestMailboxBootstrapSuggestions_AreMailboxScoped(t *testing.T) {
	st := newClassificationStore(t)
	ctx := context.Background()
	createClassificationMailbox(t, st, "owner@example.com")
	createClassificationMailbox(t, st, "other@example.com")
	insertBootstrapDomainStat(t, st, "owner@example.com", "healthymd.com", 7)
	insertBootstrapDomainStat(t, st, "other@example.com", "other.example.com", 9)

	suggestions, err := MailboxBootstrapSuggestions(ctx, st, "owner@example.com", 5)
	if err != nil {
		t.Fatalf("MailboxBootstrapSuggestions: %v", err)
	}
	if len(suggestions) != 1 {
		t.Fatalf("expected 1 scoped suggestion, got %d", len(suggestions))
	}
	if suggestions[0].MailboxID != "owner@example.com" {
		t.Fatalf("suggestion mailbox_id: got %q, want %q", suggestions[0].MailboxID, "owner@example.com")
	}
	if suggestions[0].PatternValue != "healthymd.com" {
		t.Fatalf("PatternValue: got %q, want %q", suggestions[0].PatternValue, "healthymd.com")
	}
}

func TestMailboxBootstrapSuggestions_EmptyMailboxID(t *testing.T) {
	suggestions, err := MailboxBootstrapSuggestions(context.Background(), newClassificationStore(t), "", 5)
	if err != nil {
		t.Fatalf("MailboxBootstrapSuggestions: %v", err)
	}
	if len(suggestions) != 0 {
		t.Fatalf("expected no suggestions for empty mailbox id; got %d", len(suggestions))
	}
}

func TestMailboxBootstrapSuggestions_RequiresStore(t *testing.T) {
	_, err := MailboxBootstrapSuggestions(context.Background(), nil, "owner@example.com", 5)
	if err == nil {
		t.Fatal("expected nil-store error")
	}
}

func TestMailboxBootstrapSuggestions_MinCountDefaultsToOne(t *testing.T) {
	st := newClassificationStore(t)
	ctx := context.Background()
	createClassificationMailbox(t, st, "owner@example.com")
	insertBootstrapDomainStat(t, st, "owner@example.com", "single-hit.example", 1)

	suggestions, err := MailboxBootstrapSuggestions(ctx, st, "owner@example.com", 0)
	if err != nil {
		t.Fatalf("MailboxBootstrapSuggestions: %v", err)
	}
	if len(suggestions) != 1 || suggestions[0].PatternValue != "single-hit.example" {
		t.Fatalf("unexpected suggestions: %+v", suggestions)
	}
}

func TestMailboxBootstrapSuggestions_AppliesThresholdAndDefaultDedup(t *testing.T) {
	st := newClassificationStore(t)
	ctx := context.Background()
	createClassificationMailbox(t, st, "owner@example.com")
	insertBootstrapDomainStat(t, st, "owner@example.com", "healthymd.com", 6)
	insertBootstrapDomainStat(t, st, "owner@example.com", "facebookmail.com", 8)
	insertBootstrapDomainStat(t, st, "owner@example.com", "below-threshold.example", 4)
	insertBootstrapSenderStat(t, st, "owner@example.com", "owner@healthymd.com", "healthymd.com", 6)
	insertBootstrapSenderStat(t, st, "owner@example.com", "calendar-notification@google.com", "google.com", 9)

	suggestions, err := MailboxBootstrapSuggestions(ctx, st, "owner@example.com", 5)
	if err != nil {
		t.Fatalf("MailboxBootstrapSuggestions: %v", err)
	}

	if len(suggestions) != 2 {
		t.Fatalf("expected 2 suggestions after thresholding and dedup, got %d: %+v", len(suggestions), suggestions)
	}

	want := map[string]string{
		PatternDomain + ":" + "healthymd.com":            CategoryUnknown,
		PatternSenderEmail + ":" + "owner@healthymd.com": CategoryUnknown,
	}
	for _, suggestion := range suggestions {
		key := suggestion.PatternType + ":" + suggestion.PatternValue
		if suggestion.Source != SourceSeed {
			t.Fatalf("Source for %s: got %q, want %q", key, suggestion.Source, SourceSeed)
		}
		if suggestion.Priority != 100 {
			t.Fatalf("Priority for %s: got %d, want 100", key, suggestion.Priority)
		}
		if suggestion.Category != CategoryUnknown {
			t.Fatalf("Category for %s: got %q, want %q", key, suggestion.Category, CategoryUnknown)
		}
		if _, ok := want[key]; !ok {
			t.Fatalf("unexpected suggestion %s", key)
		}
		delete(want, key)
	}
	if len(want) != 0 {
		t.Fatalf("missing suggestions: %+v", want)
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

func TestRunMailboxClassification_RequiresMailboxID(t *testing.T) {
	st := newClassificationStore(t)
	err := RunMailboxClassification(context.Background(), st, "", nil)
	if err == nil {
		t.Fatal("expected mailboxID-required error")
	}
}

func TestRunMailboxClassification_RejectsMissingProviderID(t *testing.T) {
	st := newClassificationStore(t)
	ctx := context.Background()
	createClassificationMailbox(t, st, "alpha@example.com")

	msg := mustStoreMessage(t, st, models.MessageMeta{
		ID:         "m1",
		MailboxID:  "alpha@example.com",
		Provider:   "gmail",
		FromEmail:  "person@example.com",
		Domain:     "example.com",
		ReceivedAt: time.Now().UTC(),
	})

	err := RunMailboxClassification(ctx, st, "alpha@example.com", []models.MessageMeta{msg})
	if err == nil {
		t.Fatal("expected providerID-required error")
	}
}

func TestSpecificityRank_UnknownTypeFallsBackLast(t *testing.T) {
	if got := specificityRank("mystery"); got != 7 {
		t.Fatalf("specificityRank: got %d, want 7", got)
	}
}

func TestSpecificityRank_KnownTypes(t *testing.T) {
	tests := []struct {
		patternType string
		want        int
	}{
		{PatternSenderEmail, 1},
		{PatternSenderPrefix, 2},
		{PatternDomain, 3},
		{PatternLabel, 4},
		{PatternHasAttachment, 5},
		{PatternSubjectTerm, 6},
	}

	for _, tt := range tests {
		if got := specificityRank(tt.patternType); got != tt.want {
			t.Fatalf("specificityRank(%q): got %d, want %d", tt.patternType, got, tt.want)
		}
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

// --- NormalizeSubject ---

func TestNormalizeSubject(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"Re: Invoice #123", "Invoice #123"},
		{"RE: Invoice #123", "Invoice #123"},
		{"re: invoice", "invoice"},
		{"Fw: Re: Payment due", "Payment due"},
		{"FWD: Fwd: Hello", "Hello"},
		{"Re[2]: Budget review", "Budget review"},
		{"Re[10]: status update", "status update"},
		{"Invoice (no prefix)", "Invoice (no prefix)"},
		{"  Re:   Trimmed  ", "Trimmed"},
		{"Re:", ""},
		// No prefix — returned as-is (trimmed).
		{"Hello World", "Hello World"},
		// Already normalized — idempotent.
		{"Invoice", "Invoice"},
		// Aw: German-style forward prefix.
		{"AW: Some subject", "Some subject"},
		// Nested Re:/FW:.
		{"Re: Fw: Re: Deep nesting", "Deep nesting"},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got := NormalizeSubject(tc.input)
			if got != tc.want {
				t.Errorf("NormalizeSubject(%q) = %q, want %q", tc.input, got, tc.want)
			}
			// Idempotence: calling twice must return the same result.
			got2 := NormalizeSubject(got)
			if got2 != got {
				t.Errorf("NormalizeSubject not idempotent: second call on %q returned %q", got, got2)
			}
		})
	}
}

// --- SubjectRuleClassifier ---

func TestSubjectRuleClassifier_InclusionMatch(t *testing.T) {
	rules := []SubjectRule{
		{IncludeKeywords: []string{"invoice"}, Category: CategoryVendor, Priority: 100},
	}
	c := NewSubjectRuleClassifier(rules)
	result, err := c.Classify(context.Background(), makeMsg("", "", "Invoice #42"))
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if result.Category != CategoryVendor {
		t.Errorf("Category: got %q, want %q", result.Category, CategoryVendor)
	}
	if !strings.HasPrefix(result.MatchedRule, "subject_rule:") {
		t.Errorf("MatchedRule: got %q, want prefix %q", result.MatchedRule, "subject_rule:")
	}
	if result.Source != SourceOperator {
		t.Errorf("Source: got %q, want %q", result.Source, SourceOperator)
	}
}

func TestSubjectRuleClassifier_NormalizedSubjectUsed(t *testing.T) {
	// "Re: invoice" — raw subject has prefix; normalization must strip it before matching.
	rules := []SubjectRule{
		{IncludeKeywords: []string{"invoice"}, Category: CategoryVendor, Priority: 100},
	}
	c := NewSubjectRuleClassifier(rules)
	result, err := c.Classify(context.Background(), makeMsg("", "", "Re: invoice"))
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if result.Category != CategoryVendor {
		t.Errorf("Category: got %q, want %q", result.Category, CategoryVendor)
	}
}

func TestSubjectRuleClassifier_ExclusionBlocksMatch(t *testing.T) {
	rules := []SubjectRule{
		{IncludeKeywords: []string{"invoice"}, ExcludeKeywords: []string{"office"}, Category: CategoryVendor, Priority: 100},
	}
	c := NewSubjectRuleClassifier(rules)
	// "office" is present — should not match.
	result, err := c.Classify(context.Background(), makeMsg("", "", "invoice office update"))
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if result.Category != CategoryUnknown {
		t.Errorf("Category: got %q, want %q (exclusion should block)", result.Category, CategoryUnknown)
	}
}

func TestSubjectRuleClassifier_ExclusionKeywordAbsent(t *testing.T) {
	// Exclude keyword NOT present — inclusion match should succeed.
	rules := []SubjectRule{
		{IncludeKeywords: []string{"invoice"}, ExcludeKeywords: []string{"office"}, Category: CategoryVendor, Priority: 100},
	}
	c := NewSubjectRuleClassifier(rules)
	result, err := c.Classify(context.Background(), makeMsg("", "", "invoice payment"))
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if result.Category != CategoryVendor {
		t.Errorf("Category: got %q, want %q", result.Category, CategoryVendor)
	}
}

func TestSubjectRuleClassifier_NoMatch(t *testing.T) {
	rules := []SubjectRule{
		{IncludeKeywords: []string{"invoice"}, Category: CategoryVendor, Priority: 100},
	}
	c := NewSubjectRuleClassifier(rules)
	result, err := c.Classify(context.Background(), makeMsg("", "", "meeting agenda"))
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if result.Category != CategoryUnknown {
		t.Errorf("Category: got %q, want %q", result.Category, CategoryUnknown)
	}
}

func TestSubjectRuleClassifier_PriorityOrdering(t *testing.T) {
	// Lower Priority value = evaluated first; first match wins.
	rules := []SubjectRule{
		{IncludeKeywords: []string{"invoice"}, Category: CategoryClient, Priority: 200},
		{IncludeKeywords: []string{"invoice"}, Category: CategoryVendor, Priority: 100},
	}
	c := NewSubjectRuleClassifier(rules)
	result, err := c.Classify(context.Background(), makeMsg("", "", "invoice due"))
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	// Priority 100 rule should win.
	if result.Category != CategoryVendor {
		t.Errorf("Category: got %q, want %q (priority 100 rule should win)", result.Category, CategoryVendor)
	}
}

func TestSubjectRuleClassifier_EmptyIncludeKeywordsNeverMatches(t *testing.T) {
	rules := []SubjectRule{
		{IncludeKeywords: nil, Category: CategoryVendor, Priority: 100},
	}
	c := NewSubjectRuleClassifier(rules)
	result, err := c.Classify(context.Background(), makeMsg("", "", "invoice due"))
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if result.Category != CategoryUnknown {
		t.Errorf("Category: got %q, want %q (empty IncludeKeywords must not match)", result.Category, CategoryUnknown)
	}
}

func TestSubjectRuleClassifier_MultipleIncludeKeywordsOR(t *testing.T) {
	// Any one of the include keywords is sufficient.
	rules := []SubjectRule{
		{IncludeKeywords: []string{"invoice", "payment"}, Category: CategoryVendor, Priority: 100},
	}
	c := NewSubjectRuleClassifier(rules)
	for _, subject := range []string{"invoice notice", "payment due", "invoice and payment"} {
		result, err := c.Classify(context.Background(), makeMsg("", "", subject))
		if err != nil {
			t.Fatalf("Classify(%q): %v", subject, err)
		}
		if result.Category != CategoryVendor {
			t.Errorf("Classify(%q): got %q, want %q", subject, result.Category, CategoryVendor)
		}
	}
}

func TestSubjectRuleClassifier_CaseInsensitiveKeywords(t *testing.T) {
	rules := []SubjectRule{
		{IncludeKeywords: []string{"INVOICE"}, Category: CategoryVendor, Priority: 100},
	}
	c := NewSubjectRuleClassifier(rules)
	result, err := c.Classify(context.Background(), makeMsg("", "", "invoice notice"))
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if result.Category != CategoryVendor {
		t.Errorf("Category: got %q, want %q", result.Category, CategoryVendor)
	}
}

func TestSubjectRuleClassifier_EmptyRuleList(t *testing.T) {
	c := NewSubjectRuleClassifier(nil)
	result, err := c.Classify(context.Background(), makeMsg("", "", "invoice due"))
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if result.Category != CategoryUnknown {
		t.Errorf("Category: got %q, want %q (empty rules must return unknown)", result.Category, CategoryUnknown)
	}
}
