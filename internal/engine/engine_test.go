package engine

import (
	"context"
	"math"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/UreaLaden/inboxatlas/internal/classification"
	"github.com/UreaLaden/inboxatlas/internal/config"
	"github.com/UreaLaden/inboxatlas/internal/storage"
	"github.com/UreaLaden/inboxatlas/pkg/models"
)

func TestRunClassify_UsesBaselineDefaults(t *testing.T) {
	cfg := engineTestConfig(t)
	st := engineTestStore(t, cfg)
	createEngineMailbox(t, st, "user@example.com")
	engineSeedMessage(t, st, models.MessageMeta{
		ProviderID: "m1",
		MailboxID:  "user@example.com",
		Provider:   "gmail",
		FromEmail:  "groupupdates@facebookmail.com",
		Domain:     "facebookmail.com",
		ReceivedAt: time.Now().UTC(),
	})

	result, err := RunClassify(context.Background(), cfg, "user@example.com")
	if err != nil {
		t.Fatalf("RunClassify: %v", err)
	}
	if result.MessagesProcessed != 1 {
		t.Fatalf("MessagesProcessed: got %d, want 1", result.MessagesProcessed)
	}
	if len(result.Breakdown) != 1 {
		t.Fatalf("expected 1 breakdown row, got %d", len(result.Breakdown))
	}
	if result.Breakdown[0] != (storage.ClassificationCount{Category: classification.CategorySocial, Count: 1}) {
		t.Fatalf("unexpected breakdown row: %+v", result.Breakdown[0])
	}
	if result.UnknownPct != 0 {
		t.Fatalf("UnknownPct: got %v, want 0", result.UnknownPct)
	}

	got, err := st.GetClassification(context.Background(), "m1", "user@example.com")
	if err != nil {
		t.Fatalf("GetClassification: %v", err)
	}
	if got == nil || got.Category != classification.CategorySocial {
		t.Fatalf("expected social classification, got %+v", got)
	}
}

func TestRunClassify_PopulatesBreakdownAndUnknownPct(t *testing.T) {
	cfg := engineTestConfig(t)
	st := engineTestStore(t, cfg)
	createEngineMailbox(t, st, "user@example.com")
	engineSeedMessage(t, st, models.MessageMeta{
		ProviderID: "m1",
		MailboxID:  "user@example.com",
		Provider:   "gmail",
		FromEmail:  "groupupdates@facebookmail.com",
		Domain:     "facebookmail.com",
		ReceivedAt: time.Now().UTC(),
	})
	engineSeedMessage(t, st, models.MessageMeta{
		ProviderID: "m2",
		MailboxID:  "user@example.com",
		Provider:   "gmail",
		FromEmail:  "unknown@example.com",
		Domain:     "example.com",
		ReceivedAt: time.Now().UTC(),
	})

	result, err := RunClassify(context.Background(), cfg, "user@example.com")
	if err != nil {
		t.Fatalf("RunClassify: %v", err)
	}
	if result.MessagesProcessed != 2 {
		t.Fatalf("MessagesProcessed: got %d, want 2", result.MessagesProcessed)
	}
	if len(result.Breakdown) != 2 {
		t.Fatalf("expected 2 breakdown rows, got %d", len(result.Breakdown))
	}
	if result.Breakdown[0] != (storage.ClassificationCount{Category: classification.CategorySocial, Count: 1}) {
		t.Fatalf("first breakdown row: %+v", result.Breakdown[0])
	}
	if result.Breakdown[1] != (storage.ClassificationCount{Category: classification.CategoryUnknown, Count: 1}) {
		t.Fatalf("second breakdown row: %+v", result.Breakdown[1])
	}
	if math.Abs(result.UnknownPct-50.0) > 0.000001 {
		t.Fatalf("UnknownPct: got %v, want 50", result.UnknownPct)
	}
}

func TestEnsureDefaultSeeds_UpdatesExistingSeed(t *testing.T) {
	cfg := engineTestConfig(t)
	st := engineTestStore(t, cfg)
	createEngineMailbox(t, st, "user@example.com")

	defaultSeed := classification.DefaultSeeds()[0]
	if err := st.InsertSeed(context.Background(), storage.ClassificationSeed{
		PatternType:  defaultSeed.PatternType,
		PatternValue: defaultSeed.PatternValue,
		Category:     classification.CategoryUnknown,
		Source:       classification.SourceOperator,
		Priority:     defaultSeed.Priority + 50,
	}); err != nil {
		t.Fatalf("InsertSeed: %v", err)
	}

	if err := ensureDefaultSeeds(context.Background(), st); err != nil {
		t.Fatalf("ensureDefaultSeeds: %v", err)
	}

	seeds, err := st.ListSeeds(context.Background(), "")
	if err != nil {
		t.Fatalf("ListSeeds: %v", err)
	}

	var (
		got   storage.ClassificationSeed
		found bool
	)
	for i := range seeds {
		if seeds[i].PatternType == defaultSeed.PatternType && seeds[i].PatternValue == defaultSeed.PatternValue {
			got = seeds[i]
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected default seed to exist after ensureDefaultSeeds")
	}
	if got.Category != defaultSeed.Category {
		t.Fatalf("Category after ensureDefaultSeeds: got %q, want %q", got.Category, defaultSeed.Category)
	}
	if got.Source != defaultSeed.Source {
		t.Fatalf("Source after ensureDefaultSeeds: got %q, want %q", got.Source, defaultSeed.Source)
	}
	if got.Priority != defaultSeed.Priority {
		t.Fatalf("Priority after ensureDefaultSeeds: got %d, want %d", got.Priority, defaultSeed.Priority)
	}
}

func TestEnsureDefaultSeeds_InsertsDefaults(t *testing.T) {
	cfg := engineTestConfig(t)
	st := engineTestStore(t, cfg)

	if err := ensureDefaultSeeds(context.Background(), st); err != nil {
		t.Fatalf("ensureDefaultSeeds: %v", err)
	}

	seeds, err := st.ListSeeds(context.Background(), "")
	if err != nil {
		t.Fatalf("ListSeeds: %v", err)
	}
	if len(seeds) != len(classification.DefaultSeeds()) {
		t.Fatalf("default seed count: got %d, want %d", len(seeds), len(classification.DefaultSeeds()))
	}
}

func TestEnsureDefaultSeeds_ClosedStore(t *testing.T) {
	cfg := engineTestConfig(t)
	st := engineTestStore(t, cfg)
	_ = cfg
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := ensureDefaultSeeds(context.Background(), st); err == nil {
		t.Fatal("expected ensureDefaultSeeds to fail on closed store")
	}
}

func TestRunClassify_EmptyMailbox(t *testing.T) {
	cfg := engineTestConfig(t)
	st := engineTestStore(t, cfg)
	createEngineMailbox(t, st, "user@example.com")

	_, err := RunClassify(context.Background(), cfg, "user@example.com")
	if err == nil {
		t.Fatal("expected empty mailbox error")
	}
}

func TestRunClassify_MailboxNotFound(t *testing.T) {
	cfg := engineTestConfig(t)

	_, err := RunClassify(context.Background(), cfg, "missing@example.com")
	if err == nil {
		t.Fatal("expected mailbox resolution error")
	}
}

func TestRunClassify_OpenStorageError(t *testing.T) {
	cfg := config.Default()
	cfg.StoragePath = "/dev/null/inboxatlas.db"

	_, err := RunClassify(context.Background(), cfg, "user@example.com")
	if err == nil {
		t.Fatal("expected open storage error")
	}
}

func TestRunClassify_ResolvesAlias(t *testing.T) {
	cfg := engineTestConfig(t)
	st := engineTestStore(t, cfg)
	if err := st.CreateMailbox(context.Background(), models.Mailbox{ID: "user@example.com", Alias: "work", Provider: "gmail"}); err != nil {
		t.Fatalf("CreateMailbox: %v", err)
	}
	engineSeedMessage(t, st, models.MessageMeta{
		ProviderID: "m1",
		MailboxID:  "user@example.com",
		Provider:   "gmail",
		FromEmail:  "groupupdates@facebookmail.com",
		Domain:     "facebookmail.com",
		ReceivedAt: time.Now().UTC(),
	})

	result, err := RunClassify(context.Background(), cfg, "work")
	if err != nil {
		t.Fatalf("RunClassify by alias: %v", err)
	}
	if result.MailboxID != "user@example.com" {
		t.Fatalf("MailboxID: got %q, want %q", result.MailboxID, "user@example.com")
	}
}

func TestRunClassify_PropagatesClassificationError(t *testing.T) {
	cfg := engineTestConfig(t)
	st := engineTestStore(t, cfg)
	createEngineMailbox(t, st, "user@example.com")
	engineSeedMessage(t, st, models.MessageMeta{
		ID:         "m1",
		MailboxID:  "user@example.com",
		Provider:   "gmail",
		FromEmail:  "groupupdates@facebookmail.com",
		Domain:     "facebookmail.com",
		ReceivedAt: time.Now().UTC(),
	})

	_, err := RunClassify(context.Background(), cfg, "user@example.com")
	if err == nil {
		t.Fatal("expected classification error")
	}
}

func TestListClassifySuggestions(t *testing.T) {
	cfg := engineTestConfig(t)
	st := engineTestStore(t, cfg)
	createEngineMailbox(t, st, "user@example.com")
	if err := st.UpsertDomainStat(context.Background(), "user@example.com", "healthymd.com", 6); err != nil {
		t.Fatalf("UpsertDomainStat: %v", err)
	}

	result, err := ListClassifySuggestions(context.Background(), cfg, "user@example.com")
	if err != nil {
		t.Fatalf("ListClassifySuggestions: %v", err)
	}
	if result.MailboxID != "user@example.com" {
		t.Fatalf("MailboxID: got %q", result.MailboxID)
	}
	if len(result.Suggestions) != 1 {
		t.Fatalf("expected one suggestion, got %d", len(result.Suggestions))
	}
	if result.Suggestions[0].PatternType != classification.PatternDomain || result.Suggestions[0].PatternValue != "healthymd.com" {
		t.Fatalf("unexpected suggestion: %+v", result.Suggestions[0])
	}
	if result.Suggestions[0].Category != classification.CategoryUnknown {
		t.Fatalf("Category: got %q, want %q", result.Suggestions[0].Category, classification.CategoryUnknown)
	}
}

func TestListClassifySuggestions_MailboxNotFound(t *testing.T) {
	cfg := engineTestConfig(t)

	_, err := ListClassifySuggestions(context.Background(), cfg, "missing@example.com")
	if err == nil {
		t.Fatal("expected mailbox resolution error")
	}
}

func TestListClassifySuggestions_OpenStorageError(t *testing.T) {
	cfg := config.Default()
	cfg.StoragePath = "/dev/null/inboxatlas.db"

	_, err := ListClassifySuggestions(context.Background(), cfg, "user@example.com")
	if err == nil {
		t.Fatal("expected open storage error")
	}
}

func TestListMailboxSeeds(t *testing.T) {
	cfg := engineTestConfig(t)
	st := engineTestStore(t, cfg)
	createEngineMailbox(t, st, "user@example.com")

	if err := st.InsertSeed(context.Background(), storage.ClassificationSeed{
		PatternType:  classification.PatternDomain,
		PatternValue: "global.example",
		Category:     classification.CategoryVendor,
		Source:       classification.SourceSeed,
		Priority:     100,
	}); err != nil {
		t.Fatalf("InsertSeed global: %v", err)
	}
	if err := st.InsertSeed(context.Background(), storage.ClassificationSeed{
		MailboxID:    "user@example.com",
		PatternType:  classification.PatternDomain,
		PatternValue: "mailbox.example",
		Category:     classification.CategoryClient,
		Source:       classification.SourceOperator,
		Priority:     50,
	}); err != nil {
		t.Fatalf("InsertSeed mailbox: %v", err)
	}

	seeds, err := ListMailboxSeeds(context.Background(), cfg, "user@example.com")
	if err != nil {
		t.Fatalf("ListMailboxSeeds: %v", err)
	}
	if len(seeds) != 1 {
		t.Fatalf("expected 1 mailbox-scoped seed, got %d", len(seeds))
	}
	if seeds[0].ID == 0 {
		t.Fatalf("expected mailbox seed ID to be populated, got %+v", seeds[0])
	}
	if seeds[0].MailboxID != "user@example.com" || seeds[0].PatternValue != "mailbox.example" {
		t.Fatalf("unexpected seed: %+v", seeds[0])
	}
}

func TestListMailboxSeeds_Empty(t *testing.T) {
	cfg := engineTestConfig(t)
	st := engineTestStore(t, cfg)
	createEngineMailbox(t, st, "user@example.com")

	seeds, err := ListMailboxSeeds(context.Background(), cfg, "user@example.com")
	if err != nil {
		t.Fatalf("ListMailboxSeeds: %v", err)
	}
	if len(seeds) != 0 {
		t.Fatalf("expected no mailbox seeds, got %d", len(seeds))
	}
}

func TestListMailboxSeeds_MailboxNotFound(t *testing.T) {
	cfg := engineTestConfig(t)

	_, err := ListMailboxSeeds(context.Background(), cfg, "missing@example.com")
	if err == nil {
		t.Fatal("expected mailbox resolution error")
	}
}

func TestListMailboxSeeds_OpenStorageError(t *testing.T) {
	cfg := config.Default()
	cfg.StoragePath = "/dev/null/inboxatlas.db"

	if _, err := ListMailboxSeeds(context.Background(), cfg, "user@example.com"); err == nil {
		t.Fatal("expected open storage error")
	}
}

func TestDeleteMailboxSeed_DeletesMailboxScopedSeed(t *testing.T) {
	cfg := engineTestConfig(t)
	st := engineTestStore(t, cfg)
	createEngineMailbox(t, st, "user@example.com")

	if err := st.InsertSeed(context.Background(), storage.ClassificationSeed{
		MailboxID:    "user@example.com",
		PatternType:  classification.PatternDomain,
		PatternValue: "mailbox.example",
		Category:     classification.CategoryClient,
		Source:       classification.SourceOperator,
		Priority:     50,
	}); err != nil {
		t.Fatalf("InsertSeed mailbox: %v", err)
	}
	seeds, err := st.ListSeeds(context.Background(), "user@example.com")
	if err != nil {
		t.Fatalf("ListSeeds: %v", err)
	}

	if err := DeleteMailboxSeed(context.Background(), cfg, "user@example.com", seeds[0].ID); err != nil {
		t.Fatalf("DeleteMailboxSeed: %v", err)
	}

	after, err := st.ListSeeds(context.Background(), "user@example.com")
	if err != nil {
		t.Fatalf("ListSeeds after delete: %v", err)
	}
	if len(after) != 0 {
		t.Fatalf("expected no mailbox seeds after delete, got %d", len(after))
	}
}

func TestDeleteMailboxSeed_RejectsGlobalSeed(t *testing.T) {
	cfg := engineTestConfig(t)
	st := engineTestStore(t, cfg)
	createEngineMailbox(t, st, "user@example.com")

	if err := st.InsertSeed(context.Background(), storage.ClassificationSeed{
		PatternType:  classification.PatternDomain,
		PatternValue: "global.example",
		Category:     classification.CategoryVendor,
		Source:       classification.SourceSeed,
		Priority:     100,
	}); err != nil {
		t.Fatalf("InsertSeed global: %v", err)
	}
	seeds, err := st.ListSeeds(context.Background(), "user@example.com")
	if err != nil {
		t.Fatalf("ListSeeds: %v", err)
	}

	err = DeleteMailboxSeed(context.Background(), cfg, "user@example.com", seeds[0].ID)
	if err == nil {
		t.Fatal("expected global-seed guard error")
	}
}

func TestDeleteMailboxSeed_NotFound(t *testing.T) {
	cfg := engineTestConfig(t)
	st := engineTestStore(t, cfg)
	createEngineMailbox(t, st, "user@example.com")

	err := DeleteMailboxSeed(context.Background(), cfg, "user@example.com", 999)
	if err == nil {
		t.Fatal("expected not-found error")
	}
}

func TestDeleteMailboxSeed_MailboxNotFound(t *testing.T) {
	cfg := engineTestConfig(t)

	err := DeleteMailboxSeed(context.Background(), cfg, "missing@example.com", 1)
	if err == nil {
		t.Fatal("expected mailbox resolution error")
	}
}

func TestDeleteMailboxSeed_OpenStorageError(t *testing.T) {
	cfg := config.Default()
	cfg.StoragePath = "/dev/null/inboxatlas.db"

	if err := DeleteMailboxSeed(context.Background(), cfg, "user@example.com", 1); err == nil {
		t.Fatal("expected open storage error")
	}
}

func TestGetClassificationSummary(t *testing.T) {
	cfg := engineTestConfig(t)
	st := engineTestStore(t, cfg)
	createEngineMailbox(t, st, "user@example.com")
	createEngineMailbox(t, st, "other@example.com")

	now := time.Now().UTC()
	for _, msg := range []models.MessageMeta{
		{ProviderID: "m1", MailboxID: "user@example.com", Provider: "gmail", ReceivedAt: now},
		{ProviderID: "m2", MailboxID: "user@example.com", Provider: "gmail", ReceivedAt: now},
		{ProviderID: "m3", MailboxID: "user@example.com", Provider: "gmail", ReceivedAt: now},
		{ProviderID: "m4", MailboxID: "other@example.com", Provider: "gmail", ReceivedAt: now},
	} {
		engineSeedMessage(t, st, msg)
	}

	for _, c := range []storage.Classification{
		{MessageID: "m1", MailboxID: "user@example.com", Category: classification.CategoryUnknown, Source: classification.SourceSeed, ClassifiedAt: now},
		{MessageID: "m2", MailboxID: "user@example.com", Category: classification.CategoryVendor, Source: classification.SourceSeed, ClassifiedAt: now},
		{MessageID: "m3", MailboxID: "user@example.com", Category: classification.CategoryVendor, Source: classification.SourceSeed, ClassifiedAt: now},
		{MessageID: "m4", MailboxID: "other@example.com", Category: classification.CategoryClient, Source: classification.SourceSeed, ClassifiedAt: now},
	} {
		if err := st.SaveClassification(context.Background(), c); err != nil {
			t.Fatalf("SaveClassification(%s): %v", c.MessageID, err)
		}
	}

	result, err := GetClassificationSummary(context.Background(), cfg, "user@example.com")
	if err != nil {
		t.Fatalf("GetClassificationSummary: %v", err)
	}
	if result.MailboxID != "user@example.com" {
		t.Fatalf("MailboxID: got %q", result.MailboxID)
	}
	if result.Total != 3 {
		t.Fatalf("Total: got %d, want 3", result.Total)
	}
	if len(result.Breakdown) != 2 {
		t.Fatalf("expected 2 breakdown rows, got %d", len(result.Breakdown))
	}
	if result.Breakdown[0] != (storage.ClassificationCount{Category: classification.CategoryVendor, Count: 2}) {
		t.Fatalf("first row: got %+v", result.Breakdown[0])
	}
	if result.Breakdown[1] != (storage.ClassificationCount{Category: classification.CategoryUnknown, Count: 1}) {
		t.Fatalf("second row: got %+v", result.Breakdown[1])
	}
	if math.Abs(result.UnknownPct-33.333333333333336) > 0.000001 {
		t.Fatalf("UnknownPct: got %v", result.UnknownPct)
	}
}

func TestGetClassificationSummary_Empty(t *testing.T) {
	cfg := engineTestConfig(t)
	st := engineTestStore(t, cfg)
	createEngineMailbox(t, st, "user@example.com")

	result, err := GetClassificationSummary(context.Background(), cfg, "user@example.com")
	if err != nil {
		t.Fatalf("GetClassificationSummary: %v", err)
	}
	if result.Total != 0 || len(result.Breakdown) != 0 || result.UnknownPct != 0 {
		t.Fatalf("unexpected empty summary: %+v", result)
	}
}

func TestGetClassificationSummary_MailboxNotFound(t *testing.T) {
	cfg := engineTestConfig(t)

	_, err := GetClassificationSummary(context.Background(), cfg, "missing@example.com")
	if err == nil {
		t.Fatal("expected mailbox resolution error")
	}
}

func TestRunInference_PersistsHighAndMediumCandidates(t *testing.T) {
	cfg := engineTestConfig(t)
	st := engineTestStore(t, cfg)
	createEngineMailbox(t, st, "user@example.com")
	now := time.Now().UTC()

	engineSeedMessage(t, st, models.MessageMeta{
		ProviderID: "m1",
		MailboxID:  "user@example.com",
		Provider:   "gmail",
		FromEmail:  "groupupdates@facebookmail.com",
		Domain:     "facebookmail.com",
		ReceivedAt: now,
	})
	engineSeedMessage(t, st, models.MessageMeta{
		ProviderID: "m2",
		MailboxID:  "user@example.com",
		Provider:   "gmail",
		FromEmail:  "ai@healthymd.com",
		Domain:     "healthymd.com",
		Subject:    "Invoice review",
		ReceivedAt: now.Add(time.Minute),
	})
	engineSeedMessage(t, st, models.MessageMeta{
		ProviderID: "m3",
		MailboxID:  "user@example.com",
		Provider:   "gmail",
		FromEmail:  "reply@client.example",
		Domain:     "client.example",
		Subject:    "Client follow-up",
		ReceivedAt: now.Add(2 * time.Minute),
	})
	if err := st.UpsertSenderStat(context.Background(), "user@example.com", "ai@healthymd.com", "", "healthymd.com", 4); err != nil {
		t.Fatalf("UpsertSenderStat m2: %v", err)
	}
	if err := st.UpsertDomainStat(context.Background(), "user@example.com", "healthymd.com", 4); err != nil {
		t.Fatalf("UpsertDomainStat m2: %v", err)
	}
	if err := st.UpsertSenderStat(context.Background(), "user@example.com", "reply@client.example", "", "client.example", 2); err != nil {
		t.Fatalf("UpsertSenderStat m3: %v", err)
	}
	if err := st.UpsertDomainStat(context.Background(), "user@example.com", "client.example", 2); err != nil {
		t.Fatalf("UpsertDomainStat m3: %v", err)
	}

	result, err := RunInference(context.Background(), cfg, "user@example.com", inferenceTestProviderCommand(), inferenceTestProviderArgs("persisted"))
	if err != nil {
		t.Fatalf("RunInference: %v", err)
	}
	if result.Submitted != 2 || result.High != 1 || result.Medium != 1 || result.Persisted != 2 {
		t.Fatalf("unexpected inference summary: %+v", result)
	}

	suggestions, err := st.ListInferenceSuggestions(context.Background(), "user@example.com")
	if err != nil {
		t.Fatalf("ListInferenceSuggestions: %v", err)
	}
	if len(suggestions) != 2 {
		t.Fatalf("expected 2 persisted suggestions, got %d", len(suggestions))
	}
	if suggestions[0].PatternValue != "healthymd.com" && suggestions[1].PatternValue != "healthymd.com" {
		t.Fatalf("expected healthymd.com inference suggestion, got %+v", suggestions)
	}
}

func TestRunInference_RejectsInvalidAndLowCandidates(t *testing.T) {
	cfg := engineTestConfig(t)
	st := engineTestStore(t, cfg)
	createEngineMailbox(t, st, "user@example.com")
	now := time.Now().UTC()

	for _, msg := range []models.MessageMeta{
		{ProviderID: "m1", MailboxID: "user@example.com", Provider: "gmail", FromEmail: "a@x.com", Domain: "x.com", ReceivedAt: now},
		{ProviderID: "m2", MailboxID: "user@example.com", Provider: "gmail", FromEmail: "b@y.com", Domain: "y.com", ReceivedAt: now.Add(time.Minute)},
	} {
		engineSeedMessage(t, st, msg)
	}
	if err := st.UpsertDomainStat(context.Background(), "user@example.com", "x.com", 1); err != nil {
		t.Fatalf("UpsertDomainStat x: %v", err)
	}
	if err := st.UpsertDomainStat(context.Background(), "user@example.com", "y.com", 1); err != nil {
		t.Fatalf("UpsertDomainStat y: %v", err)
	}

	result, err := RunInference(context.Background(), cfg, "user@example.com", inferenceTestProviderCommand(), inferenceTestProviderArgs("mixed"))
	if err != nil {
		t.Fatalf("RunInference: %v", err)
	}
	if result.Submitted != 2 || result.Low != 1 || result.Rejected != 1 || result.Persisted != 0 {
		t.Fatalf("unexpected inference summary: %+v", result)
	}
}

func TestRunInference_NoUnknownMessages(t *testing.T) {
	cfg := engineTestConfig(t)
	st := engineTestStore(t, cfg)
	createEngineMailbox(t, st, "user@example.com")
	engineSeedMessage(t, st, models.MessageMeta{
		ProviderID: "m1",
		MailboxID:  "user@example.com",
		Provider:   "gmail",
		FromEmail:  "groupupdates@facebookmail.com",
		Domain:     "facebookmail.com",
		ReceivedAt: time.Now().UTC(),
	})

	result, err := RunInference(context.Background(), cfg, "user@example.com", inferenceTestProviderCommand(), inferenceTestProviderArgs("persisted"))
	if err != nil {
		t.Fatalf("RunInference: %v", err)
	}
	if result.Submitted != 0 || result.Persisted != 0 {
		t.Fatalf("expected no submitted inference requests, got %+v", result)
	}
}

func TestRunInference_UsesSenderEmailFallbackPattern(t *testing.T) {
	cfg := engineTestConfig(t)
	st := engineTestStore(t, cfg)
	createEngineMailbox(t, st, "user@example.com")
	engineSeedMessage(t, st, models.MessageMeta{
		ProviderID: "m1",
		MailboxID:  "user@example.com",
		Provider:   "gmail",
		FromEmail:  "ai@example.com",
		Subject:    "Needs review",
		ReceivedAt: time.Now().UTC(),
	})
	if err := st.UpsertSenderStat(context.Background(), "user@example.com", "ai@example.com", "", "", 3); err != nil {
		t.Fatalf("UpsertSenderStat: %v", err)
	}

	result, err := RunInference(context.Background(), cfg, "user@example.com", inferenceTestProviderCommand(), inferenceTestProviderArgs("sender-fallback"))
	if err != nil {
		t.Fatalf("RunInference: %v", err)
	}
	if result.Persisted != 1 {
		t.Fatalf("expected one persisted suggestion, got %+v", result)
	}

	got, err := st.ListInferenceSuggestions(context.Background(), "user@example.com")
	if err != nil {
		t.Fatalf("ListInferenceSuggestions: %v", err)
	}
	if len(got) != 1 || got[0].PatternType != classification.PatternSenderEmail || got[0].PatternValue != "ai@example.com" {
		t.Fatalf("unexpected sender fallback suggestion: %+v", got)
	}
}

func TestRunInference_RejectsCandidateWithoutPromotablePattern(t *testing.T) {
	cfg := engineTestConfig(t)
	st := engineTestStore(t, cfg)
	createEngineMailbox(t, st, "user@example.com")
	engineSeedMessage(t, st, models.MessageMeta{
		ProviderID: "m1",
		MailboxID:  "user@example.com",
		Provider:   "gmail",
		Subject:    "Needs review",
		ReceivedAt: time.Now().UTC(),
	})

	result, err := RunInference(context.Background(), cfg, "user@example.com", inferenceTestProviderCommand(), inferenceTestProviderArgs("sender-fallback"))
	if err != nil {
		t.Fatalf("RunInference: %v", err)
	}
	if result.Submitted != 1 || result.High != 1 || result.Rejected != 1 || result.Persisted != 0 {
		t.Fatalf("unexpected inference summary: %+v", result)
	}
}

func TestRunInference_EmptyMailbox(t *testing.T) {
	cfg := engineTestConfig(t)
	st := engineTestStore(t, cfg)
	createEngineMailbox(t, st, "user@example.com")

	if _, err := RunInference(context.Background(), cfg, "user@example.com", inferenceTestProviderCommand(), inferenceTestProviderArgs("persisted")); err == nil {
		t.Fatal("expected empty mailbox error")
	}
}

func TestRunInference_MailboxNotFound(t *testing.T) {
	cfg := engineTestConfig(t)
	if _, err := RunInference(context.Background(), cfg, "missing@example.com", inferenceTestProviderCommand(), inferenceTestProviderArgs("persisted")); err == nil {
		t.Fatal("expected mailbox resolution error")
	}
}

func TestRunInference_OpenStorageError(t *testing.T) {
	cfg := config.Default()
	cfg.StoragePath = "/dev/null/inboxatlas.db"

	if _, err := RunInference(context.Background(), cfg, "user@example.com", inferenceTestProviderCommand(), inferenceTestProviderArgs("persisted")); err == nil {
		t.Fatal("expected open storage error")
	}
}

func TestRunInference_ProviderError(t *testing.T) {
	cfg := engineTestConfig(t)
	st := engineTestStore(t, cfg)
	createEngineMailbox(t, st, "user@example.com")
	engineSeedMessage(t, st, models.MessageMeta{
		ProviderID: "m1",
		MailboxID:  "user@example.com",
		Provider:   "gmail",
		FromEmail:  "ai@example.com",
		Domain:     "example.com",
		ReceivedAt: time.Now().UTC(),
	})
	if err := st.UpsertDomainStat(context.Background(), "user@example.com", "example.com", 1); err != nil {
		t.Fatalf("UpsertDomainStat: %v", err)
	}

	if _, err := RunInference(context.Background(), cfg, "user@example.com", inferenceTestProviderCommand(), inferenceTestProviderArgs("stderr-exit")); err == nil {
		t.Fatal("expected provider error")
	}
}

func TestRunInference_PropagatesDeterministicClassificationError(t *testing.T) {
	cfg := engineTestConfig(t)
	st := engineTestStore(t, cfg)
	createEngineMailbox(t, st, "user@example.com")
	engineSeedMessage(t, st, models.MessageMeta{
		MailboxID:  "user@example.com",
		Provider:   "gmail",
		FromEmail:  "ai@example.com",
		Domain:     "example.com",
		ReceivedAt: time.Now().UTC(),
	})

	if _, err := RunInference(context.Background(), cfg, "user@example.com", inferenceTestProviderCommand(), inferenceTestProviderArgs("persisted")); err == nil {
		t.Fatal("expected deterministic classification error")
	}
}

func TestListInferenceSuggestions(t *testing.T) {
	cfg := engineTestConfig(t)
	st := engineTestStore(t, cfg)
	createEngineMailbox(t, st, "user@example.com")
	engineSeedMessage(t, st, models.MessageMeta{
		ProviderID: "m1",
		MailboxID:  "user@example.com",
		Provider:   "gmail",
		FromEmail:  "ai@healthymd.com",
		Domain:     "healthymd.com",
		ReceivedAt: time.Now().UTC(),
	})
	if err := st.SaveInferenceCandidate(context.Background(), storage.InferenceSuggestion{
		MailboxID:      "user@example.com",
		MessageID:      "m1",
		PatternType:    classification.PatternDomain,
		PatternValue:   "healthymd.com",
		Category:       classification.CategoryClient,
		Confidence:     0.81,
		ConfidenceBand: "high",
		ReviewRequired: false,
	}); err != nil {
		t.Fatalf("SaveInferenceCandidate: %v", err)
	}

	result, err := ListInferenceSuggestions(context.Background(), cfg, "user@example.com")
	if err != nil {
		t.Fatalf("ListInferenceSuggestions: %v", err)
	}
	if len(result.Suggestions) != 1 || result.Suggestions[0].PatternValue != "healthymd.com" {
		t.Fatalf("unexpected inference suggestions: %+v", result.Suggestions)
	}
}

func TestListInferenceSuggestions_MailboxNotFound(t *testing.T) {
	cfg := engineTestConfig(t)
	if _, err := ListInferenceSuggestions(context.Background(), cfg, "missing@example.com"); err == nil {
		t.Fatal("expected mailbox resolution error")
	}
}

func TestPromoteClassifySuggestion_PromotesInferenceSuggestion(t *testing.T) {
	cfg := engineTestConfig(t)
	st := engineTestStore(t, cfg)
	createEngineMailbox(t, st, "user@example.com")
	engineSeedMessage(t, st, models.MessageMeta{
		ProviderID: "m1",
		MailboxID:  "user@example.com",
		Provider:   "gmail",
		FromEmail:  "ai@healthymd.com",
		Domain:     "healthymd.com",
		ReceivedAt: time.Now().UTC(),
	})
	if err := st.SaveInferenceCandidate(context.Background(), storage.InferenceSuggestion{
		MailboxID:      "user@example.com",
		MessageID:      "m1",
		PatternType:    classification.PatternDomain,
		PatternValue:   "healthymd.com",
		Category:       classification.CategoryClient,
		Confidence:     0.81,
		ConfidenceBand: "high",
		ReviewRequired: false,
	}); err != nil {
		t.Fatalf("SaveInferenceCandidate: %v", err)
	}

	result, err := PromoteClassifySuggestion(context.Background(), cfg, "user@example.com", PromoteSuggestionRequest{
		PatternType:  classification.PatternDomain,
		PatternValue: "healthymd.com",
		Category:     classification.CategoryClient,
	})
	if err != nil {
		t.Fatalf("PromoteClassifySuggestion: %v", err)
	}
	if !result.Created {
		t.Fatal("expected inference suggestion promotion to create seed")
	}
}

func TestInferencePatternForMessage(t *testing.T) {
	if gotType, gotValue, ok := inferencePatternForMessage(models.MessageMeta{Domain: "Example.COM"}); !ok || gotType != classification.PatternDomain || gotValue != "example.com" {
		t.Fatalf("domain pattern: got type=%q value=%q ok=%v", gotType, gotValue, ok)
	}
	if gotType, gotValue, ok := inferencePatternForMessage(models.MessageMeta{FromEmail: "User@Example.com"}); !ok || gotType != classification.PatternSenderEmail || gotValue != "user@example.com" {
		t.Fatalf("sender pattern: got type=%q value=%q ok=%v", gotType, gotValue, ok)
	}
	if _, _, ok := inferencePatternForMessage(models.MessageMeta{}); ok {
		t.Fatal("expected no pattern for empty message")
	}
}

func TestPromoteClassifySuggestion_SuggestionNotFound(t *testing.T) {
	cfg := engineTestConfig(t)
	st := engineTestStore(t, cfg)
	createEngineMailbox(t, st, "user@example.com")

	_, err := PromoteClassifySuggestion(context.Background(), cfg, "user@example.com", PromoteSuggestionRequest{
		PatternType:  classification.PatternDomain,
		PatternValue: "healthymd.com",
		Category:     classification.CategoryVendor,
	})
	if err == nil {
		t.Fatal("expected suggestion-not-found error")
	}
}

func TestPromoteClassifySuggestion_Idempotent(t *testing.T) {
	cfg := engineTestConfig(t)
	st := engineTestStore(t, cfg)
	createEngineMailbox(t, st, "user@example.com")
	if err := st.UpsertDomainStat(context.Background(), "user@example.com", "healthymd.com", 6); err != nil {
		t.Fatalf("UpsertDomainStat: %v", err)
	}

	req := PromoteSuggestionRequest{
		PatternType:  classification.PatternDomain,
		PatternValue: "healthymd.com",
		Category:     classification.CategoryClient,
	}

	first, err := PromoteClassifySuggestion(context.Background(), cfg, "user@example.com", req)
	if err != nil {
		t.Fatalf("first PromoteClassifySuggestion: %v", err)
	}
	if !first.Created {
		t.Fatal("expected first promotion to create seed")
	}

	second, err := PromoteClassifySuggestion(context.Background(), cfg, "user@example.com", req)
	if err != nil {
		t.Fatalf("second PromoteClassifySuggestion: %v", err)
	}
	if second.Created {
		t.Fatal("expected second promotion to be idempotent")
	}

	seeds, err := st.ListSeeds(context.Background(), "user@example.com")
	if err != nil {
		t.Fatalf("ListSeeds: %v", err)
	}
	count := 0
	for _, seed := range seeds {
		if seed.MailboxID == "user@example.com" && seed.PatternType == req.PatternType && seed.PatternValue == req.PatternValue {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected 1 promoted seed, got %d", count)
	}
}

func TestPromoteClassifySuggestion_IgnoresGlobalAndUnrelatedMailboxSeeds(t *testing.T) {
	cfg := engineTestConfig(t)
	st := engineTestStore(t, cfg)
	createEngineMailbox(t, st, "user@example.com")
	createEngineMailbox(t, st, "other@example.com")
	if err := st.UpsertDomainStat(context.Background(), "user@example.com", "healthymd.com", 6); err != nil {
		t.Fatalf("UpsertDomainStat user: %v", err)
	}
	if err := st.UpsertDomainStat(context.Background(), "other@example.com", "other.example.com", 6); err != nil {
		t.Fatalf("UpsertDomainStat other: %v", err)
	}

	if err := st.InsertSeed(context.Background(), storage.ClassificationSeed{
		PatternType:  classification.PatternDomain,
		PatternValue: "healthymd.com",
		Category:     classification.CategoryVendor,
		Source:       classification.SourceSeed,
		Priority:     100,
	}); err != nil {
		t.Fatalf("InsertSeed global: %v", err)
	}
	if err := st.InsertSeed(context.Background(), storage.ClassificationSeed{
		MailboxID:    "user@example.com",
		PatternType:  classification.PatternDomain,
		PatternValue: "other.example.com",
		Category:     classification.CategoryVendor,
		Source:       classification.SourceOperator,
		Priority:     100,
	}); err != nil {
		t.Fatalf("InsertSeed mailbox: %v", err)
	}

	result, err := PromoteClassifySuggestion(context.Background(), cfg, "user@example.com", PromoteSuggestionRequest{
		PatternType:  classification.PatternDomain,
		PatternValue: "healthymd.com",
		Category:     classification.CategoryClient,
	})
	if err != nil {
		t.Fatalf("PromoteClassifySuggestion: %v", err)
	}
	if !result.Created {
		t.Fatal("expected promotion to create a mailbox-scoped seed")
	}
}

func TestPromoteClassifySuggestion_RequiresFields(t *testing.T) {
	cfg := engineTestConfig(t)
	st := engineTestStore(t, cfg)
	createEngineMailbox(t, st, "user@example.com")

	_, err := PromoteClassifySuggestion(context.Background(), cfg, "user@example.com", PromoteSuggestionRequest{})
	if err == nil {
		t.Fatal("expected validation error")
	}
}

func TestPromoteClassifySuggestion_OpenStorageError(t *testing.T) {
	cfg := config.Default()
	cfg.StoragePath = "/dev/null/inboxatlas.db"

	_, err := PromoteClassifySuggestion(context.Background(), cfg, "user@example.com", PromoteSuggestionRequest{
		PatternType:  classification.PatternDomain,
		PatternValue: "healthymd.com",
		Category:     classification.CategoryClient,
	})
	if err == nil {
		t.Fatal("expected open storage error")
	}
}

func TestPromoteClassifySuggestion_CustomPriority(t *testing.T) {
	cfg := engineTestConfig(t)
	st := engineTestStore(t, cfg)
	createEngineMailbox(t, st, "user@example.com")
	if err := st.UpsertDomainStat(context.Background(), "user@example.com", "healthymd.com", 6); err != nil {
		t.Fatalf("UpsertDomainStat: %v", err)
	}

	result, err := PromoteClassifySuggestion(context.Background(), cfg, "user@example.com", PromoteSuggestionRequest{
		PatternType:  classification.PatternDomain,
		PatternValue: "healthymd.com",
		Category:     classification.CategoryClient,
		Priority:     25,
		HasPriority:  true,
	})
	if err != nil {
		t.Fatalf("PromoteClassifySuggestion: %v", err)
	}
	if result.Priority != 25 {
		t.Fatalf("Priority: got %d, want 25", result.Priority)
	}
}

func TestPromoteClassifySuggestion_ConflictingExistingSeed(t *testing.T) {
	cfg := engineTestConfig(t)
	st := engineTestStore(t, cfg)
	createEngineMailbox(t, st, "user@example.com")
	if err := st.UpsertDomainStat(context.Background(), "user@example.com", "healthymd.com", 6); err != nil {
		t.Fatalf("UpsertDomainStat: %v", err)
	}

	if err := st.InsertSeed(context.Background(), storage.ClassificationSeed{
		MailboxID:    "user@example.com",
		PatternType:  classification.PatternDomain,
		PatternValue: "healthymd.com",
		Category:     classification.CategoryVendor,
		Source:       classification.SourceOperator,
		Priority:     50,
	}); err != nil {
		t.Fatalf("InsertSeed: %v", err)
	}

	_, err := PromoteClassifySuggestion(context.Background(), cfg, "user@example.com", PromoteSuggestionRequest{
		PatternType:  classification.PatternDomain,
		PatternValue: "healthymd.com",
		Category:     classification.CategoryClient,
	})
	if err == nil {
		t.Fatal("expected conflicting seed error")
	}
}

func TestToEngineSuggestions_MapsFields(t *testing.T) {
	suggestions := toEngineSuggestions([]classification.ClassificationSeed{{
		MailboxID:    "user@example.com",
		PatternType:  classification.PatternDomain,
		PatternValue: "example.com",
		Category:     classification.CategoryVendor,
		Source:       classification.SourceSeed,
		Priority:     42,
	}})

	if len(suggestions) != 1 {
		t.Fatalf("len(suggestions): got %d, want 1", len(suggestions))
	}
	if suggestions[0].MailboxID != "user@example.com" ||
		suggestions[0].PatternType != classification.PatternDomain ||
		suggestions[0].PatternValue != "example.com" ||
		suggestions[0].Category != classification.CategoryVendor ||
		suggestions[0].Source != classification.SourceSeed ||
		suggestions[0].Priority != 42 {
		t.Fatalf("unexpected mapped suggestion: %+v", suggestions[0])
	}
}

func TestFindSuggestion(t *testing.T) {
	st := engineTestStore(t, engineTestConfig(t))
	createEngineMailbox(t, st, "user@example.com")
	if err := st.UpsertDomainStat(context.Background(), "user@example.com", "healthymd.com", 6); err != nil {
		t.Fatalf("UpsertDomainStat: %v", err)
	}

	got, ok, err := findSuggestion(context.Background(), st, "user@example.com", classification.PatternDomain, "healthymd.com")
	if err != nil {
		t.Fatalf("findSuggestion: %v", err)
	}
	if !ok {
		t.Fatal("expected suggestion to be found")
	}
	if got.MailboxID != "user@example.com" {
		t.Fatalf("MailboxID: got %q, want %q", got.MailboxID, "user@example.com")
	}
	if got.Category != classification.CategoryUnknown {
		t.Fatalf("Category: got %q, want %q", got.Category, classification.CategoryUnknown)
	}

	if _, ok, err := findSuggestion(context.Background(), st, "user@example.com", classification.PatternDomain, "missing.example"); err != nil {
		t.Fatalf("findSuggestion missing: %v", err)
	} else if ok {
		t.Fatal("expected missing suggestion lookup to fail")
	}
}

func TestFindSuggestion_ClosedStore(t *testing.T) {
	cfg := engineTestConfig(t)
	st := engineTestStore(t, cfg)
	createEngineMailbox(t, st, "user@example.com")
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, ok, err := findSuggestion(context.Background(), st, "user@example.com", classification.PatternDomain, "healthymd.com"); err == nil || ok {
		t.Fatalf("expected closed-store error, got ok=%v err=%v", ok, err)
	}
}

func TestClassificationCategories(t *testing.T) {
	got := ClassificationCategories()
	if len(got) == 0 || got[0] != classification.CategoryInternal || got[len(got)-1] != classification.CategoryUnknown {
		t.Fatalf("unexpected categories: %+v", got)
	}
}

func TestClassificationPatternTypes(t *testing.T) {
	got := ClassificationPatternTypes()
	if len(got) == 0 || got[0] != classification.PatternDomain || got[len(got)-1] != classification.PatternSubjectTerm {
		t.Fatalf("unexpected pattern types: %+v", got)
	}
}

func TestOpenResolvedStore_ByAlias(t *testing.T) {
	cfg := engineTestConfig(t)
	st := engineTestStore(t, cfg)
	if err := st.CreateMailbox(context.Background(), models.Mailbox{ID: "user@example.com", Alias: "work", Provider: "gmail"}); err != nil {
		t.Fatalf("CreateMailbox: %v", err)
	}

	opened, mb, err := openResolvedStore(context.Background(), cfg, "work")
	if err != nil {
		t.Fatalf("openResolvedStore: %v", err)
	}
	t.Cleanup(func() { _ = opened.Close() })

	if mb.ID != "user@example.com" {
		t.Fatalf("Mailbox ID: got %q, want %q", mb.ID, "user@example.com")
	}
}

func TestOpenResolvedStore_OpenError(t *testing.T) {
	cfg := config.Default()
	cfg.StoragePath = "/dev/null/inboxatlas.db"

	_, _, err := openResolvedStore(context.Background(), cfg, "user@example.com")
	if err == nil {
		t.Fatal("expected openResolvedStore to fail")
	}
}

func TestOpenResolvedStore_MailboxNotFound(t *testing.T) {
	cfg := engineTestConfig(t)

	_, _, err := openResolvedStore(context.Background(), cfg, "missing@example.com")
	if err == nil {
		t.Fatal("expected mailbox resolution error")
	}
}

func engineTestConfig(t *testing.T) config.Config {
	t.Helper()
	cfg := config.Default()
	cfg.StoragePath = filepath.Join(t.TempDir(), "engine.db")
	return cfg
}

func engineTestStore(t *testing.T, cfg config.Config) *storage.Store {
	t.Helper()
	st, err := storage.Open(cfg.StoragePath)
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func createEngineMailbox(t *testing.T, st *storage.Store, id string) {
	t.Helper()
	if err := st.CreateMailbox(context.Background(), models.Mailbox{ID: id, Provider: "gmail"}); err != nil {
		t.Fatalf("CreateMailbox(%q): %v", id, err)
	}
}

func engineSeedMessage(t *testing.T, st *storage.Store, msg models.MessageMeta) {
	t.Helper()
	if err := st.UpsertMessage(context.Background(), msg); err != nil {
		t.Fatalf("UpsertMessage(%q): %v", msg.ProviderID, err)
	}
}

func inferenceTestProviderCommand() string {
	if _, err := exec.LookPath("sh"); err == nil {
		return "sh"
	}
	return "cmd"
}

func inferenceTestProviderArgs(mode string) []string {
	if _, err := exec.LookPath("sh"); err == nil {
		return []string{"-c", inferenceTestProviderScript(mode)}
	}
	return []string{"/d", "/c", inferenceTestProviderWindows(mode)}
}

func inferenceTestProviderScript(mode string) string {
	switch mode {
	case "persisted":
		return `cat >/dev/null; cat <<'EOF'
[{"message_id":"m2","category":"client","confidence":0.84,"confidence_band":"high","review_required":false,"evidence":{"domain_signal":"known domain"}},{"message_id":"m3","category":"vendor","confidence":0.62,"confidence_band":"medium","review_required":true,"evidence":{"sender_signal":"repeat sender"}}]
EOF`
	case "mixed":
		return `cat >/dev/null; cat <<'EOF'
[{"message_id":"m1","category":"client","confidence":0.20,"confidence_band":"low","review_required":false,"evidence":{}},{"message_id":"missing","category":"vendor","confidence":0.82,"confidence_band":"high","review_required":false,"evidence":{}}]
EOF`
	case "sender-fallback":
		return `cat >/dev/null; cat <<'EOF'
[{"message_id":"m1","category":"client","confidence":0.84,"confidence_band":"high","review_required":false,"evidence":{"sender_signal":"known sender"}}]
EOF`
	case "stderr-exit":
		return `cat >/dev/null; printf 'debug trace\n' >&2; exit 9`
	default:
		panic("unknown mode: " + mode)
	}
}

func inferenceTestProviderWindows(mode string) string {
	switch mode {
	case "persisted":
		return `more >nul & echo [{"message_id":"m2","category":"client","confidence":0.84,"confidence_band":"high","review_required":false,"evidence":{"domain_signal":"known domain"}},{"message_id":"m3","category":"vendor","confidence":0.62,"confidence_band":"medium","review_required":true,"evidence":{"sender_signal":"repeat sender"}}]`
	case "mixed":
		return `more >nul & echo [{"message_id":"m1","category":"client","confidence":0.20,"confidence_band":"low","review_required":false,"evidence":{}},{"message_id":"missing","category":"vendor","confidence":0.82,"confidence_band":"high","review_required":false,"evidence":{}}]`
	case "sender-fallback":
		return `more >nul & echo [{"message_id":"m1","category":"client","confidence":0.84,"confidence_band":"high","review_required":false,"evidence":{"sender_signal":"known sender"}}]`
	case "stderr-exit":
		return `more >nul & >&2 echo debug trace & exit /b 9`
	default:
		panic("unknown mode: " + mode)
	}
}
