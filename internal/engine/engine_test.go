package engine

import (
	"context"
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

	got, err := st.GetClassification(context.Background(), "m1", "user@example.com")
	if err != nil {
		t.Fatalf("GetClassification: %v", err)
	}
	if got == nil || got.Category != classification.CategorySocial {
		t.Fatalf("expected social classification, got %+v", got)
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

func TestListClassifySuggestions(t *testing.T) {
	cfg := engineTestConfig(t)
	st := engineTestStore(t, cfg)
	createEngineMailbox(t, st, "user@example.com")

	result, err := ListClassifySuggestions(context.Background(), cfg, "user@example.com")
	if err != nil {
		t.Fatalf("ListClassifySuggestions: %v", err)
	}
	if result.MailboxID != "user@example.com" {
		t.Fatalf("MailboxID: got %q", result.MailboxID)
	}
	if len(result.Suggestions) == 0 {
		t.Fatal("expected suggestions")
	}
}

func TestListClassifySuggestions_MailboxNotFound(t *testing.T) {
	cfg := engineTestConfig(t)

	_, err := ListClassifySuggestions(context.Background(), cfg, "missing@example.com")
	if err == nil {
		t.Fatal("expected mailbox resolution error")
	}
}

func TestPromoteClassifySuggestion_Idempotent(t *testing.T) {
	cfg := engineTestConfig(t)
	st := engineTestStore(t, cfg)
	createEngineMailbox(t, st, "user@example.com")

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

func TestPromoteClassifySuggestion_RequiresFields(t *testing.T) {
	cfg := engineTestConfig(t)
	st := engineTestStore(t, cfg)
	createEngineMailbox(t, st, "user@example.com")

	_, err := PromoteClassifySuggestion(context.Background(), cfg, "user@example.com", PromoteSuggestionRequest{})
	if err == nil {
		t.Fatal("expected validation error")
	}
}

func TestPromoteClassifySuggestion_CustomPriority(t *testing.T) {
	cfg := engineTestConfig(t)
	st := engineTestStore(t, cfg)
	createEngineMailbox(t, st, "user@example.com")

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
