package mock_test

import (
	"context"
	"errors"
	"testing"

	"github.com/UreaLaden/inboxatlas/internal/providers/mock"
	"github.com/UreaLaden/inboxatlas/pkg/models"
)

func makeMessagesWithIDs(ids ...string) []*models.MessageMeta {
	msgs := make([]*models.MessageMeta, len(ids))
	for i, id := range ids {
		msgs[i] = &models.MessageMeta{ProviderID: id}
	}
	return msgs
}

// --- Authenticate ---

func TestMockProvider_Authenticate_NoError(t *testing.T) {
	p := &mock.Provider{}
	if err := p.Authenticate(context.Background()); err != nil {
		t.Errorf("Authenticate: unexpected error: %v", err)
	}
}

func TestMockProvider_Authenticate_InjectedError(t *testing.T) {
	wantErr := errors.New("auth failed")
	p := &mock.Provider{AuthErr: wantErr}
	if err := p.Authenticate(context.Background()); !errors.Is(err, wantErr) {
		t.Errorf("Authenticate: got %v, want %v", err, wantErr)
	}
}

// --- ListMessages pagination ---

func TestMockProvider_ListMessages_FirstPage(t *testing.T) {
	p := &mock.Provider{
		Messages: makeMessagesWithIDs("a", "b", "c", "d", "e"),
		PageSize: 3,
	}
	ids, next, err := p.ListMessages(context.Background(), "")
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}
	if len(ids) != 3 {
		t.Errorf("page size: got %d, want 3", len(ids))
	}
	if ids[0] != "a" || ids[2] != "c" {
		t.Errorf("ids: got %v, want [a b c]", ids)
	}
	if next != "3" {
		t.Errorf("nextToken: got %q, want %q", next, "3")
	}
}

func TestMockProvider_ListMessages_SecondPage(t *testing.T) {
	p := &mock.Provider{
		Messages: makeMessagesWithIDs("a", "b", "c", "d", "e"),
		PageSize: 3,
	}
	ids, next, err := p.ListMessages(context.Background(), "3")
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}
	if len(ids) != 2 {
		t.Errorf("page size: got %d, want 2", len(ids))
	}
	if ids[0] != "d" || ids[1] != "e" {
		t.Errorf("ids: got %v, want [d e]", ids)
	}
	if next != "" {
		t.Errorf("nextToken: got %q, want empty (last page)", next)
	}
}

func TestMockProvider_ListMessages_EmptyMessages(t *testing.T) {
	p := &mock.Provider{}
	ids, next, err := p.ListMessages(context.Background(), "")
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}
	if len(ids) != 0 {
		t.Errorf("expected empty ids, got %v", ids)
	}
	if next != "" {
		t.Errorf("expected empty nextToken, got %q", next)
	}
}

func TestMockProvider_ListMessages_DefaultPageSize(t *testing.T) {
	msgs := make([]*models.MessageMeta, 15)
	for i := range msgs {
		msgs[i] = &models.MessageMeta{ProviderID: string(rune('a' + i))}
	}
	p := &mock.Provider{Messages: msgs} // PageSize == 0 → defaults to 10
	ids, next, err := p.ListMessages(context.Background(), "")
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}
	if len(ids) != 10 {
		t.Errorf("default page size: got %d, want 10", len(ids))
	}
	if next != "10" {
		t.Errorf("nextToken: got %q, want %q", next, "10")
	}
}

func TestMockProvider_ListMessages_InjectedError(t *testing.T) {
	wantErr := errors.New("list failed")
	p := &mock.Provider{ListErr: wantErr}
	_, _, err := p.ListMessages(context.Background(), "")
	if !errors.Is(err, wantErr) {
		t.Errorf("ListMessages: got %v, want %v", err, wantErr)
	}
}

func TestMockProvider_ListMessages_NegativePageToken(t *testing.T) {
	p := &mock.Provider{Messages: makeMessagesWithIDs("a")}
	_, _, err := p.ListMessages(context.Background(), "-1")
	if err == nil {
		t.Fatal("expected error for negative page token")
	}
}

func TestMockProvider_ListMessages_OutOfRangeOffset(t *testing.T) {
	p := &mock.Provider{Messages: makeMessagesWithIDs("a", "b"), PageSize: 2}
	ids, next, err := p.ListMessages(context.Background(), "9")
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}
	if len(ids) != 0 {
		t.Errorf("expected empty ids, got %v", ids)
	}
	if next != "" {
		t.Errorf("nextToken: got %q, want empty", next)
	}
}

func TestMockProvider_ListMessages_NonPositivePageSizeDefaults(t *testing.T) {
	p := &mock.Provider{
		Messages: makeMessagesWithIDs("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"),
		PageSize: -5,
	}
	ids, next, err := p.ListMessages(context.Background(), "")
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}
	if len(ids) != 10 {
		t.Errorf("default page size: got %d, want 10", len(ids))
	}
	if next != "10" {
		t.Errorf("nextToken: got %q, want %q", next, "10")
	}
}

func TestMockProvider_ListMessages_SkipsNilEntries(t *testing.T) {
	p := &mock.Provider{
		Messages: []*models.MessageMeta{
			{ProviderID: "a"},
			nil,
			{ProviderID: "c"},
		},
		PageSize: 10,
	}
	ids, next, err := p.ListMessages(context.Background(), "")
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}
	if len(ids) != 2 || ids[0] != "a" || ids[1] != "c" {
		t.Errorf("ids = %v, want [a c]", ids)
	}
	if next != "" {
		t.Errorf("nextToken: got %q, want empty", next)
	}
}

// --- GetMessageMeta ---

func TestMockProvider_GetMessageMeta_Found(t *testing.T) {
	want := &models.MessageMeta{ProviderID: "x1", Subject: "Hello"}
	p := &mock.Provider{Messages: []*models.MessageMeta{want}}
	got, err := p.GetMessageMeta(context.Background(), "x1")
	if err != nil {
		t.Fatalf("GetMessageMeta: %v", err)
	}
	if got.ProviderID != "x1" {
		t.Errorf("ProviderID = %q, want %q", got.ProviderID, "x1")
	}
	if got.Subject != "Hello" {
		t.Errorf("Subject = %q, want %q", got.Subject, "Hello")
	}
}

func TestMockProvider_GetMessageMeta_NotFound(t *testing.T) {
	p := &mock.Provider{}
	_, err := p.GetMessageMeta(context.Background(), "missing")
	if err == nil {
		t.Error("expected error for missing message")
	}
}

func TestMockProvider_GetMessageMeta_ReturnsCopy(t *testing.T) {
	original := &models.MessageMeta{ProviderID: "c1", Subject: "Original"}
	p := &mock.Provider{Messages: []*models.MessageMeta{original}}
	got, _ := p.GetMessageMeta(context.Background(), "c1")
	got.Subject = "Modified"
	if original.Subject != "Original" {
		t.Error("GetMessageMeta must return a copy, not the original pointer")
	}
}

func TestMockProvider_GetMessageMeta_InjectedError(t *testing.T) {
	wantErr := errors.New("get failed")
	p := &mock.Provider{
		Messages: makeMessagesWithIDs("x"),
		GetErr:   wantErr,
	}
	_, err := p.GetMessageMeta(context.Background(), "x")
	if !errors.Is(err, wantErr) {
		t.Errorf("GetMessageMeta: got %v, want %v", err, wantErr)
	}
}

func TestMockProvider_GetMessageMeta_SkipsNilEntries(t *testing.T) {
	p := &mock.Provider{
		Messages: []*models.MessageMeta{
			nil,
			{ProviderID: "x1", Subject: "Hello"},
		},
	}
	got, err := p.GetMessageMeta(context.Background(), "x1")
	if err != nil {
		t.Fatalf("GetMessageMeta: %v", err)
	}
	if got.ProviderID != "x1" {
		t.Errorf("ProviderID = %q, want %q", got.ProviderID, "x1")
	}
}
