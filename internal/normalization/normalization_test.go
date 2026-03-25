package normalization

import (
	"testing"
	"time"

	"github.com/UreaLaden/inboxatlas/pkg/models"
)

// --- ParseFrom ---

func TestParseFrom(t *testing.T) {
	cases := []struct {
		input    string
		wantName string
		wantAddr string
	}{
		{"John Doe <john@example.com>", "John Doe", "john@example.com"},
		{"john@example.com", "", "john@example.com"},
		{" Alice <alice@corp.io> ", "Alice", "alice@corp.io"},
		{"", "", ""},
		{"notanemail", "", "notanemail"},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			name, addr := ParseFrom(tc.input)
			if name != tc.wantName {
				t.Errorf("ParseFrom(%q) name = %q, want %q", tc.input, name, tc.wantName)
			}
			if addr != tc.wantAddr {
				t.Errorf("ParseFrom(%q) addr = %q, want %q", tc.input, addr, tc.wantAddr)
			}
		})
	}
}

// --- ExtractDomain ---

func TestExtractDomain(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"john@example.com", "example.com"},
		{"alice@CORP.IO", "corp.io"},
		{"notanemail", ""},
		{"", ""},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			got := ExtractDomain(tc.input)
			if got != tc.want {
				t.Errorf("ExtractDomain(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

// --- NormalizeMessage ---

func TestNormalizeMessage(t *testing.T) {
	cases := []struct {
		name  string
		input models.MessageMeta
		check func(t *testing.T, got models.MessageMeta)
	}{
		{
			name:  "uppercase domain in From",
			input: models.MessageMeta{FromEmail: "alice@CORP.IO"},
			check: func(t *testing.T, got models.MessageMeta) {
				if got.Domain != "corp.io" {
					t.Errorf("Domain = %q, want %q", got.Domain, "corp.io")
				}
			},
		},
		{
			name:  "whitespace in Subject",
			input: models.MessageMeta{Subject: "  Hello  "},
			check: func(t *testing.T, got models.MessageMeta) {
				if got.Subject != "Hello" {
					t.Errorf("Subject = %q, want %q", got.Subject, "Hello")
				}
			},
		},
		{
			name:  "whitespace in Snippet",
			input: models.MessageMeta{Snippet: " snippet "},
			check: func(t *testing.T, got models.MessageMeta) {
				if got.Snippet != "snippet" {
					t.Errorf("Snippet = %q, want %q", got.Snippet, "snippet")
				}
			},
		},
		{
			name:  "display name and email in FromEmail",
			input: models.MessageMeta{FromEmail: "Bob <bob@x.io>"},
			check: func(t *testing.T, got models.MessageMeta) {
				if got.FromName != "Bob" {
					t.Errorf("FromName = %q, want %q", got.FromName, "Bob")
				}
				if got.FromEmail != "bob@x.io" {
					t.Errorf("FromEmail = %q, want %q", got.FromEmail, "bob@x.io")
				}
			},
		},
		{
			name:  "empty From",
			input: models.MessageMeta{FromEmail: ""},
			check: func(t *testing.T, got models.MessageMeta) {
				if got.FromEmail != "" {
					t.Errorf("FromEmail = %q, want empty", got.FromEmail)
				}
				if got.Domain != "" {
					t.Errorf("Domain = %q, want empty", got.Domain)
				}
			},
		},
		{
			name:  "labels preserved verbatim",
			input: models.MessageMeta{Labels: []string{"INBOX", "SENT"}},
			check: func(t *testing.T, got models.MessageMeta) {
				if len(got.Labels) != 2 || got.Labels[0] != "INBOX" || got.Labels[1] != "SENT" {
					t.Errorf("Labels = %v, want [INBOX SENT]", got.Labels)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := NormalizeMessage(tc.input)
			tc.check(t, got)
		})
	}
}

func TestNormalizeMessage_DoesNotMutateInput(t *testing.T) {
	original := models.MessageMeta{
		Subject:   "  untrimmed  ",
		FromEmail: "alice@CORP.IO",
	}
	_ = NormalizeMessage(original)
	if original.Subject != "  untrimmed  " {
		t.Error("NormalizeMessage must not mutate the input")
	}
}

func TestNormalizeMessage_ReceivedAtPreserved(t *testing.T) {
	ts := time.Date(2024, 1, 15, 9, 0, 0, 0, time.UTC)
	msg := models.MessageMeta{ReceivedAt: ts}
	got := NormalizeMessage(msg)
	if !got.ReceivedAt.Equal(ts) {
		t.Errorf("ReceivedAt changed: got %v, want %v", got.ReceivedAt, ts)
	}
}
