// Package models defines shared data types used across InboxAtlas packages.
package models

import (
	"context"
	"time"
)

// Mailbox represents a registered email account in the InboxAtlas registry.
// ID is the canonical (lowercased) email address and serves as the primary key.
// Alias is an optional user-assigned shorthand (e.g. "work", "personal").
type Mailbox struct {
	ID           string
	Alias        string
	Provider     string
	CreatedAt    time.Time
	LastSyncedAt *time.Time
}

// MessageMeta holds normalized metadata for a single email message.
// Mailbox is a human-readable alias or label; MailboxID is the canonical
// lowercased email address used as the foreign key to the mailboxes table.
// ProviderID is the provider's native message identifier (e.g. Gmail message ID).
// Labels preserves provider label IDs verbatim; canonical mapping is deferred to Epic 6.
type MessageMeta struct {
	ID         string
	Provider   string
	Mailbox    string // alias or display label (for human output)
	MailboxID  string // canonical lowercased email address (FK to mailboxes.id)
	ThreadID   string
	FromEmail  string
	FromName   string
	Domain     string
	Subject    string
	Snippet    string
	ReceivedAt time.Time
	Labels     []string
	ProviderID string // provider's native message ID (e.g. Gmail message ID)
}

// MailProvider is the abstraction all mailbox integrations must satisfy.
// Internal logic must depend on this interface, not on provider-specific types.
type MailProvider interface {
	Authenticate(ctx context.Context) error
	ListMessages(ctx context.Context, pageToken string) (ids []string, nextToken string, err error)
	GetMessageMeta(ctx context.Context, id string) (*MessageMeta, error)
}

// RetryableError is implemented by errors from MailProvider methods that the
// ingestion pipeline may safely retry with exponential backoff.
// Providers wrap HTTP 429 (quota exceeded) and 503 (service unavailable) errors
// in a type implementing this interface; the ingestion layer checks it via errors.As.
type RetryableError interface {
	error
	IsRetryable() bool
}
