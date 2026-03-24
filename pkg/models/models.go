// Package models defines shared data types used across InboxAtlas packages.
package models

import "time"

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
