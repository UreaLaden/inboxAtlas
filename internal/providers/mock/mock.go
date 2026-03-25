// Package mock provides a controllable in-memory MailProvider for use in tests.
// It is a maintained test-support package (not a _test.go file) and may be
// imported by any test package in the repository.
package mock

import (
	"context"
	"fmt"
	"strconv"

	"github.com/UreaLaden/inboxatlas/pkg/models"
)

// Provider is a controllable in-memory MailProvider for use in tests.
// Populate Messages before calling ListMessages or GetMessageMeta.
// Set AuthErr, ListErr, or GetErr to inject errors into the corresponding methods.
type Provider struct {
	Messages []*models.MessageMeta // messages the provider "knows about"
	PageSize int                   // 0 defaults to 10
	AuthErr  error
	ListErr  error
	GetErr   error
}

// Authenticate returns AuthErr if set, nil otherwise.
func (m *Provider) Authenticate(_ context.Context) error {
	return m.AuthErr
}

// ListMessages returns a page of message IDs. pageToken is an integer offset
// encoded as a string; pass empty string for the first page. Returns the next
// offset token, or empty string when all messages have been returned.
func (m *Provider) ListMessages(_ context.Context, pageToken string) ([]string, string, error) {
	if m.ListErr != nil {
		return nil, "", m.ListErr
	}

	offset := 0
	if pageToken != "" {
		var err error
		offset, err = strconv.Atoi(pageToken)
		if err != nil {
			return nil, "", fmt.Errorf("mock: invalid page token %q: %w", pageToken, err)
		}
		if offset < 0 {
			return nil, "", fmt.Errorf("mock: invalid negative page token %q", pageToken)
		}
	}

	size := m.PageSize
	if size <= 0 {
		size = 10
	}

	total := len(m.Messages)
	if offset >= total {
		return []string{}, "", nil
	}
	end := min(offset+size, total)

	slice := m.Messages[offset:end]
	ids := make([]string, 0, len(slice))
	for _, msg := range slice {
		if msg == nil {
			continue
		}
		ids = append(ids, msg.ProviderID)
	}

	var nextToken string
	if end < total {
		nextToken = strconv.Itoa(end)
	}

	return ids, nextToken, nil
}

// GetMessageMeta returns the MessageMeta for the given provider ID.
// Returns GetErr if set. Returns an error if no message with that ProviderID exists.
func (m *Provider) GetMessageMeta(_ context.Context, id string) (*models.MessageMeta, error) {
	if m.GetErr != nil {
		return nil, m.GetErr
	}
	for _, msg := range m.Messages {
		if msg == nil {
			continue
		}
		if msg.ProviderID == id {
			cp := *msg
			return &cp, nil
		}
	}
	return nil, fmt.Errorf("mock: message %q not found", id)
}
