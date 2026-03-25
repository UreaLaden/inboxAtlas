// Package gmail implements the MailProvider interface for Gmail via the Gmail API.
package gmail

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/mail"
	"strings"
	"time"

	"golang.org/x/oauth2"
	gmailapi "google.golang.org/api/gmail/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"

	"github.com/UreaLaden/inboxatlas/internal/auth"
	"github.com/UreaLaden/inboxatlas/internal/normalization"
	"github.com/UreaLaden/inboxatlas/pkg/models"
)

// retryableError marks a provider error as eligible for backoff retry.
type retryableError struct{ err error }

func (e *retryableError) Error() string     { return e.err.Error() }
func (e *retryableError) IsRetryable() bool { return true }
func (e *retryableError) Unwrap() error     { return e.err }

// wrapIfRetryable returns a *retryableError if err is a *googleapi.Error with
// code 429 or 503. All other errors are returned unchanged.
func wrapIfRetryable(err error) error {
	if err == nil {
		return nil
	}
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) && (apiErr.Code == 429 || apiErr.Code == 503) {
		return &retryableError{err: err}
	}
	return err
}

// Provider implements models.MailProvider for Gmail using the Gmail REST API.
// Call New to construct a Provider; call Authenticate before any other method.
type Provider struct {
	cfg   *oauth2.Config
	ts    auth.TokenStorage // OS-native or file-based token storage
	email string
	svc   *gmailapi.Service // nil until Authenticate is called
}

// New constructs a Gmail Provider for the given account. ts supplies the stored
// OAuth token; cfg is the OAuth2 client configuration.
func New(cfg *oauth2.Config, ts auth.TokenStorage, email string) *Provider {
	return &Provider{cfg: cfg, ts: ts, email: email}
}

// Authenticate loads the stored OAuth token for the account and establishes a
// Gmail API service. It must be called before ListMessages or GetMessageMeta.
// If no token exists, it returns an error directing the user to authenticate first.
func (p *Provider) Authenticate(ctx context.Context) error {
	token, err := p.ts.Load("gmail", p.email)
	if err != nil {
		return fmt.Errorf("gmail: no stored token for %s — run 'inboxatlas auth gmail --account %s' first: %w", p.email, p.email, err)
	}
	src := p.cfg.TokenSource(ctx, token)
	svc, err := gmailapi.NewService(ctx, option.WithTokenSource(src))
	if err != nil {
		return fmt.Errorf("gmail: authenticate: %w", err)
	}
	p.svc = svc
	return nil
}

// ListMessages returns a page of message IDs for the account. pageToken is the
// cursor returned by the previous call; pass empty string for the first page.
// Returns the next page token (empty when exhausted) alongside the ID slice.
func (p *Provider) ListMessages(ctx context.Context, pageToken string) ([]string, string, error) {
	if p.svc == nil {
		return nil, "", fmt.Errorf("gmail: not authenticated")
	}
	req := p.svc.Users.Messages.List(p.email)
	if pageToken != "" {
		req = req.PageToken(pageToken)
	}
	resp, err := req.Context(ctx).Do()
	if err != nil {
		return nil, "", fmt.Errorf("gmail: list messages: %w", wrapIfRetryable(err))
	}
	ids := make([]string, 0, len(resp.Messages))
	for _, m := range resp.Messages {
		ids = append(ids, m.Id)
	}
	return ids, resp.NextPageToken, nil
}

// GetMessageMeta fetches metadata for a single message by ID. The returned
// MessageMeta has all header fields populated; the Mailbox (alias) field is
// left empty — callers must fill it from the mailbox registry if needed.
func (p *Provider) GetMessageMeta(ctx context.Context, id string) (*models.MessageMeta, error) {
	if p.svc == nil {
		return nil, fmt.Errorf("gmail: not authenticated")
	}
	msg, err := p.svc.Users.Messages.Get(p.email, id).Format("metadata").Context(ctx).Do()
	if err != nil {
		return nil, fmt.Errorf("gmail: get message %s: %w", id, wrapIfRetryable(err))
	}
	return parseMessageMeta(p.email, msg), nil
}

// parseMessageMeta converts a *gmailapi.Message into a *models.MessageMeta.
// It handles nil Payload and nil Payload.Headers gracefully, returning a
// partially populated struct rather than panicking.
func parseMessageMeta(accountEmail string, msg *gmailapi.Message) *models.MessageMeta {
	meta := &models.MessageMeta{
		ID:         msg.Id,
		ProviderID: msg.Id,
		MailboxID:  strings.ToLower(accountEmail),
		ThreadID:   msg.ThreadId,
		Snippet:    msg.Snippet,
		Labels:     msg.LabelIds,
		Provider:   "gmail",
	}

	if msg.InternalDate > 0 {
		meta.ReceivedAt = time.UnixMilli(msg.InternalDate)
	}

	if msg.Payload == nil || msg.Payload.Headers == nil {
		return meta
	}

	// Build a case-insensitive header lookup map.
	headers := make(map[string]string, len(msg.Payload.Headers))
	for _, h := range msg.Payload.Headers {
		headers[strings.ToLower(h.Name)] = h.Value
	}

	fromHeader := headers["from"]
	meta.FromName, meta.FromEmail = normalization.ParseFrom(fromHeader)
	meta.Domain = normalization.ExtractDomain(meta.FromEmail)
	meta.Subject = headers["subject"]

	if dateStr := headers["date"]; dateStr != "" {
		if t, err := mail.ParseDate(dateStr); err == nil {
			meta.ReceivedAt = t
		} else {
			slog.Warn("gmail: failed to parse Date header; using InternalDate fallback",
				"date", dateStr, "err", err)
		}
	}

	return meta
}
