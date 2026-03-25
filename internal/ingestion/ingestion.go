// Package ingestion implements the synchronous page-loop pipeline that fetches
// message metadata from a MailProvider, normalizes it, and persists it to storage.
package ingestion

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/UreaLaden/inboxatlas/internal/normalization"
	"github.com/UreaLaden/inboxatlas/internal/storage"
	"github.com/UreaLaden/inboxatlas/pkg/models"
)

// backoffInitialDelay is the starting delay for exponential backoff. It is an
// unexported package-level variable so tests can inject a shorter value without
// changing production behaviour.
var backoffInitialDelay = time.Second

// Options holds all parameters for a sync run.
// RequestDelay 0 defaults to 100ms. MaxRetries 0 defaults to 5.
type Options struct {
	MailboxID    string // canonical lowercased mailbox email
	Provider     string // e.g. "gmail"
	MailProvider models.MailProvider
	Store        *storage.Store
	Stdout       io.Writer     // progress output; must not be nil
	RequestDelay time.Duration // per GetMessageMeta delay floor
	MaxRetries   int           // max retries on retryable errors; 0 → 5
}

// Run executes a full synchronous sync for the mailbox described by opts.
// Progress lines are written to opts.Stdout. On failure a checkpoint with
// status "interrupted" is saved before returning the error.
func Run(ctx context.Context, opts Options) error {
	if opts.MaxRetries == 0 {
		opts.MaxRetries = 5
	}
	if opts.RequestDelay == 0 {
		opts.RequestDelay = 100 * time.Millisecond
	}

	// Authenticate the provider.
	if err := opts.MailProvider.Authenticate(ctx); err != nil {
		return fmt.Errorf("%s: authenticate: %w", opts.Provider, err)
	}

	// Check for an existing checkpoint to determine the start cursor.
	cp, err := opts.Store.GetCheckpoint(ctx, opts.MailboxID, opts.Provider)
	if err != nil {
		return fmt.Errorf("get checkpoint: %w", err)
	}

	var pageCursor string
	var messagesSynced int
	startedAt := time.Now()

	if cp != nil && (cp.Status == "running" || cp.Status == "interrupted") {
		pageCursor = cp.PageCursor
		messagesSynced = cp.MessagesSynced
		startedAt = cp.StartedAt
		_, _ = fmt.Fprintf(opts.Stdout, "Resuming %s sync for %s (%d messages synced so far)...\n",
			opts.Provider, opts.MailboxID, messagesSynced)
	} else {
		_, _ = fmt.Fprintf(opts.Stdout, "Syncing %s for %s...\n", opts.Provider, opts.MailboxID)
	}

	// Save an initial "running" checkpoint.
	if err := opts.Store.SaveCheckpoint(ctx, storage.SyncCheckpoint{
		MailboxID:      opts.MailboxID,
		Provider:       opts.Provider,
		PageCursor:     pageCursor,
		MessagesSynced: messagesSynced,
		Status:         "running",
		StartedAt:      startedAt,
		UpdatedAt:      time.Now(),
	}); err != nil {
		return fmt.Errorf("save initial checkpoint: %w", err)
	}

	pageNum := 0

	for {
		pageNum++

		// Fetch a page of message IDs with backoff on retryable errors.
		ids, nextToken, err := listWithBackoff(ctx, opts.MailProvider, pageCursor, opts.MaxRetries)
		if err != nil {
			saveInterrupted(opts, pageCursor, messagesSynced, startedAt)
			return fmt.Errorf("list messages page %d: %w", pageNum, err)
		}

		// Fetch, normalize, and store each message.
		for _, id := range ids {
			meta, err := getMetaWithBackoff(ctx, opts.MailProvider, id, opts.MaxRetries)
			if err != nil {
				saveInterrupted(opts, pageCursor, messagesSynced, startedAt)
				return fmt.Errorf("get message %s: %w", id, err)
			}

			normalized := normalization.NormalizeMessage(*meta)
			normalized.MailboxID = opts.MailboxID

			if err := opts.Store.UpsertMessage(ctx, normalized); err != nil {
				saveInterrupted(opts, pageCursor, messagesSynced, startedAt)
				return fmt.Errorf("upsert message: %w", err)
			}

			messagesSynced++

			// Per-request delay floor between GetMessageMeta calls.
			if err := sleepCtx(ctx, opts.RequestDelay); err != nil {
				saveInterrupted(opts, pageCursor, messagesSynced, startedAt)
				return err
			}
		}

		// Progress output after each page.
		_, _ = fmt.Fprintf(opts.Stdout, "Page %d: %d messages (total: %d)\n",
			pageNum, len(ids), messagesSynced)

		// Advance the cursor and save checkpoint.
		pageCursor = nextToken
		if err := opts.Store.SaveCheckpoint(ctx, storage.SyncCheckpoint{
			MailboxID:      opts.MailboxID,
			Provider:       opts.Provider,
			PageCursor:     pageCursor,
			MessagesSynced: messagesSynced,
			Status:         "running",
			StartedAt:      startedAt,
			UpdatedAt:      time.Now(),
		}); err != nil {
			return fmt.Errorf("save page checkpoint: %w", err)
		}

		if pageCursor == "" {
			break // all pages consumed
		}

		// Check context between pages.
		if err := ctx.Err(); err != nil {
			saveInterrupted(opts, pageCursor, messagesSynced, startedAt)
			return err
		}
	}

	// Mark sync complete.
	if err := opts.Store.SaveCheckpoint(ctx, storage.SyncCheckpoint{
		MailboxID:      opts.MailboxID,
		Provider:       opts.Provider,
		PageCursor:     "",
		MessagesSynced: messagesSynced,
		Status:         "completed",
		StartedAt:      startedAt,
		UpdatedAt:      time.Now(),
	}); err != nil {
		return fmt.Errorf("save completed checkpoint: %w", err)
	}

	if err := opts.Store.UpdateLastSynced(ctx, opts.MailboxID, time.Now()); err != nil {
		return fmt.Errorf("update last synced: %w", err)
	}

	_, _ = fmt.Fprintf(opts.Stdout, "Sync complete: %d messages synced.\n", messagesSynced)
	return nil
}

// isRetryable reports whether err (or any error in its chain) implements
// models.RetryableError and returns true for IsRetryable.
func isRetryable(err error) bool {
	var re models.RetryableError
	return errors.As(err, &re) && re.IsRetryable()
}

// sleepCtx sleeps for d or until ctx is cancelled.
// Returns ctx.Err() on cancellation.
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// saveInterrupted saves a checkpoint with status "interrupted" using a background
// context so that the save succeeds even when the request context is cancelled.
// Errors are silently dropped — the caller is already returning an error.
func saveInterrupted(opts Options, cursor string, synced int, startedAt time.Time) {
	_ = opts.Store.SaveCheckpoint(context.Background(), storage.SyncCheckpoint{
		MailboxID:      opts.MailboxID,
		Provider:       opts.Provider,
		PageCursor:     cursor,
		MessagesSynced: synced,
		Status:         "interrupted",
		StartedAt:      startedAt,
		UpdatedAt:      time.Now(),
	})
}

// listWithBackoff calls ListMessages with exponential backoff on retryable errors.
func listWithBackoff(ctx context.Context, p models.MailProvider, cursor string, maxRetries int) ([]string, string, error) {
	delay := backoffInitialDelay
	for attempt := 0; ; attempt++ {
		ids, next, err := p.ListMessages(ctx, cursor)
		if err == nil {
			return ids, next, nil
		}
		if attempt >= maxRetries || !isRetryable(err) {
			return nil, "", err
		}
		slog.Warn("retryable list error, backing off",
			"attempt", attempt+1, "max", maxRetries, "delay", delay, "err", err)
		if err := sleepCtx(ctx, delay); err != nil {
			return nil, "", err
		}
		delay *= 2
	}
}

// getMetaWithBackoff calls GetMessageMeta with exponential backoff on retryable errors.
func getMetaWithBackoff(ctx context.Context, p models.MailProvider, id string, maxRetries int) (*models.MessageMeta, error) {
	delay := backoffInitialDelay
	for attempt := 0; ; attempt++ {
		meta, err := p.GetMessageMeta(ctx, id)
		if err == nil {
			return meta, nil
		}
		if attempt >= maxRetries || !isRetryable(err) {
			return nil, err
		}
		slog.Warn("retryable get-meta error, backing off",
			"attempt", attempt+1, "max", maxRetries, "delay", delay, "err", err)
		if err := sleepCtx(ctx, delay); err != nil {
			return nil, err
		}
		delay *= 2
	}
}
