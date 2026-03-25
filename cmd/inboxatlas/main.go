// Command inboxatlas is the main entrypoint for the InboxAtlas CLI.
package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/oauth2"

	"github.com/UreaLaden/inboxatlas/internal/auth"
	"github.com/UreaLaden/inboxatlas/internal/config"
	"github.com/UreaLaden/inboxatlas/internal/ingestion"
	gmailprovider "github.com/UreaLaden/inboxatlas/internal/providers/gmail"
	"github.com/UreaLaden/inboxatlas/internal/storage"
	"github.com/UreaLaden/inboxatlas/internal/version"
	"github.com/UreaLaden/inboxatlas/pkg/models"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: failed to load config: %v\n", err)
		os.Exit(1)
	}

	if err := config.EnsureDirs(); err != nil {
		fmt.Fprintf(os.Stderr, "error: failed to create config directories: %v\n", err)
		os.Exit(1)
	}

	initLogger(cfg.LogLevel)

	root := buildRoot(cfg)

	// Support "ia" as a shorthand alias for the root command.
	// When invoked as "ia", update the root Use field so help output is correct.
	if filepath.Base(os.Args[0]) == "ia" || filepath.Base(os.Args[0]) == "ia.exe" {
		root.Use = "ia"
	}

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

// initLogger initialises the default slog logger at the given level, writing
// to stderr per §12.4 CLI error conventions.
func initLogger(level string) {
	var l slog.Level
	switch level {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: l})))
}

// buildRoot constructs and returns the Cobra root command with all subcommands
// registered.
func buildRoot(cfg config.Config) *cobra.Command {
	root := &cobra.Command{
		Use:     "inboxatlas",
		Short:   "InboxAtlas — inbox discovery and classification platform",
		Long:    "InboxAtlas analyzes large mailboxes, extracts operational signal from noisy communication,\nand establishes a foundation for future routing and workflow automation.",
		Version: version.Version,
	}

	root.AddCommand(buildVersionCmd())
	root.AddCommand(buildConfigCmd(cfg))
	root.AddCommand(buildMailboxCmd(cfg))
	root.AddCommand(buildAuthCmd(cfg))
	root.AddCommand(buildSyncCmd(cfg))

	return root
}

// buildVersionCmd returns the "version" subcommand.
func buildVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the InboxAtlas version",
		Run: func(cmd *cobra.Command, _ []string) {
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), version.Version)
		},
	}
}

// buildConfigCmd returns the "config" subcommand group.
func buildConfigCmd(cfg config.Config) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Manage InboxAtlas configuration",
	}
	cmd.AddCommand(buildConfigShowCmd(cfg))
	return cmd
}

// buildConfigShowCmd returns the "config show" subcommand which prints the
// active resolved configuration to stdout.
func buildConfigShowCmd(cfg config.Config) *cobra.Command {
	return &cobra.Command{
		Use:   "show",
		Short: "Print the active resolved configuration",
		Run: func(cmd *cobra.Command, _ []string) {
			w := cmd.OutOrStdout()
			_, _ = fmt.Fprintf(w, "storage_path:      %s\n", cfg.StoragePath)
			_, _ = fmt.Fprintf(w, "log_level:         %s\n", cfg.LogLevel)
			_, _ = fmt.Fprintf(w, "token_dir:         %s\n", cfg.TokenDir)
			_, _ = fmt.Fprintf(w, "default_provider:  %s\n", cfg.DefaultProvider)
			_, _ = fmt.Fprintf(w, "credentials_path:  %s\n", cfg.CredentialsPath)
			_, _ = fmt.Fprintf(w, "token_storage:     %s\n", cfg.TokenStorage)
			_, _ = fmt.Fprintf(w, "sync_delay_ms:     %d\n", cfg.SyncDelayMS)
		},
	}
}

// buildMailboxCmd returns the "mailbox" subcommand group.
func buildMailboxCmd(cfg config.Config) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "mailbox",
		Short: "Manage registered mailboxes",
	}
	cmd.AddCommand(buildMailboxListCmd(cfg))
	cmd.AddCommand(buildMailboxRemoveCmd(cfg))
	return cmd
}

// buildMailboxListCmd returns the "mailbox list" subcommand.
func buildMailboxListCmd(cfg config.Config) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List all registered mailboxes",
		RunE: func(cmd *cobra.Command, _ []string) error {
			st, err := storage.Open(cfg.StoragePath)
			if err != nil {
				return fmt.Errorf("open storage: %w", err)
			}
			defer func() { _ = st.Close() }()
			return runMailboxList(cmd.OutOrStdout(), st)
		},
	}
}

// buildMailboxRemoveCmd returns the "mailbox remove" subcommand.
func buildMailboxRemoveCmd(cfg config.Config) *cobra.Command {
	var account string
	var force bool

	cmd := &cobra.Command{
		Use:   "remove",
		Short: "Remove a registered mailbox",
		RunE: func(cmd *cobra.Command, _ []string) error {
			st, err := storage.Open(cfg.StoragePath)
			if err != nil {
				return fmt.Errorf("open storage: %w", err)
			}
			defer func() { _ = st.Close() }()
			return runMailboxRemove(cmd.OutOrStdout(), os.Stdin, st, account, force)
		},
	}

	cmd.Flags().StringVar(&account, "account", "", "mailbox email or alias to remove")
	cmd.Flags().BoolVar(&force, "force", false, "skip confirmation prompt")
	_ = cmd.MarkFlagRequired("account")

	return cmd
}

// runMailboxList writes all registered mailboxes to w in a human-readable
// table. It is separated from the Cobra handler for testability.
func runMailboxList(w io.Writer, st *storage.Store) error {
	mailboxes, err := st.ListMailboxes(context.Background())
	if err != nil {
		return err
	}

	if len(mailboxes) == 0 {
		_, _ = fmt.Fprintln(w, "No mailboxes registered. Use 'inboxatlas auth gmail --account <email>' to add one.")
		return nil
	}

	tw := tabwriter.NewWriter(w, 0, 0, 3, ' ', 0)
	_, _ = fmt.Fprintln(tw, "ALIAS\tEMAIL\tPROVIDER\tLAST SYNCED")
	for _, mb := range mailboxes {
		lastSynced := "never"
		if mb.LastSyncedAt != nil {
			lastSynced = mb.LastSyncedAt.Format("2006-01-02 15:04")
		}
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", mb.Alias, mb.ID, mb.Provider, lastSynced)
	}
	return tw.Flush()
}

// buildAuthCmd returns the "auth" subcommand group.
func buildAuthCmd(cfg config.Config) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "auth",
		Short: "Authenticate with a mail provider",
	}
	cmd.AddCommand(buildAuthGmailCmd(cfg))
	return cmd
}

// buildAuthGmailCmd returns the "auth gmail" subcommand.
func buildAuthGmailCmd(cfg config.Config) *cobra.Command {
	var account string
	var alias string

	cmd := &cobra.Command{
		Use:   "gmail",
		Short: "Authenticate with Gmail using OAuth 2.0",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAuthGmail(cmd.Context(), cmd.OutOrStdout(), cfg, account, alias)
		},
	}
	cmd.Flags().StringVar(&account, "account", "", "Gmail address to authenticate")
	cmd.Flags().StringVar(&alias, "alias", "", "optional alias for this mailbox")
	_ = cmd.MarkFlagRequired("account")
	return cmd
}

// runAuthGmail checks credentials then delegates to runAuthGmailWithFlow using
// auth.RunFlow as the live flow implementation.
func runAuthGmail(ctx context.Context, w io.Writer, cfg config.Config, account, alias string) error {
	if _, err := os.Stat(cfg.CredentialsPath); err != nil {
		return fmt.Errorf("credentials file not found at %s — set credentials_path in config or INBOXATLAS_CREDENTIALS_PATH", cfg.CredentialsPath)
	}
	oauthCfg, err := auth.LoadCredentials(cfg.CredentialsPath)
	if err != nil {
		return err
	}
	return runAuthGmailWithFlow(ctx, w, cfg, account, alias, oauthCfg, auth.RunFlow)
}

// runAuthGmailWithFlow runs the OAuth flow using flow, saves the token, and
// registers the mailbox. It is separated from runAuthGmail for testability.
func runAuthGmailWithFlow(ctx context.Context, w io.Writer, cfg config.Config, account, alias string, oauthCfg *oauth2.Config, flow func(context.Context, *oauth2.Config, io.Writer) (*oauth2.Token, error)) error {
	canonicalAccount := strings.ToLower(account)

	token, err := flow(ctx, oauthCfg, w)
	if err != nil {
		return err
	}

	ts := auth.NewTokenStorage(&cfg)
	if err := ts.Save("gmail", canonicalAccount, token); err != nil {
		return err
	}

	st, err := storage.Open(cfg.StoragePath)
	if err != nil {
		return fmt.Errorf("open storage: %w", err)
	}
	defer func() { _ = st.Close() }()

	mb := models.Mailbox{ID: canonicalAccount, Alias: alias, Provider: "gmail"}
	if err := st.CreateMailbox(ctx, mb); err != nil {
		// If already registered, treat as no-op.
		existing, getErr := st.GetMailbox(ctx, canonicalAccount)
		if getErr != nil || existing == nil {
			return fmt.Errorf("register mailbox: %w", err)
		}
	}

	_, _ = fmt.Fprintf(w, "Authenticated successfully. Mailbox %s registered.\n", canonicalAccount)
	return nil
}

// buildSyncCmd returns the "sync" subcommand group.
func buildSyncCmd(cfg config.Config) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sync",
		Short: "Sync messages from a mail provider",
	}
	cmd.AddCommand(buildSyncGmailCmd(cfg))
	cmd.AddCommand(buildSyncStatusCmd(cfg))
	return cmd
}

// buildSyncGmailCmd returns the "sync gmail" subcommand.
func buildSyncGmailCmd(cfg config.Config) *cobra.Command {
	var account string
	cmd := &cobra.Command{
		Use:   "gmail",
		Short: "Sync messages from Gmail",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runSyncGmail(cmd.Context(), cmd.OutOrStdout(), cfg, account)
		},
	}
	cmd.Flags().StringVar(&account, "account", "", "mailbox email or alias to sync")
	_ = cmd.MarkFlagRequired("account")
	return cmd
}

// buildSyncStatusCmd returns the "sync status" subcommand.
func buildSyncStatusCmd(cfg config.Config) *cobra.Command {
	var account string
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show sync status for a mailbox",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runSyncStatus(cmd.OutOrStdout(), cfg, account)
		},
	}
	cmd.Flags().StringVar(&account, "account", "", "mailbox email or alias")
	_ = cmd.MarkFlagRequired("account")
	return cmd
}

// runSyncGmail resolves the mailbox, loads credentials, builds a Gmail provider,
// and runs a full ingestion sync. It is separated from the Cobra handler for
// testability.
func runSyncGmail(ctx context.Context, w io.Writer, cfg config.Config, account string) error {
	st, err := storage.Open(cfg.StoragePath)
	if err != nil {
		return fmt.Errorf("open storage: %w", err)
	}
	defer func() { _ = st.Close() }()

	mb, err := storage.ResolveMailbox(ctx, st, account)
	if err != nil {
		return err
	}

	if _, err := os.Stat(cfg.CredentialsPath); err != nil {
		return fmt.Errorf("credentials file not found at %s — set credentials_path in config or INBOXATLAS_CREDENTIALS_PATH", cfg.CredentialsPath)
	}
	oauthCfg, err := auth.LoadCredentials(cfg.CredentialsPath)
	if err != nil {
		return err
	}

	ts := auth.NewTokenStorage(&cfg)
	provider := gmailprovider.New(oauthCfg, ts, mb.ID)

	return ingestion.Run(ctx, ingestion.Options{
		MailboxID:    mb.ID,
		Provider:     "gmail",
		MailProvider: provider,
		Store:        st,
		Stdout:       w,
		RequestDelay: time.Duration(cfg.SyncDelayMS) * time.Millisecond,
		MaxRetries:   5,
	})
}

// runSyncStatus prints the current sync checkpoint for a mailbox to w. It is
// separated from the Cobra handler for testability.
func runSyncStatus(w io.Writer, cfg config.Config, account string) error {
	st, err := storage.Open(cfg.StoragePath)
	if err != nil {
		return fmt.Errorf("open storage: %w", err)
	}
	defer func() { _ = st.Close() }()

	ctx := context.Background()
	mb, err := storage.ResolveMailbox(ctx, st, account)
	if err != nil {
		return err
	}

	cp, err := st.GetCheckpoint(ctx, mb.ID, "gmail")
	if err != nil {
		return fmt.Errorf("get sync status: %w", err)
	}
	if cp == nil {
		_, _ = fmt.Fprintf(w, "No sync checkpoint found for %s (gmail).\n", mb.ID)
		return nil
	}

	_, _ = fmt.Fprintf(w, "Mailbox:         %s\n", mb.ID)
	_, _ = fmt.Fprintf(w, "Provider:        %s\n", cp.Provider)
	_, _ = fmt.Fprintf(w, "Status:          %s\n", cp.Status)
	_, _ = fmt.Fprintf(w, "Messages synced: %d\n", cp.MessagesSynced)
	_, _ = fmt.Fprintf(w, "Started:         %s\n", cp.StartedAt.Format("2006-01-02 15:04:05"))
	_, _ = fmt.Fprintf(w, "Last updated:    %s\n", cp.UpdatedAt.Format("2006-01-02 15:04:05"))
	return nil
}

// runMailboxRemove resolves account, optionally prompts for confirmation (reading
// from r), and deletes the mailbox. It is separated from the Cobra handler for
// testability.
func runMailboxRemove(w io.Writer, r io.Reader, st *storage.Store, account string, force bool) error {
	ctx := context.Background()

	mb, err := storage.ResolveMailbox(ctx, st, account)
	if err != nil {
		return err
	}

	if !force {
		_, _ = fmt.Fprintf(w, "Remove mailbox '%s'? [y/N] ", mb.ID)
		sc := bufio.NewScanner(r)
		sc.Scan()
		if strings.ToLower(strings.TrimSpace(sc.Text())) != "y" {
			_, _ = fmt.Fprintln(w, "Cancelled.")
			return nil
		}
	}

	if err := st.DeleteMailbox(ctx, mb.ID); err != nil {
		return err
	}
	_, _ = fmt.Fprintln(w, "Mailbox removed.")
	return nil
}
