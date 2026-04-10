// Command inboxatlas is the main entrypoint for the InboxAtlas CLI.
package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/oauth2"

	"github.com/UreaLaden/inboxatlas/internal/analysis"
	"github.com/UreaLaden/inboxatlas/internal/auth"
	"github.com/UreaLaden/inboxatlas/internal/config"
	"github.com/UreaLaden/inboxatlas/internal/engine"
	exportpkg "github.com/UreaLaden/inboxatlas/internal/export"
	"github.com/UreaLaden/inboxatlas/internal/ingestion"
	gmailprovider "github.com/UreaLaden/inboxatlas/internal/providers/gmail"
	"github.com/UreaLaden/inboxatlas/internal/storage"
	"github.com/UreaLaden/inboxatlas/internal/version"
	"github.com/UreaLaden/inboxatlas/pkg/models"
)

var validateGmailDelegation = auth.ValidateGmailDelegation
var resolveGmailTokenSource = auth.ResolveGmailTokenSource

type gmailSyncProvider interface {
	models.MailProvider
	ListLabels(context.Context) ([]gmailprovider.LabelMeta, error)
}

var newGmailProvider = func(email string, tokenSourceFactory func(context.Context) (oauth2.TokenSource, error)) gmailSyncProvider {
	return gmailprovider.New(email, tokenSourceFactory)
}
var runIngestion = ingestion.Run
var runClassify = engine.RunClassify
var runInference = engine.RunInference
var reportExportPDFRenderer exportpkg.PDFRenderer
var readSummaryPromptFile = os.ReadFile
var newSummaryProvider = func(command string, args []string) exportpkg.SummaryProvider {
	return exportpkg.CommandSummaryProvider{Command: command, Args: args}
}

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
	root.AddCommand(buildClassifyCmd(cfg))
	root.AddCommand(buildReportCmd(cfg))

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
	cmd.AddCommand(buildAuthStatusCmd(cfg))
	return cmd
}

// buildAuthStatusCmd returns the "auth status" subcommand.
func buildAuthStatusCmd(cfg config.Config) *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show authentication state for all registered mailboxes",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAuthStatus(cmd.OutOrStdout(), cfg)
		},
	}
}

// authState describes the resolved authentication state for a single mailbox.
type authState struct {
	email    string
	provider string
	mode     string
	status   string
}

// runAuthStatus enumerates all registered mailboxes and prints a table of auth
// state for each. It is separated from the Cobra handler for testability.
func runAuthStatus(w io.Writer, cfg config.Config) error {
	st, err := storage.Open(cfg.StoragePath)
	if err != nil {
		return fmt.Errorf("open storage: %w", err)
	}
	defer func() { _ = st.Close() }()

	mailboxes, err := st.ListMailboxes(context.Background())
	if err != nil {
		return err
	}

	if len(mailboxes) == 0 {
		_, _ = fmt.Fprintln(w, "No mailboxes registered. Use 'inboxatlas auth gmail --account <email>' to add one.")
		return nil
	}

	states := make([]authState, 0, len(mailboxes))
	for _, mb := range mailboxes {
		states = append(states, resolveAuthState(cfg, mb.ID, mb.Provider))
	}

	tw := tabwriter.NewWriter(w, 0, 0, 3, ' ', 0)
	_, _ = fmt.Fprintln(tw, "MAILBOX\tPROVIDER\tAUTH MODE\tSTATUS")
	for _, s := range states {
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", s.email, s.provider, s.mode, s.status)
	}
	return tw.Flush()
}

// resolveAuthState determines the auth mode and status for a single mailbox
// without making any live API calls.
func resolveAuthState(cfg config.Config, email, provider string) authState {
	state := authState{email: email, provider: provider, mode: "unknown", status: "unknown"}

	if provider != "gmail" {
		state.status = "unsupported provider"
		return state
	}

	// Try service-account credentials first. If they load, this is a delegated mailbox.
	if _, err := auth.LoadServiceAccountJWTConfig(cfg.CredentialsPath); err == nil {
		state.mode = "delegated"
		state.status = "authenticated (delegated)"
		return state
	}

	// Try installed-app (OAuth) credentials next.
	if _, err := auth.LoadCredentials(cfg.CredentialsPath); err == nil {
		state.mode = "oauth"
		ts := auth.NewTokenStorage(&cfg)
		if _, err := ts.Load("gmail", email); err != nil {
			state.status = "not authenticated"
		} else {
			state.status = "authenticated"
		}
		return state
	}

	// Neither credential type loaded — file is missing or invalid.
	if _, err := os.Stat(cfg.CredentialsPath); os.IsNotExist(err) {
		state.mode = "—"
		state.status = "no credentials file"
	} else {
		state.mode = "—"
		state.status = "invalid credentials file"
	}
	return state
}

// buildAuthGmailCmd returns the "auth gmail" subcommand.
func buildAuthGmailCmd(cfg config.Config) *cobra.Command {
	var account string
	var alias string
	var delegated bool

	cmd := &cobra.Command{
		Use:   "gmail",
		Short: "Authenticate with Gmail using OAuth 2.0",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if delegated {
				return runAuthGmailDelegated(cmd.Context(), cmd.OutOrStdout(), cfg, account, alias)
			}
			return runAuthGmail(cmd.Context(), cmd.OutOrStdout(), cfg, account, alias)
		},
	}
	cmd.Flags().StringVar(&account, "account", "", "Gmail address to authenticate")
	cmd.Flags().StringVar(&alias, "alias", "", "optional alias for this mailbox")
	cmd.Flags().BoolVar(&delegated, "delegated", false, "validate domain-wide delegation for this mailbox")
	_ = cmd.MarkFlagRequired("account")
	return cmd
}

// runAuthGmail checks credentials then delegates to runAuthGmailWithFlow using
// auth.RunFlow as the live flow implementation.
func runAuthGmail(ctx context.Context, w io.Writer, cfg config.Config, account, alias string) error {
	if _, err := os.Stat(cfg.CredentialsPath); err != nil {
		return fmt.Errorf("credentials file not found at %s — set credentials_path in config or INBOXATLAS_CREDENTIALS_PATH", cfg.CredentialsPath)
	}
	oauthCfg, err := auth.LoadInstalledAppCredentials(cfg.CredentialsPath)
	if err != nil {
		return err
	}
	return runAuthGmailWithFlow(ctx, w, cfg, account, alias, oauthCfg, auth.RunFlow)
}

func runAuthGmailDelegated(ctx context.Context, w io.Writer, cfg config.Config, account, alias string) error {
	if _, err := os.Stat(cfg.CredentialsPath); err != nil {
		return fmt.Errorf("credentials file not found at %s — set credentials_path in config or INBOXATLAS_CREDENTIALS_PATH", cfg.CredentialsPath)
	}
	canonicalAccount := strings.ToLower(account)
	if err := validateGmailDelegation(ctx, cfg.CredentialsPath, canonicalAccount); err != nil {
		return err
	}
	if err := registerMailbox(ctx, cfg, canonicalAccount, alias); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(w, "Delegation validated successfully. Mailbox %s registered.\n", canonicalAccount)
	return nil
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

	if err := registerMailbox(ctx, cfg, canonicalAccount, alias); err != nil {
		return err
	}

	_, _ = fmt.Fprintf(w, "Authenticated successfully. Mailbox %s registered.\n", canonicalAccount)
	return nil
}

func registerMailbox(ctx context.Context, cfg config.Config, canonicalAccount, alias string) error {
	st, err := storage.Open(cfg.StoragePath)
	if err != nil {
		return fmt.Errorf("open storage: %w", err)
	}
	defer func() { _ = st.Close() }()

	mb := models.Mailbox{ID: canonicalAccount, Alias: alias, Provider: "gmail"}
	if err := st.CreateMailbox(ctx, mb); err != nil {
		existing, getErr := st.GetMailbox(ctx, canonicalAccount)
		if getErr != nil || existing == nil {
			return fmt.Errorf("register mailbox: %w", err)
		}
	}
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
	var limit int
	cmd := &cobra.Command{
		Use:   "gmail",
		Short: "Sync messages from Gmail",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runSyncGmail(cmd.Context(), cmd.OutOrStdout(), cfg, account, limit)
		},
	}
	cmd.Flags().StringVar(&account, "account", "", "mailbox email or alias to sync")
	cmd.Flags().IntVar(&limit, "limit", 0, "stop after syncing this many messages and complete (0 = unlimited)")
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

// runSyncGmail resolves the mailbox, resolves the auth mode, builds a Gmail provider,
// and runs a full ingestion sync. It is separated from the Cobra handler for
// testability.
func runSyncGmail(ctx context.Context, w io.Writer, cfg config.Config, account string, limit int) error {
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
	tokenSourceFactory, err := resolveGmailTokenSource(&cfg, mb.ID)
	if err != nil {
		return err
	}

	provider := newGmailProvider(mb.ID, tokenSourceFactory)

	if err := runIngestion(ctx, ingestion.Options{
		MailboxID:    mb.ID,
		Provider:     "gmail",
		MailProvider: provider,
		Store:        st,
		Stdout:       w,
		RequestDelay: time.Duration(cfg.SyncDelayMS) * time.Millisecond,
		MaxRetries:   5,
		MessageLimit: limit,
	}); err != nil {
		return err
	}

	labels, err := provider.ListLabels(ctx)
	if err != nil {
		slog.WarnContext(ctx, "label catalog sync failed; label names may be stale", "err", err)
		return nil
	}

	entries := make([]storage.LabelCatalogEntry, 0, len(labels))
	for _, label := range labels {
		entries = append(entries, storage.LabelCatalogEntry{
			LabelID:     label.ID,
			DisplayName: label.DisplayName,
			LabelType:   label.Type,
		})
	}
	if err := st.UpsertLabelCatalog(ctx, mb.ID, entries); err != nil {
		slog.WarnContext(ctx, "label catalog persist failed", "err", err)
	}
	return nil
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
		_, _ = fmt.Fprintf(w, "Permanently purge InboxAtlas local data for mailbox '%s'? [y/N] ", mb.ID)
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
	_, _ = fmt.Fprintln(w, "Mailbox removed. InboxAtlas local data purged.")
	return nil
}

// buildClassifyCmd returns the "classify" subcommand group.
func buildClassifyCmd(cfg config.Config) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "classify",
		Short: "Run and manage mailbox classification workflows",
	}
	cmd.AddCommand(buildClassifyRunCmd(cfg))
	cmd.AddCommand(buildClassifyMessagesCmd(cfg))
	cmd.AddCommand(buildClassifyLabelAnalysisCmd(cfg))
	cmd.AddCommand(buildClassifyResultsCmd(cfg))
	cmd.AddCommand(buildClassifySuggestionsCmd(cfg))
	cmd.AddCommand(buildClassifyInferCmd(cfg))
	cmd.AddCommand(buildClassifyPromoteCmd(cfg))
	cmd.AddCommand(buildClassifySeedsCmd(cfg))
	cmd.AddCommand(buildClassifyCategoriesCmd())
	cmd.AddCommand(buildClassifyIntentsCmd())
	cmd.AddCommand(buildClassifyPatternTypesCmd())
	return cmd
}

// buildClassifyLabelAnalysisCmd returns the "classify label-analysis"
// subcommand.
func buildClassifyLabelAnalysisCmd(cfg config.Config) *cobra.Command {
	var account string
	var format string
	var minCount int
	var topDomains int

	cmd := &cobra.Command{
		Use:   "label-analysis",
		Short: "Show Gmail label frequency to guide seed authoring",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runClassifyLabelAnalysis(cmd.Context(), cmd.OutOrStdout(), cfg, account, format, minCount, topDomains)
		},
	}
	cmd.Flags().StringVar(&account, "account", "", "mailbox email or alias")
	cmd.Flags().StringVar(&format, "format", "table", "output format: table, json")
	cmd.Flags().IntVar(&minCount, "min-count", 1, "minimum message count to include a label")
	cmd.Flags().IntVar(&topDomains, "top-domains", 0, "top domains to include per label")
	_ = cmd.MarkFlagRequired("account")
	return cmd
}

// buildClassifyInferCmd returns the "classify infer" command, which runs one
// mailbox-scoped inference pass and also hosts inference suggestion listing.
func buildClassifyInferCmd(cfg config.Config) *cobra.Command {
	var account string
	var providerCommand string
	var providerArgs []string

	cmd := &cobra.Command{
		Use:   "infer",
		Short: "Run AI-assisted inference for still-unknown mailbox messages",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runClassifyInfer(cmd.Context(), cmd.OutOrStdout(), cfg, account, providerCommand, providerArgs)
		},
	}
	cmd.Flags().StringVar(&account, "account", "", "mailbox email or alias")
	cmd.Flags().StringVar(&providerCommand, "provider-command", "", "external command that returns structured inference candidates")
	cmd.Flags().StringSliceVar(&providerArgs, "provider-arg", nil, "argument to pass to the inference provider command; repeatable")
	_ = cmd.MarkFlagRequired("account")
	cmd.AddCommand(buildClassifyInferSuggestionsCmd(cfg))
	return cmd
}

// buildClassifyInferSuggestionsCmd returns the "classify infer suggestions"
// subcommand.
func buildClassifyInferSuggestionsCmd(cfg config.Config) *cobra.Command {
	var account string
	var format string
	cmd := &cobra.Command{
		Use:   "suggestions",
		Short: "List persisted AI inference suggestions for operator review",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runClassifyInferSuggestions(cmd.Context(), cmd.OutOrStdout(), cfg, account, format)
		},
	}
	cmd.Flags().StringVar(&account, "account", "", "mailbox email or alias")
	cmd.Flags().StringVar(&format, "format", "table", "output format: table, json")
	_ = cmd.MarkFlagRequired("account")
	return cmd
}

// buildClassifyRunCmd returns the "classify run" subcommand.
func buildClassifyRunCmd(cfg config.Config) *cobra.Command {
	var account string
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run mailbox-scoped classification for one mailbox",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runClassifyRun(cmd.Context(), cmd.OutOrStdout(), cfg, account)
		},
	}
	cmd.Flags().StringVar(&account, "account", "", "mailbox email or alias")
	_ = cmd.MarkFlagRequired("account")
	return cmd
}

// buildClassifyMessagesCmd returns the "classify messages" subcommand.
func buildClassifyMessagesCmd(cfg config.Config) *cobra.Command {
	var account string
	var category string
	var intent string
	var since string
	var format string
	var limit int

	cmd := &cobra.Command{
		Use:   "messages",
		Short: "List classified messages with optional category and intent filters",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runClassifyMessages(cmd.Context(), cmd.OutOrStdout(), cfg, account, category, intent, since, format, limit)
		},
	}
	cmd.Flags().StringVar(&account, "account", "", "mailbox email or alias")
	cmd.Flags().StringVar(&category, "category", "", "filter by relationship category (optional)")
	cmd.Flags().StringVar(&intent, "intent", "", "filter by intent (optional)")
	cmd.Flags().StringVar(&since, "since", "", "only include messages on or after this RFC3339 timestamp (e.g. 2026-04-01T00:00:00Z)")
	cmd.Flags().StringVar(&format, "format", "table", "output format: table, csv, json")
	cmd.Flags().IntVar(&limit, "limit", 100, "maximum rows to return (0 = no limit)")
	_ = cmd.MarkFlagRequired("account")
	return cmd
}

// buildClassifySuggestionsCmd returns the "classify suggestions" subcommand.
func buildClassifySuggestionsCmd(cfg config.Config) *cobra.Command {
	var account string
	var format string
	cmd := &cobra.Command{
		Use:   "suggestions",
		Short: "Show mailbox bootstrap classification suggestions",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runClassifySuggestions(cmd.Context(), cmd.OutOrStdout(), cfg, account, format)
		},
	}
	cmd.Flags().StringVar(&account, "account", "", "mailbox email or alias")
	cmd.Flags().StringVar(&format, "format", "table", "output format: table, json")
	_ = cmd.MarkFlagRequired("account")
	return cmd
}

// buildClassifyResultsCmd returns the "classify results" subcommand.
func buildClassifyResultsCmd(cfg config.Config) *cobra.Command {
	var account string
	var format string
	cmd := &cobra.Command{
		Use:   "results",
		Short: "Review mailbox classification results",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runClassifyResults(cmd.Context(), cmd.OutOrStdout(), cfg, account, format)
		},
	}
	cmd.Flags().StringVar(&account, "account", "", "mailbox email or alias")
	cmd.Flags().StringVar(&format, "format", "table", "output format: table, json")
	_ = cmd.MarkFlagRequired("account")
	return cmd
}

// buildClassifyPromoteCmd returns the "classify promote" subcommand.
func buildClassifyPromoteCmd(cfg config.Config) *cobra.Command {
	var account string
	var patternType string
	var patternValue string
	var category string
	var priority int

	cmd := &cobra.Command{
		Use:   "promote",
		Short: "Promote a mailbox bootstrap suggestion into active mailbox-scoped seeds",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runClassifyPromote(cmd.Context(), cmd.OutOrStdout(), cfg, account, engine.PromoteSuggestionRequest{
				PatternType:  patternType,
				PatternValue: patternValue,
				Category:     category,
				Priority:     priority,
				HasPriority:  cmd.Flags().Changed("priority"),
			})
		},
	}
	cmd.Flags().StringVar(&account, "account", "", "mailbox email or alias")
	cmd.Flags().StringVar(&patternType, "pattern-type", "", "suggestion pattern type")
	cmd.Flags().StringVar(&patternValue, "pattern-value", "", "suggestion pattern value")
	cmd.Flags().StringVar(&category, "category", "", "suggestion category")
	cmd.Flags().IntVar(&priority, "priority", 0, "override promoted seed priority")
	_ = cmd.MarkFlagRequired("account")
	_ = cmd.MarkFlagRequired("pattern-type")
	_ = cmd.MarkFlagRequired("pattern-value")
	_ = cmd.MarkFlagRequired("category")
	return cmd
}

// buildClassifySeedsCmd returns the "classify seeds" parent command.
func buildClassifySeedsCmd(cfg config.Config) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "seeds",
		Short: "List and delete active mailbox-scoped seeds",
	}
	cmd.AddCommand(buildClassifySeedsListCmd(cfg))
	cmd.AddCommand(buildClassifySeedsDeleteCmd(cfg))
	return cmd
}

// buildClassifySeedsListCmd returns the "classify seeds list" subcommand.
func buildClassifySeedsListCmd(cfg config.Config) *cobra.Command {
	var account string
	var format string
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List active mailbox-scoped seeds",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runClassifySeedsList(cmd.Context(), cmd.OutOrStdout(), cfg, account, format)
		},
	}
	cmd.Flags().StringVar(&account, "account", "", "mailbox email or alias")
	cmd.Flags().StringVar(&format, "format", "table", "output format: table, json")
	_ = cmd.MarkFlagRequired("account")
	return cmd
}

// buildClassifySeedsDeleteCmd returns the "classify seeds delete" subcommand.
func buildClassifySeedsDeleteCmd(cfg config.Config) *cobra.Command {
	var account string
	var seedID int64
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete one active mailbox-scoped seed",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runClassifySeedsDelete(cmd.Context(), cmd.OutOrStdout(), cfg, account, seedID)
		},
	}
	cmd.Flags().StringVar(&account, "account", "", "mailbox email or alias")
	cmd.Flags().Int64Var(&seedID, "id", 0, "mailbox seed ID")
	_ = cmd.MarkFlagRequired("account")
	_ = cmd.MarkFlagRequired("id")
	return cmd
}

// buildClassifyCategoriesCmd returns the "classify categories" subcommand.
func buildClassifyCategoriesCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "categories",
		Short: "List valid deterministic classification categories",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runClassifyCategories(cmd.OutOrStdout())
		},
	}
}

// buildClassifyPatternTypesCmd returns the "classify pattern-types" subcommand.
func buildClassifyPatternTypesCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "pattern-types",
		Short: "List valid deterministic classification pattern types",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runClassifyPatternTypes(cmd.OutOrStdout())
		},
	}
}

// buildClassifyIntentsCmd returns the "classify intents" subcommand.
func buildClassifyIntentsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "intents",
		Short: "List valid deterministic classification intents",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runClassifyIntents(cmd.OutOrStdout())
		},
	}
}

func validateClassifyFormat(f string) (string, error) {
	switch f {
	case "table", "json":
		return f, nil
	default:
		return "", fmt.Errorf("unknown format %q — valid values: table, json", f)
	}
}

func validateClassifyMessagesFormat(f string) (string, error) {
	switch f {
	case "table", "csv", "json":
		return f, nil
	default:
		return "", fmt.Errorf("unknown format %q — valid values: table, csv, json", f)
	}
}

func gmailLabelName(id string) string {
	switch id {
	case "INBOX":
		return "Inbox"
	case "SENT":
		return "Sent"
	case "TRASH":
		return "Trash"
	case "SPAM":
		return "Spam"
	case "STARRED":
		return "Starred"
	case "YELLOW_STAR":
		return "Yellow star"
	case "BLUE_STAR":
		return "Blue star"
	case "RED_STAR":
		return "Red star"
	case "ORANGE_STAR":
		return "Orange star"
	case "GREEN_STAR":
		return "Green star"
	case "PURPLE_STAR":
		return "Purple star"
	case "IMPORTANT":
		return "Important"
	case "YELLOW_BANG":
		return "Yellow bang"
	case "RED_BANG":
		return "Red bang"
	case "ORANGE_BANG":
		return "Orange bang"
	case "GREEN_BANG":
		return "Green bang"
	case "BLUE_INFO":
		return "Blue info"
	case "PURPLE_QUESTION":
		return "Purple question"
	case "UNREAD":
		return "Unread"
	case "DRAFT":
		return "Drafts"
	case "ALL_MAIL":
		return "All mail"
	case "CHAT":
		return "Chat"
	case "CATEGORY_PROMOTIONS":
		return "Promotions"
	case "CATEGORY_SOCIAL":
		return "Social"
	case "CATEGORY_UPDATES":
		return "Updates"
	case "CATEGORY_FORUMS":
		return "Forums"
	case "CATEGORY_PERSONAL":
		return "Personal"
	default:
		return id
	}
}

// runClassifyRun executes mailbox-scoped classification for one mailbox.
func runClassifyRun(ctx context.Context, w io.Writer, cfg config.Config, account string) error {
	result, err := runClassify(ctx, cfg, account)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(w, "Classified %d messages for %s.\n", result.MessagesProcessed, result.MailboxID)
	if len(result.Breakdown) == 0 {
		return nil
	}

	tw := tabwriter.NewWriter(w, 0, 0, 3, ' ', 0)
	showIntent := false
	for _, row := range result.Breakdown {
		if row.Intent != "" {
			showIntent = true
			break
		}
	}
	if showIntent {
		_, _ = fmt.Fprintln(tw, "CATEGORY\tINTENT\tCOUNT")
	} else {
		_, _ = fmt.Fprintln(tw, "CATEGORY\tCOUNT")
	}
	for _, row := range result.Breakdown {
		if showIntent {
			_, _ = fmt.Fprintf(tw, "%s\t%s\t%d\n", row.Category, row.Intent, row.Count)
			continue
		}
		_, _ = fmt.Fprintf(tw, "%s\t%d\n", row.Category, row.Count)
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(w, "Unknown: %.1f%%\n", result.UnknownPct)
	return nil
}

// runClassifyMessages renders per-message classification rows with optional
// category and intent filters for one mailbox.
func runClassifyMessages(ctx context.Context, w io.Writer, cfg config.Config, account, category, intent, since, format string, limit int) error {
	f, err := validateClassifyMessagesFormat(format)
	if err != nil {
		return err
	}
	if err := validateClassificationCategory(category); err != nil {
		return err
	}
	if err := validateClassificationIntent(intent); err != nil {
		return err
	}
	var sinceTime *time.Time
	if since != "" {
		parsed, err := time.Parse(time.RFC3339, since)
		if err != nil {
			return fmt.Errorf("invalid --since value %q: must be RFC3339 (e.g. 2026-04-01T00:00:00Z)", since)
		}
		sinceTime = &parsed
	}

	result, err := engine.ListClassifiedMessages(ctx, cfg, account, engine.ClassifiedMessagesFilter{
		Category: category,
		Intent:   intent,
		Limit:    limit,
		Since:    sinceTime,
	})
	if err != nil {
		return err
	}

	if f == "json" {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(result.Messages)
	}
	if f == "csv" {
		cw := csv.NewWriter(w)
		if err := cw.Write([]string{"MessageID", "Timestamp", "Sender", "Domain", "Subject", "Intent", "Category", "HasAttachment"}); err != nil {
			return err
		}
		for _, msg := range result.Messages {
			if err := cw.Write([]string{
				msg.MessageID,
				msg.ReceivedAt.Format(time.RFC3339),
				msg.FromEmail,
				msg.Domain,
				msg.Subject,
				msg.Intent,
				msg.Category,
				fmt.Sprintf("%t", msg.HasAttachment),
			}); err != nil {
				return err
			}
		}
		cw.Flush()
		return cw.Error()
	}

	if len(result.Messages) == 0 {
		_, _ = fmt.Fprintf(w, "No classified messages found for %s.\n", result.MailboxID)
		return nil
	}

	tw := tabwriter.NewWriter(w, 0, 0, 3, ' ', 0)
	_, _ = fmt.Fprintln(tw, "MESSAGE ID\tFROM\tDOMAIN\tSUBJECT\tCATEGORY\tINTENT\tHAS ATTACHMENT\tRECEIVED")
	for _, msg := range result.Messages {
		renderedIntent := msg.Intent
		if renderedIntent == "" {
			renderedIntent = "-"
		}
		subject := msg.Subject
		if len(subject) > 40 {
			subject = subject[:37] + "..."
		}
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			msg.MessageID,
			msg.FromEmail,
			msg.Domain,
			subject,
			msg.Category,
			renderedIntent,
			fmt.Sprintf("%t", msg.HasAttachment),
			msg.ReceivedAt.Format("2006-01-02"),
		)
	}
	return tw.Flush()
}

// runClassifySuggestions renders mailbox bootstrap suggestions for one mailbox.
func runClassifySuggestions(ctx context.Context, w io.Writer, cfg config.Config, account, format string) error {
	f, err := validateClassifyFormat(format)
	if err != nil {
		return err
	}

	result, err := engine.ListClassifySuggestions(ctx, cfg, account)
	if err != nil {
		return err
	}

	if f == "json" {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(result.Suggestions)
	}

	if len(result.Suggestions) == 0 {
		_, _ = fmt.Fprintf(w, "No mailbox bootstrap suggestions for %s.\n", result.MailboxID)
		return nil
	}

	tw := tabwriter.NewWriter(w, 0, 0, 3, ' ', 0)
	_, _ = fmt.Fprintln(tw, "PATTERN TYPE\tPATTERN VALUE\tCATEGORY\tSOURCE\tPRIORITY")
	for _, suggestion := range result.Suggestions {
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d\n", suggestion.PatternType, suggestion.PatternValue, suggestion.Category, suggestion.Source, suggestion.Priority)
	}
	return tw.Flush()
}

// runClassifyLabelAnalysis renders mailbox-scoped Gmail label frequency rows
// for manual operator review.
func runClassifyLabelAnalysis(ctx context.Context, w io.Writer, cfg config.Config, account, format string, minCount, topDomains int) error {
	f, err := validateClassifyFormat(format)
	if err != nil {
		return err
	}
	if topDomains > 0 {
		return runClassifyLabelDomainAnalysis(ctx, w, cfg, account, f, minCount, topDomains)
	}

	result, err := engine.ListLabelStats(ctx, cfg, account, minCount)
	if err != nil {
		return err
	}

	if f == "json" {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(result.Labels)
	}

	if len(result.Labels) == 0 {
		_, _ = fmt.Fprintf(w, "No label stats found for %s.\n", result.MailboxID)
		return nil
	}

	tw := tabwriter.NewWriter(w, 0, 0, 3, ' ', 0)
	_, _ = fmt.Fprintln(tw, "LABEL ID\tNAME\tMESSAGES")
	for _, row := range result.Labels {
		name := row.DisplayName
		if name == "" {
			name = gmailLabelName(row.Label)
		}
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%d\n", row.Label, name, row.MessageCount)
	}
	return tw.Flush()
}

// runClassifyLabelDomainAnalysis renders mailbox-scoped Gmail label rows
// expanded into top domains for manual operator review.
func runClassifyLabelDomainAnalysis(ctx context.Context, w io.Writer, cfg config.Config, account, format string, minCount, topDomains int) error {
	result, err := engine.ListLabelDomainStats(ctx, cfg, account, minCount, topDomains)
	if err != nil {
		return err
	}

	for i := range result.Labels {
		if result.Labels[i].Name == "" {
			result.Labels[i].Name = gmailLabelName(result.Labels[i].Label)
		}
	}

	if format == "json" {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(result.Labels)
	}

	if len(result.Labels) == 0 {
		_, _ = fmt.Fprintf(w, "No label stats found for %s.\n", result.MailboxID)
		return nil
	}

	tw := tabwriter.NewWriter(w, 0, 0, 3, ' ', 0)
	_, _ = fmt.Fprintln(tw, "LABEL ID\tNAME\tDOMAIN\tMESSAGES")
	for _, row := range result.Labels {
		for _, domain := range row.Domains {
			_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%d\n", row.Label, row.Name, domain.Domain, domain.MessageCount)
		}
	}
	return tw.Flush()
}

// runClassifyInfer executes one mailbox-scoped AI inference pass.
func runClassifyInfer(ctx context.Context, w io.Writer, cfg config.Config, account, providerCommand string, providerArgs []string) error {
	command := strings.TrimSpace(providerCommand)
	if command == "" {
		command = strings.TrimSpace(os.Getenv("INBOXATLAS_INFERENCE_PROVIDER_CMD"))
	}
	if command == "" {
		return fmt.Errorf("inference provider command is required via --provider-command or INBOXATLAS_INFERENCE_PROVIDER_CMD")
	}

	result, err := runInference(ctx, cfg, account, command, providerArgs)
	if err != nil {
		return err
	}

	_, _ = fmt.Fprintf(w, "Inference submitted %d unknown messages for %s.\n", result.Submitted, result.MailboxID)
	_, _ = fmt.Fprintf(w, "Persisted %d suggestions. High: %d Medium: %d Low: %d Rejected: %d\n", result.Persisted, result.High, result.Medium, result.Low, result.Rejected)
	return nil
}

// runClassifyInferSuggestions renders persisted AI inference suggestions for
// one mailbox.
func runClassifyInferSuggestions(ctx context.Context, w io.Writer, cfg config.Config, account, format string) error {
	f, err := validateClassifyFormat(format)
	if err != nil {
		return err
	}

	result, err := engine.ListInferenceSuggestions(ctx, cfg, account)
	if err != nil {
		return err
	}

	if f == "json" {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(result.Suggestions)
	}

	if len(result.Suggestions) == 0 {
		_, _ = fmt.Fprintf(w, "No AI inference suggestions for %s.\n", result.MailboxID)
		return nil
	}

	tw := tabwriter.NewWriter(w, 0, 0, 3, ' ', 0)
	_, _ = fmt.Fprintln(tw, "MESSAGE ID\tPATTERN TYPE\tPATTERN VALUE\tCATEGORY\tCONFIDENCE\tBAND\tREVIEW REQUIRED")
	for _, suggestion := range result.Suggestions {
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%.2f\t%s\t%t\n", suggestion.MessageID, suggestion.PatternType, suggestion.PatternValue, suggestion.Category, suggestion.Confidence, suggestion.ConfidenceBand, suggestion.ReviewRequired)
	}
	return tw.Flush()
}

// runClassifyResults renders mailbox-scoped classification summary output.
func runClassifyResults(ctx context.Context, w io.Writer, cfg config.Config, account, format string) error {
	f, err := validateClassifyFormat(format)
	if err != nil {
		return err
	}

	result, err := engine.GetClassificationSummary(ctx, cfg, account)
	if err != nil {
		return err
	}

	if f == "json" {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(result)
	}

	if len(result.Breakdown) == 0 {
		_, _ = fmt.Fprintf(w, "No classifications found for %s.\n", result.MailboxID)
		return nil
	}

	tw := tabwriter.NewWriter(w, 0, 0, 3, ' ', 0)
	showIntent := false
	for _, row := range result.Breakdown {
		if row.Intent != "" {
			showIntent = true
			break
		}
	}
	if showIntent {
		_, _ = fmt.Fprintln(tw, "CATEGORY\tINTENT\tCOUNT")
	} else {
		_, _ = fmt.Fprintln(tw, "CATEGORY\tCOUNT")
	}
	for _, row := range result.Breakdown {
		if showIntent {
			_, _ = fmt.Fprintf(tw, "%s\t%s\t%d\n", row.Category, row.Intent, row.Count)
			continue
		}
		_, _ = fmt.Fprintf(tw, "%s\t%d\n", row.Category, row.Count)
	}
	if err := tw.Flush(); err != nil {
		return err
	}

	_, _ = fmt.Fprintf(w, "Total: %d\n", result.Total)
	_, _ = fmt.Fprintf(w, "Unknown: %.1f%%\n", result.UnknownPct)
	return nil
}

// runClassifyPromote promotes a reviewed mailbox bootstrap suggestion into the
// active mailbox-scoped seed set.
func runClassifyPromote(ctx context.Context, w io.Writer, cfg config.Config, account string, req engine.PromoteSuggestionRequest) error {
	result, err := engine.PromoteClassifySuggestion(ctx, cfg, account, req)
	if err != nil {
		return err
	}

	if result.Created {
		_, _ = fmt.Fprintf(w, "Promoted suggestion for %s: %s:%s -> %s (priority %d).\n", result.MailboxID, result.PatternType, result.PatternValue, result.Category, result.Priority)
		return nil
	}

	_, _ = fmt.Fprintf(w, "Suggestion already promoted for %s: %s:%s -> %s (priority %d).\n", result.MailboxID, result.PatternType, result.PatternValue, result.Category, result.Priority)
	return nil
}

// runClassifySeedsList renders active mailbox-scoped seeds for one mailbox.
func runClassifySeedsList(ctx context.Context, w io.Writer, cfg config.Config, account, format string) error {
	f, err := validateClassifyFormat(format)
	if err != nil {
		return err
	}

	seeds, err := engine.ListMailboxSeeds(ctx, cfg, account)
	if err != nil {
		return err
	}

	if f == "json" {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(seeds)
	}

	if len(seeds) == 0 {
		_, _ = fmt.Fprintf(w, "No mailbox-scoped seeds found for %s.\n", account)
		return nil
	}

	tw := tabwriter.NewWriter(w, 0, 0, 3, ' ', 0)
	_, _ = fmt.Fprintln(tw, "ID\tMAILBOX\tPATTERN TYPE\tPATTERN VALUE\tCATEGORY\tSOURCE\tPRIORITY")
	for _, seed := range seeds {
		_, _ = fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\t%s\t%d\n", seed.ID, seed.MailboxID, seed.PatternType, seed.PatternValue, seed.Category, seed.Source, seed.Priority)
	}
	return tw.Flush()
}

// runClassifySeedsDelete deletes one active mailbox-scoped seed by ID.
func runClassifySeedsDelete(ctx context.Context, w io.Writer, cfg config.Config, account string, seedID int64) error {
	if err := engine.DeleteMailboxSeed(ctx, cfg, account, seedID); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(w, "Deleted mailbox seed %d for %s.\n", seedID, account)
	return nil
}

// runClassifyCategories writes the supported deterministic category names.
func runClassifyCategories(w io.Writer) error {
	for _, category := range engine.ClassificationCategories() {
		if _, err := fmt.Fprintln(w, category); err != nil {
			return err
		}
	}
	return nil
}

func validateClassificationCategory(category string) error {
	if category == "" {
		return nil
	}
	for _, known := range engine.ClassificationCategories() {
		if category == known {
			return nil
		}
	}
	return fmt.Errorf("unknown category %q — run 'inboxatlas classify categories' for valid values", category)
}

func validateClassificationIntent(intent string) error {
	if intent == "" {
		return nil
	}
	for _, known := range engine.ClassificationIntents() {
		if intent == known {
			return nil
		}
	}
	return fmt.Errorf("unknown intent %q — run 'inboxatlas classify intents' for valid values", intent)
}

// runClassifyIntents writes the supported deterministic intent names.
func runClassifyIntents(w io.Writer) error {
	for _, intent := range engine.ClassificationIntents() {
		if _, err := fmt.Fprintln(w, intent); err != nil {
			return err
		}
	}
	return nil
}

// runClassifyPatternTypes writes the supported deterministic pattern types.
func runClassifyPatternTypes(w io.Writer) error {
	for _, patternType := range engine.ClassificationPatternTypes() {
		if _, err := fmt.Fprintln(w, patternType); err != nil {
			return err
		}
	}
	return nil
}

// buildReportCmd returns the "report" subcommand group.
func buildReportCmd(cfg config.Config) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "report",
		Short: "Generate discovery reports from synced mailbox data",
	}
	cmd.AddCommand(buildReportSummarizeCmd(cfg))
	cmd.AddCommand(buildReportExportCmd(cfg))
	cmd.AddCommand(buildReportDomainsCmd(cfg))
	cmd.AddCommand(buildReportSendersCmd(cfg))
	cmd.AddCommand(buildReportSubjectsCmd(cfg))
	cmd.AddCommand(buildReportVolumeCmd(cfg))
	return cmd
}

type reportExportOptions struct {
	reportsDir  string
	outputDir   string
	format      string
	ownerEmail  string
	ownerDomain string
	summaryFile string
}

type reportSummarizeOptions struct {
	reportsDir      string
	outputFile      string
	ownerEmail      string
	ownerDomain     string
	providerCommand string
	providerArgs    []string
	promptFile      string
}

// buildReportSummarizeCmd returns the "report summarize" subcommand.
func buildReportSummarizeCmd(cfg config.Config) *cobra.Command {
	var opts reportSummarizeOptions
	_ = cfg

	cmd := &cobra.Command{
		Use:   "summarize",
		Short: "Generate a canonical summary.md artifact from report inputs",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runReportSummarize(cmd.Context(), cmd.OutOrStdout(), opts)
		},
	}
	cmd.Flags().StringVar(&opts.reportsDir, "reports-dir", "", "directory containing report CSV inputs")
	cmd.Flags().StringVar(&opts.outputFile, "output-file", "", "path to write generated summary markdown")
	cmd.Flags().StringVar(&opts.ownerEmail, "owner-email", "", "owner email used for internal filtering and packaging")
	cmd.Flags().StringVar(&opts.ownerDomain, "owner-domain", "", "owner domain used for internal filtering")
	cmd.Flags().StringVar(&opts.providerCommand, "provider-command", "", "external command that returns structured summary JSON")
	cmd.Flags().StringSliceVar(&opts.providerArgs, "provider-arg", nil, "argument to pass to the provider command; repeatable")
	cmd.Flags().StringVar(&opts.promptFile, "prompt-file", ".claude/commands/summarizeReport.md", "prompt contract file supplied to the provider")
	_ = cmd.MarkFlagRequired("reports-dir")
	return cmd
}

// buildReportExportCmd returns the "report export" subcommand.
func buildReportExportCmd(cfg config.Config) *cobra.Command {
	var opts reportExportOptions
	_ = cfg

	cmd := &cobra.Command{
		Use:   "export",
		Short: "Package workbook and snapshot exports from a reports directory",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runReportExport(cmd.Context(), cmd.OutOrStdout(), opts)
		},
	}
	cmd.Flags().StringVar(&opts.reportsDir, "reports-dir", "", "directory containing report CSV inputs")
	cmd.Flags().StringVar(&opts.outputDir, "output-dir", "", "directory to write exported artifacts into")
	cmd.Flags().StringVar(&opts.format, "format", "excel", "export format: excel, html, pdf, all")
	cmd.Flags().StringVar(&opts.ownerEmail, "owner-email", "", "owner email used for internal filtering and packaging")
	cmd.Flags().StringVar(&opts.ownerDomain, "owner-domain", "", "owner domain used for internal filtering")
	cmd.Flags().StringVar(&opts.summaryFile, "summary-file", "", "summary markdown file required for html/pdf exports")
	_ = cmd.MarkFlagRequired("reports-dir")
	_ = cmd.MarkFlagRequired("output-dir")
	return cmd
}

// buildReportDomainsCmd returns the "report domains" subcommand.
func buildReportDomainsCmd(cfg config.Config) *cobra.Command {
	var account string
	var allAccounts bool
	var format string
	var limit int
	var output string

	cmd := &cobra.Command{
		Use:   "domains",
		Short: "Report top sending domains",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runReportCommand(cmd.OutOrStdout(), output, func(w io.Writer) error {
				return runReportDomains(cmd.Context(), w, cfg, account, allAccounts, format, limit)
			})
		},
	}
	cmd.Flags().StringVar(&account, "account", "", "mailbox email or alias")
	cmd.Flags().BoolVar(&allAccounts, "all-accounts", false, "aggregate across all registered mailboxes")
	cmd.Flags().StringVar(&format, "format", "table", "output format: table, csv, json")
	cmd.Flags().IntVar(&limit, "limit", 25, "maximum number of rows to return")
	cmd.Flags().StringVar(&output, "output", "", "write rendered report output to a file")
	return cmd
}

// buildReportSendersCmd returns the "report senders" subcommand.
func buildReportSendersCmd(cfg config.Config) *cobra.Command {
	var account string
	var allAccounts bool
	var format string
	var limit int
	var output string

	cmd := &cobra.Command{
		Use:   "senders",
		Short: "Report top message senders",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runReportCommand(cmd.OutOrStdout(), output, func(w io.Writer) error {
				return runReportSenders(cmd.Context(), w, cfg, account, allAccounts, format, limit)
			})
		},
	}
	cmd.Flags().StringVar(&account, "account", "", "mailbox email or alias")
	cmd.Flags().BoolVar(&allAccounts, "all-accounts", false, "aggregate across all registered mailboxes")
	cmd.Flags().StringVar(&format, "format", "table", "output format: table, csv, json")
	cmd.Flags().IntVar(&limit, "limit", 25, "maximum number of rows to return")
	cmd.Flags().StringVar(&output, "output", "", "write rendered report output to a file")
	return cmd
}

// buildReportSubjectsCmd returns the "report subjects" subcommand.
func buildReportSubjectsCmd(cfg config.Config) *cobra.Command {
	var account string
	var allAccounts bool
	var format string
	var limit int
	var output string

	cmd := &cobra.Command{
		Use:   "subjects",
		Short: "Report top subject line terms",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runReportCommand(cmd.OutOrStdout(), output, func(w io.Writer) error {
				return runReportSubjects(cmd.Context(), w, cfg, account, allAccounts, format, limit)
			})
		},
	}
	cmd.Flags().StringVar(&account, "account", "", "mailbox email or alias")
	cmd.Flags().BoolVar(&allAccounts, "all-accounts", false, "aggregate across all registered mailboxes")
	cmd.Flags().StringVar(&format, "format", "table", "output format: table, csv, json")
	cmd.Flags().IntVar(&limit, "limit", 25, "maximum number of rows to return")
	cmd.Flags().StringVar(&output, "output", "", "write rendered report output to a file")
	return cmd
}

// buildReportVolumeCmd returns the "report volume" subcommand.
func buildReportVolumeCmd(cfg config.Config) *cobra.Command {
	var account string
	var allAccounts bool
	var format string
	var output string

	cmd := &cobra.Command{
		Use:   "volume",
		Short: "Report monthly message volume",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runReportCommand(cmd.OutOrStdout(), output, func(w io.Writer) error {
				return runReportVolume(cmd.Context(), w, cfg, account, allAccounts, format)
			})
		},
	}
	cmd.Flags().StringVar(&account, "account", "", "mailbox email or alias")
	cmd.Flags().BoolVar(&allAccounts, "all-accounts", false, "aggregate across all registered mailboxes")
	cmd.Flags().StringVar(&format, "format", "table", "output format: table, csv, json")
	cmd.Flags().StringVar(&output, "output", "", "write rendered report output to a file")
	return cmd
}

// runReportCommand selects the report output destination and executes run
// against it. When outputPath is empty, defaultWriter is used.
func runReportCommand(defaultWriter io.Writer, outputPath string, run func(io.Writer) error) error {
	writer := defaultWriter
	var file *os.File
	var err error
	if outputPath != "" {
		file, err = os.Create(outputPath)
		if err != nil {
			return fmt.Errorf("open output file: %w", err)
		}
		defer func() { _ = file.Close() }()
		writer = file
	}
	return run(writer)
}

func validateExportFormat(f string) (string, error) {
	switch f {
	case "excel", "html", "pdf", "all":
		return f, nil
	default:
		return "", fmt.Errorf("unknown export format %q — valid values: excel, html, pdf, all", f)
	}
}

func runReportSummarize(ctx context.Context, w io.Writer, opts reportSummarizeOptions) error {
	model, err := exportpkg.ParseReportsDir(exportpkg.Options{
		ReportsDir:  opts.reportsDir,
		OwnerEmail:  opts.ownerEmail,
		OwnerDomain: opts.ownerDomain,
	})
	if err != nil {
		return err
	}

	input, err := exportpkg.BuildSummaryInput(model)
	if err != nil {
		return err
	}

	prompt, err := loadSummaryPrompt(opts.promptFile)
	if err != nil {
		return err
	}

	command := strings.TrimSpace(opts.providerCommand)
	if command == "" {
		command = strings.TrimSpace(os.Getenv("INBOXATLAS_SUMMARY_PROVIDER_CMD"))
	}
	if command == "" {
		return fmt.Errorf("summary provider command is required via --provider-command or INBOXATLAS_SUMMARY_PROVIDER_CMD")
	}

	outputPath := strings.TrimSpace(opts.outputFile)
	if outputPath == "" {
		outputPath = filepath.Join(opts.reportsDir, "summary.md")
	}

	provider := newSummaryProvider(command, opts.providerArgs)
	output, err := provider.GenerateSummary(ctx, prompt, input)
	if err != nil {
		return err
	}

	narrative, err := exportpkg.AdaptSummaryOutput(input, output)
	if err != nil {
		return err
	}

	markdown, err := exportpkg.FormatSnapshotNarrativeMarkdown(narrative)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		return fmt.Errorf("create summary output dir: %w", err)
	}
	if err := os.WriteFile(outputPath, markdown, 0o600); err != nil {
		return fmt.Errorf("write summary file: %w", err)
	}

	_, _ = fmt.Fprintf(w, "Wrote %s\n", outputPath)
	return nil
}

func loadSummaryPrompt(path string) (string, error) {
	body, err := readSummaryPromptFile(path)
	if err != nil {
		return "", fmt.Errorf("read summary prompt: %w", err)
	}
	return string(body), nil
}

func runReportExport(ctx context.Context, w io.Writer, opts reportExportOptions) error {
	_ = ctx

	format, err := validateExportFormat(opts.format)
	if err != nil {
		return err
	}

	model, err := exportpkg.ParseReportsDir(exportpkg.Options{
		ReportsDir:  opts.reportsDir,
		OwnerEmail:  opts.ownerEmail,
		OwnerDomain: opts.ownerDomain,
	})
	if err != nil {
		return err
	}

	var narrative exportpkg.SnapshotNarrative
	if exportNeedsNarrative(format) {
		if strings.TrimSpace(opts.summaryFile) == "" {
			return fmt.Errorf("--summary-file is required for %s export", format)
		}
		narrative, err = exportpkg.LoadSnapshotNarrative(opts.summaryFile)
		if err != nil {
			return err
		}
	}

	files := make(map[string][]byte)
	baseName := exportBaseName(model)

	if format == "excel" || format == "all" {
		workbook, err := exportpkg.BuildWorkbook(model, exportpkg.WorkbookOptions{})
		if err != nil {
			return err
		}
		files[baseName+".xlsx"] = workbook
	}

	if format == "html" || format == "all" {
		html, err := exportpkg.BuildSnapshotHTML(model, narrative, exportpkg.SnapshotOptions{})
		if err != nil {
			return err
		}
		files[baseName+".html"] = html
	}

	if format == "pdf" || format == "all" {
		pdf, err := exportpkg.BuildSnapshotPDF(model, narrative, exportpkg.SnapshotOptions{}, reportExportPDFRenderer)
		if err != nil {
			return err
		}
		files[baseName+".pdf"] = pdf
	}

	if err := os.MkdirAll(opts.outputDir, 0o755); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}

	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		path := filepath.Join(opts.outputDir, name)
		if err := os.WriteFile(path, files[name], 0o600); err != nil {
			return fmt.Errorf("write export file: %w", err)
		}
		_, _ = fmt.Fprintf(w, "Wrote %s\n", path)
	}
	return nil
}

func exportNeedsNarrative(format string) bool {
	return format == "html" || format == "pdf" || format == "all"
}

func exportBaseName(model *exportpkg.Model) string {
	owner := model.Owner.Email
	if owner == "" {
		owner = model.Owner.Domain
	}
	if owner == "" {
		owner = "all-mailboxes"
	}

	start := model.Summary.ReportingPeriodStart
	end := model.Summary.ReportingPeriodEnd
	period := "unknown-period"
	if start != "" && end != "" {
		if start == end {
			period = start
		} else {
			period = start + "-to-" + end
		}
	}

	return "inbox-report-" + sanitizeExportPart(owner) + "-" + sanitizeExportPart(period)
}

func sanitizeExportPart(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return "unknown"
	}

	var out strings.Builder
	lastDash := false
	for _, r := range value {
		isAlphaNum := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if isAlphaNum {
			out.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			out.WriteByte('-')
			lastDash = true
		}
	}

	result := strings.Trim(out.String(), "-")
	if result == "" {
		return "unknown"
	}
	return result
}

// resolveReportMailboxID validates the account/all-accounts flags and returns
// the canonical mailbox ID to pass to query functions. Returns "" when
// all-accounts is set. It is shared by all report run* functions.
func resolveReportMailboxID(ctx context.Context, cfg config.Config, account string, allAccounts bool) (string, *storage.Store, error) {
	if allAccounts && account != "" {
		return "", nil, fmt.Errorf("--account and --all-accounts are mutually exclusive")
	}
	if !allAccounts && account == "" {
		return "", nil, fmt.Errorf("--account is required (or use --all-accounts)")
	}

	st, err := storage.Open(cfg.StoragePath)
	if err != nil {
		return "", nil, fmt.Errorf("open storage: %w", err)
	}

	if allAccounts {
		return "", st, nil
	}

	mb, err := storage.ResolveMailbox(ctx, st, account)
	if err != nil {
		_ = st.Close()
		return "", nil, err
	}
	return mb.ID, st, nil
}

// validateFormat checks that f is a known Format value.
func validateFormat(f string) (analysis.Format, error) {
	switch analysis.Format(f) {
	case analysis.FormatTable, analysis.FormatCSV, analysis.FormatJSON:
		return analysis.Format(f), nil
	default:
		return "", fmt.Errorf("unknown format %q — valid values: table, csv, json", f)
	}
}

// runReportDomains queries and renders the top sending domains for a mailbox.
// It is separated from the Cobra handler for testability.
func runReportDomains(ctx context.Context, w io.Writer, cfg config.Config, account string, allAccounts bool, format string, limit int) error {
	mailboxID, st, err := resolveReportMailboxID(ctx, cfg, account, allAccounts)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	f, err := validateFormat(format)
	if err != nil {
		return err
	}

	rows, err := analysis.QueryDomains(ctx, st, mailboxID, limit)
	if err != nil {
		return err
	}
	return analysis.RenderDomains(w, rows, f)
}

// runReportSenders queries and renders the top message senders for a mailbox.
// It is separated from the Cobra handler for testability.
func runReportSenders(ctx context.Context, w io.Writer, cfg config.Config, account string, allAccounts bool, format string, limit int) error {
	mailboxID, st, err := resolveReportMailboxID(ctx, cfg, account, allAccounts)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	f, err := validateFormat(format)
	if err != nil {
		return err
	}

	rows, err := analysis.QuerySenders(ctx, st, mailboxID, limit)
	if err != nil {
		return err
	}
	return analysis.RenderSenders(w, rows, f)
}

// runReportSubjects queries and renders the top subject line terms for a mailbox.
// It is separated from the Cobra handler for testability.
func runReportSubjects(ctx context.Context, w io.Writer, cfg config.Config, account string, allAccounts bool, format string, limit int) error {
	mailboxID, st, err := resolveReportMailboxID(ctx, cfg, account, allAccounts)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	f, err := validateFormat(format)
	if err != nil {
		return err
	}

	terms, err := analysis.QuerySubjectTerms(ctx, st, mailboxID, limit)
	if err != nil {
		return err
	}
	return analysis.RenderSubjectTerms(w, terms, f)
}

// runReportVolume queries and renders monthly message volume for a mailbox.
// It is separated from the Cobra handler for testability.
func runReportVolume(ctx context.Context, w io.Writer, cfg config.Config, account string, allAccounts bool, format string) error {
	mailboxID, st, err := resolveReportMailboxID(ctx, cfg, account, allAccounts)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	f, err := validateFormat(format)
	if err != nil {
		return err
	}

	rows, err := analysis.QueryVolume(ctx, st, mailboxID)
	if err != nil {
		return err
	}
	return analysis.RenderVolume(w, rows, f)
}
