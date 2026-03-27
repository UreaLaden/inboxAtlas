// Command inboxatlas is the main entrypoint for the InboxAtlas CLI.
package main

import (
	"bufio"
	"context"
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
var newGmailProvider = func(email string, tokenSourceFactory func(context.Context) (oauth2.TokenSource, error)) models.MailProvider {
	return gmailprovider.New(email, tokenSourceFactory)
}
var runIngestion = ingestion.Run
var reportExportPDFRenderer exportpkg.PDFRenderer

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

// runSyncGmail resolves the mailbox, resolves the auth mode, builds a Gmail provider,
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
	tokenSourceFactory, err := resolveGmailTokenSource(&cfg, mb.ID)
	if err != nil {
		return err
	}

	provider := newGmailProvider(mb.ID, tokenSourceFactory)

	return runIngestion(ctx, ingestion.Options{
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
	cmd.AddCommand(buildClassifySuggestionsCmd(cfg))
	cmd.AddCommand(buildClassifyPromoteCmd(cfg))
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

func validateClassifyFormat(f string) (string, error) {
	switch f {
	case "table", "json":
		return f, nil
	default:
		return "", fmt.Errorf("unknown format %q — valid values: table, json", f)
	}
}

// runClassifyRun executes mailbox-scoped classification for one mailbox.
func runClassifyRun(ctx context.Context, w io.Writer, cfg config.Config, account string) error {
	result, err := engine.RunClassify(ctx, cfg, account)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(w, "Classified %d messages for %s.\n", result.MessagesProcessed, result.MailboxID)
	return nil
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

// buildReportCmd returns the "report" subcommand group.
func buildReportCmd(cfg config.Config) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "report",
		Short: "Generate discovery reports from synced mailbox data",
	}
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
