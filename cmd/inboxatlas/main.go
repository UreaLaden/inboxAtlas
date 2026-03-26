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

	"github.com/UreaLaden/inboxatlas/internal/analysis"
	"github.com/UreaLaden/inboxatlas/internal/auth"
	"github.com/UreaLaden/inboxatlas/internal/config"
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

// buildReportCmd returns the "report" subcommand group.
func buildReportCmd(cfg config.Config) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "report",
		Short: "Generate discovery reports from synced mailbox data",
	}
	cmd.AddCommand(buildReportDomainsCmd(cfg))
	cmd.AddCommand(buildReportSendersCmd(cfg))
	cmd.AddCommand(buildReportSubjectsCmd(cfg))
	cmd.AddCommand(buildReportVolumeCmd(cfg))
	return cmd
}

// buildReportDomainsCmd returns the "report domains" subcommand.
func buildReportDomainsCmd(cfg config.Config) *cobra.Command {
	var account string
	var allAccounts bool
	var format string
	var limit int

	cmd := &cobra.Command{
		Use:   "domains",
		Short: "Report top sending domains",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runReportDomains(cmd.Context(), cmd.OutOrStdout(), cfg, account, allAccounts, format, limit)
		},
	}
	cmd.Flags().StringVar(&account, "account", "", "mailbox email or alias")
	cmd.Flags().BoolVar(&allAccounts, "all-accounts", false, "aggregate across all registered mailboxes")
	cmd.Flags().StringVar(&format, "format", "table", "output format: table, csv, json")
	cmd.Flags().IntVar(&limit, "limit", 25, "maximum number of rows to return")
	return cmd
}

// buildReportSendersCmd returns the "report senders" subcommand.
func buildReportSendersCmd(cfg config.Config) *cobra.Command {
	var account string
	var allAccounts bool
	var format string
	var limit int

	cmd := &cobra.Command{
		Use:   "senders",
		Short: "Report top message senders",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runReportSenders(cmd.Context(), cmd.OutOrStdout(), cfg, account, allAccounts, format, limit)
		},
	}
	cmd.Flags().StringVar(&account, "account", "", "mailbox email or alias")
	cmd.Flags().BoolVar(&allAccounts, "all-accounts", false, "aggregate across all registered mailboxes")
	cmd.Flags().StringVar(&format, "format", "table", "output format: table, csv, json")
	cmd.Flags().IntVar(&limit, "limit", 25, "maximum number of rows to return")
	return cmd
}

// buildReportSubjectsCmd returns the "report subjects" subcommand.
func buildReportSubjectsCmd(cfg config.Config) *cobra.Command {
	var account string
	var allAccounts bool
	var format string
	var limit int

	cmd := &cobra.Command{
		Use:   "subjects",
		Short: "Report top subject line terms",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runReportSubjects(cmd.Context(), cmd.OutOrStdout(), cfg, account, allAccounts, format, limit)
		},
	}
	cmd.Flags().StringVar(&account, "account", "", "mailbox email or alias")
	cmd.Flags().BoolVar(&allAccounts, "all-accounts", false, "aggregate across all registered mailboxes")
	cmd.Flags().StringVar(&format, "format", "table", "output format: table, csv, json")
	cmd.Flags().IntVar(&limit, "limit", 25, "maximum number of rows to return")
	return cmd
}

// buildReportVolumeCmd returns the "report volume" subcommand.
func buildReportVolumeCmd(cfg config.Config) *cobra.Command {
	var account string
	var allAccounts bool
	var format string

	cmd := &cobra.Command{
		Use:   "volume",
		Short: "Report monthly message volume",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runReportVolume(cmd.Context(), cmd.OutOrStdout(), cfg, account, allAccounts, format)
		},
	}
	cmd.Flags().StringVar(&account, "account", "", "mailbox email or alias")
	cmd.Flags().BoolVar(&allAccounts, "all-accounts", false, "aggregate across all registered mailboxes")
	cmd.Flags().StringVar(&format, "format", "table", "output format: table, csv, json")
	return cmd
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
