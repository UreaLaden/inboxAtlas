// Command inboxatlas is the main entrypoint for the InboxAtlas CLI.
package main

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/UreaLaden/inboxatlas/internal/config"
	"github.com/UreaLaden/inboxatlas/internal/version"
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

	return root
}

// buildVersionCmd returns the "version" subcommand.
func buildVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the InboxAtlas version",
		Run: func(_ *cobra.Command, _ []string) {
			fmt.Println(version.Version)
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
		Run: func(_ *cobra.Command, _ []string) {
			fmt.Printf("storage_path:      %s\n", cfg.StoragePath)
			fmt.Printf("log_level:         %s\n", cfg.LogLevel)
			fmt.Printf("token_dir:         %s\n", cfg.TokenDir)
			fmt.Printf("default_provider:  %s\n", cfg.DefaultProvider)
			fmt.Printf("credentials_path:  %s\n", cfg.CredentialsPath)
		},
	}
}
