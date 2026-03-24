// Package config handles loading, defaulting, and environment-variable override
// of the InboxAtlas configuration.
package config

import (
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"
)

// Config holds all runtime configuration values for InboxAtlas.
// Values are resolved by merging defaults, the TOML config file, and
// environment variable overrides (in that precedence order).
type Config struct {
	StoragePath     string `toml:"storage_path"`
	LogLevel        string `toml:"log_level"`
	TokenDir        string `toml:"token_dir"`
	DefaultProvider string `toml:"default_provider"`
	CredentialsPath string `toml:"credentials_path"`
}

// Default returns a Config populated with default values derived from the
// current user's home directory.
func Default() Config {
	home := mustHomeDir()
	return Config{
		StoragePath:     filepath.Join(home, ".local", "share", "inboxatlas", "inboxatlas.db"),
		LogLevel:        "info",
		TokenDir:        filepath.Join(home, ".config", "inboxatlas", "tokens"),
		DefaultProvider: "gmail",
		CredentialsPath: filepath.Join(home, ".config", "inboxatlas", "credentials.json"),
	}
}

// Load returns the resolved Config by applying defaults, then the TOML config
// file (if present), then environment variable overrides. It returns an error
// only if a config file exists but cannot be parsed.
func Load() (Config, error) {
	cfg := Default()

	cfgFile := filepath.Join(mustHomeDir(), ".config", "inboxatlas", "config.toml")
	if _, err := os.Stat(cfgFile); err == nil {
		if _, err := toml.DecodeFile(cfgFile, &cfg); err != nil {
			return cfg, err
		}
	}

	applyEnv(&cfg)
	return cfg, nil
}

// EnsureDirs creates the InboxAtlas config and data directories if they do not
// already exist. It is safe to call on every startup.
func EnsureDirs() error {
	home := mustHomeDir()
	dirs := []string{
		filepath.Join(home, ".config", "inboxatlas"),
		filepath.Join(home, ".local", "share", "inboxatlas"),
	}
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return err
		}
	}
	return nil
}

// applyEnv overrides cfg fields with values from INBOXATLAS_* environment
// variables. Non-empty env vars always win over the config file.
func applyEnv(cfg *Config) {
	if v := os.Getenv("INBOXATLAS_STORAGE_PATH"); v != "" {
		cfg.StoragePath = v
	}
	if v := os.Getenv("INBOXATLAS_LOG_LEVEL"); v != "" {
		cfg.LogLevel = v
	}
	if v := os.Getenv("INBOXATLAS_TOKEN_DIR"); v != "" {
		cfg.TokenDir = v
	}
	if v := os.Getenv("INBOXATLAS_DEFAULT_PROVIDER"); v != "" {
		cfg.DefaultProvider = v
	}
	if v := os.Getenv("INBOXATLAS_CREDENTIALS_PATH"); v != "" {
		cfg.CredentialsPath = v
	}
}

// mustHomeDir returns the current user's home directory. It panics if the home
// directory cannot be determined, which indicates a broken OS environment.
func mustHomeDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		panic("inboxatlas: cannot determine home directory: " + err.Error())
	}
	return home
}
