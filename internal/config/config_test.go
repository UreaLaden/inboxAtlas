package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDefault(t *testing.T) {
	cfg := Default()
	if cfg.LogLevel != "info" {
		t.Errorf("LogLevel: got %q, want %q", cfg.LogLevel, "info")
	}
	if cfg.DefaultProvider != "gmail" {
		t.Errorf("DefaultProvider: got %q, want %q", cfg.DefaultProvider, "gmail")
	}
	if cfg.StoragePath == "" {
		t.Error("StoragePath must not be empty")
	}
	if cfg.TokenDir == "" {
		t.Error("TokenDir must not be empty")
	}
	if cfg.CredentialsPath == "" {
		t.Error("CredentialsPath must not be empty")
	}
}

func TestLoadDefaults(t *testing.T) {
	// Point home away from any real config file by using a temp dir.
	t.Setenv("HOME", t.TempDir())
	t.Setenv("USERPROFILE", t.TempDir()) // Windows

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("LogLevel: got %q, want %q", cfg.LogLevel, "info")
	}
	if cfg.DefaultProvider != "gmail" {
		t.Errorf("DefaultProvider: got %q, want %q", cfg.DefaultProvider, "gmail")
	}
}

func TestEnvVarOverrides(t *testing.T) {
	t.Setenv("INBOXATLAS_STORAGE_PATH", "/tmp/test.db")
	t.Setenv("INBOXATLAS_LOG_LEVEL", "debug")
	t.Setenv("INBOXATLAS_TOKEN_DIR", "/tmp/tokens")
	t.Setenv("INBOXATLAS_DEFAULT_PROVIDER", "outlook")
	t.Setenv("INBOXATLAS_CREDENTIALS_PATH", "/tmp/creds.json")

	cfg := Default()
	applyEnv(&cfg)

	if cfg.StoragePath != "/tmp/test.db" {
		t.Errorf("StoragePath: got %q, want %q", cfg.StoragePath, "/tmp/test.db")
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("LogLevel: got %q, want %q", cfg.LogLevel, "debug")
	}
	if cfg.TokenDir != "/tmp/tokens" {
		t.Errorf("TokenDir: got %q, want %q", cfg.TokenDir, "/tmp/tokens")
	}
	if cfg.DefaultProvider != "outlook" {
		t.Errorf("DefaultProvider: got %q, want %q", cfg.DefaultProvider, "outlook")
	}
	if cfg.CredentialsPath != "/tmp/creds.json" {
		t.Errorf("CredentialsPath: got %q, want %q", cfg.CredentialsPath, "/tmp/creds.json")
	}
}

func TestEnvVarDoesNotOverrideWhenEmpty(t *testing.T) {
	t.Setenv("INBOXATLAS_LOG_LEVEL", "")

	cfg := Default()
	applyEnv(&cfg)

	if cfg.LogLevel != "info" {
		t.Errorf("LogLevel should remain default when env is empty, got %q", cfg.LogLevel)
	}
}

func TestLoadFromTOMLFile(t *testing.T) {
	tmp := t.TempDir()
	cfgDir := filepath.Join(tmp, ".config", "inboxatlas")
	if err := os.MkdirAll(cfgDir, 0o700); err != nil {
		t.Fatal(err)
	}
	tomlContent := `
storage_path = "/custom/path/inboxatlas.db"
log_level = "warn"
token_dir = "/custom/tokens"
default_provider = "gmail"
credentials_path = "/custom/creds.json"
`
	if err := os.WriteFile(filepath.Join(cfgDir, "config.toml"), []byte(tomlContent), 0o600); err != nil {
		t.Fatal(err)
	}

	// Override HOME so Load() finds our temp config file.
	t.Setenv("HOME", tmp)
	t.Setenv("USERPROFILE", tmp)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.StoragePath != "/custom/path/inboxatlas.db" {
		t.Errorf("StoragePath: got %q, want %q", cfg.StoragePath, "/custom/path/inboxatlas.db")
	}
	if cfg.LogLevel != "warn" {
		t.Errorf("LogLevel: got %q, want %q", cfg.LogLevel, "warn")
	}
}

func TestLoadEnvOverridesFile(t *testing.T) {
	tmp := t.TempDir()
	cfgDir := filepath.Join(tmp, ".config", "inboxatlas")
	if err := os.MkdirAll(cfgDir, 0o700); err != nil {
		t.Fatal(err)
	}
	tomlContent := `log_level = "warn"`
	if err := os.WriteFile(filepath.Join(cfgDir, "config.toml"), []byte(tomlContent), 0o600); err != nil {
		t.Fatal(err)
	}

	t.Setenv("HOME", tmp)
	t.Setenv("USERPROFILE", tmp)
	t.Setenv("INBOXATLAS_LOG_LEVEL", "error")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}
	if cfg.LogLevel != "error" {
		t.Errorf("LogLevel: env should override file; got %q, want %q", cfg.LogLevel, "error")
	}
}

func TestLoadInvalidTOML(t *testing.T) {
	tmp := t.TempDir()
	cfgDir := filepath.Join(tmp, ".config", "inboxatlas")
	if err := os.MkdirAll(cfgDir, 0o700); err != nil {
		t.Fatal(err)
	}
	// Write a file with invalid TOML syntax.
	if err := os.WriteFile(filepath.Join(cfgDir, "config.toml"), []byte("log_level = [unclosed"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", tmp)
	t.Setenv("USERPROFILE", tmp)

	_, err := Load()
	if err == nil {
		t.Error("Load() expected error for invalid TOML, got nil")
	}
}

func TestConfig_TokenStorageDefault(t *testing.T) {
	cfg := Default()
	if cfg.TokenStorage != "keyring" {
		t.Errorf("TokenStorage default: got %q, want %q", cfg.TokenStorage, "keyring")
	}
}

func TestConfig_TokenStorageEnvOverride(t *testing.T) {
	t.Setenv("INBOXATLAS_TOKEN_STORAGE", "file")
	cfg := Default()
	applyEnv(&cfg)
	if cfg.TokenStorage != "file" {
		t.Errorf("TokenStorage env override: got %q, want %q", cfg.TokenStorage, "file")
	}
}

func TestEnsureDirs(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)
	t.Setenv("USERPROFILE", tmp)

	if err := EnsureDirs(); err != nil {
		t.Fatalf("EnsureDirs() error: %v", err)
	}

	dirs := []string{
		filepath.Join(tmp, ".config", "inboxatlas"),
		filepath.Join(tmp, ".local", "share", "inboxatlas"),
	}
	for _, d := range dirs {
		if _, err := os.Stat(d); err != nil {
			t.Errorf("expected dir %q to exist: %v", d, err)
		}
	}

	// Idempotent — calling again must not error.
	if err := EnsureDirs(); err != nil {
		t.Fatalf("EnsureDirs() second call error: %v", err)
	}
}
