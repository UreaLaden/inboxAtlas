package auth

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	keyring "github.com/zalando/go-keyring"
	"golang.org/x/oauth2"

	"github.com/UreaLaden/inboxatlas/internal/config"
)

type pipeListener struct {
	conn   net.Conn
	addr   net.Addr
	used   bool
	closed chan struct{}
}

func newPipeListener() (*pipeListener, net.Conn) {
	serverConn, clientConn := net.Pipe()
	return &pipeListener{
		conn:   serverConn,
		addr:   &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0},
		closed: make(chan struct{}),
	}, clientConn
}

func (l *pipeListener) Accept() (net.Conn, error) {
	if !l.used {
		l.used = true
		return l.conn, nil
	}
	<-l.closed
	return nil, net.ErrClosed
}

func (l *pipeListener) Close() error {
	select {
	case <-l.closed:
	default:
		close(l.closed)
	}
	return l.conn.Close()
}

func (l *pipeListener) Addr() net.Addr { return l.addr }

func desktopCredentialsJSON() string {
	return `{"installed":{"client_id":"test-id","client_secret":"test-secret","redirect_uris":["http://localhost"]}}`
}

func serviceAccountCredentialsJSON() string {
	return `{
  "type": "service_account",
  "project_id": "test-project",
  "private_key_id": "test-key-id",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDXY9J2xep1+t0NenE2\nRj3Xr1m66FQmQF5wUxyX5wz12j7YkIcR2F99a8xj2zd0Q0imeISFRCGDpa2BkLomqKgP\n0vvArkH5AO9M/dwZ0pniS3pSke1Mt2rt7NmBGG99nmCaTKty+O+kO5RAwOB1p5MNDoAu\n0aKBslZx2drXr/7EQo1leChX2NDVYqoQZnIcXtNCmieYwgd0CQpZK0s8AjtKoaVHgMHq\nYJv1nVbWcv16O3MHuvb6jVWeItPxX2VINeodIZ6Tn6PvxI6Bfq5lHppZArYrusS4x+h0\nfOkH1ZZ1xdjQbaO5SpM90oCbGyF/F7fs/3Gzdh0dX8GZFODdgNpTi27DAgMBAAECggEA\nAJnXrhIRbkIuAeGHNirMRHkRkNvztNFVQVw1Gc7YCOUMIqFZ3VAbw/T/jGj33jjXNMTv\nEihQ/HVbUaz2YsmiVjCwXTlzAIXazhbugzuDUFcPRl1BDpRP70dNDO7xjMnIKh4j/wcg\n3NEPoPFcAckU4iigIvuXvYDn8ApX2HFqRSbuuSSMzdON3NofM8JrIoYNewc0hXtOD87b\niV/mQJu1WDVYj1WFJsbgx5caX5/C/PObbIVdQydb9h9NP7VDaRao7IhiHBpjz2uVH54F\natoNgtrENcGukdxbYlR5c+3F4iAfDdc0AGJi/7luWGINuD/7++UZ5EKeosFVJeFt3PcT\nYQKBgQD0iDLuhvZux41DmvNmO6PsK0uFUxnCLzpSw0DxVh1T/kZLKZVugRUIaBDdYQzI\nKoEt1TGVtC6zqD2c/Ik2Vq6KYFAsktwVDqveufqdpypuXn7Z1xXHDD236UMtO4Zwzp1T\nwHjMATkUMlzUxr87hcPLZ9eczsQnUnxE27XGr0C+ZwKBgQDg4YAlnXrhIRbkIuAeGHNi\nrMRHkRkNvztNFVQVw1Gc7YCOUMIqFZ3VAbw/T/jGj33jjXNMTvEihQ/HVbUaz2YsmiVj\nCwXTlzAIXazhbugzuDUFcPRl1BDpRP70dNDO7xjMnIKh4j/wcg3NEPoPFcAckU4iigIv\nuXvYDn8ApX2HFqRSbQKBgQCQvOB0fOkH1ZZ1xdjQbaO5SpM90oCbGyF/F7fs/3Gzdh0d\nX8GZFODdgNpTi27DANJQImQxGc1dQc5sKXc5teLoI0lp4rWuIwoMvVJE9idh+NangNh4\ntW7x1YgnSUZXoqBYwygJyI072QtdgQXl3k5ufADG7n2AFDzy83H8XTur2QKBgG1dQc5s\nKXc5teLoI0lp4rWuIwoMvVJE9idh+NangNh4tW7x1YgnSUZXoqBYwygJyI072QtdgQXl\n3k5ufADG7n2AFDzy83H8XTur2qxGn8pY/+bexdFv+DE5jBqFaUG2RgxN6E466+vWXTjh\nAIXazhbugzuDUFcPRl1BDpRP70dNDO7xAoGAHdCqkfNNpJBWlAbIYW/W2PASi6DPd7OJ\nRRqtD9h5pz50jdK5Zk90un0nLBKBPXn1HULICwhf66A1VpzwuNFuIBqmoeZaZX6mE6PD\nLl35H5TADaBrZEcD3xKhsR4HIX66vepQP9en5ZaY1f+T5iAG2wE8xmPKzW0fvkY=\n-----END PRIVATE KEY-----\n",
  "client_email": "svc-account@test-project.iam.gserviceaccount.com",
  "client_id": "1234567890",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/svc-account%40test-project.iam.gserviceaccount.com"
}`
}

// --- LoadCredentials ---

func TestLoadCredentials_Valid(t *testing.T) {
	dir := t.TempDir()
	creds := desktopCredentialsJSON()
	p := filepath.Join(dir, "credentials.json")
	if err := os.WriteFile(p, []byte(creds), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadCredentials(p)
	if err != nil {
		t.Fatalf("LoadCredentials: %v", err)
	}
	if cfg.ClientID != "test-id" {
		t.Errorf("ClientID: got %q, want %q", cfg.ClientID, "test-id")
	}
	if cfg.ClientSecret != "test-secret" {
		t.Errorf("ClientSecret: got %q, want %q", cfg.ClientSecret, "test-secret")
	}
	if len(cfg.Scopes) != 1 || cfg.Scopes[0] != gmailScope {
		t.Errorf("Scopes: got %v, want [%s]", cfg.Scopes, gmailScope)
	}
}

func TestLoadServiceAccountJWTConfig_Valid(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "credentials.json")
	if err := os.WriteFile(p, []byte(serviceAccountCredentialsJSON()), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadServiceAccountJWTConfig(p)
	if err != nil {
		t.Fatalf("LoadServiceAccountJWTConfig: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected JWT config")
		return
	}
	if cfg.Email != "svc-account@test-project.iam.gserviceaccount.com" {
		t.Fatalf("Email = %q", cfg.Email)
	}
	if cfg.Subject != "" {
		t.Fatalf("Subject = %q, want empty", cfg.Subject)
	}
}

func TestLoadServiceAccountJWTConfig_NotFound(t *testing.T) {
	_, err := LoadServiceAccountJWTConfig(filepath.Join(t.TempDir(), "missing.json"))
	if err == nil || !strings.Contains(err.Error(), "read credentials") {
		t.Fatalf("expected read credentials error, got %v", err)
	}
}

func TestResolveGmailTokenSource_Delegated(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.CredentialsPath = filepath.Join(dir, "service-account.json")
	if err := os.WriteFile(cfg.CredentialsPath, []byte(serviceAccountCredentialsJSON()), 0o600); err != nil {
		t.Fatal(err)
	}

	factory, err := ResolveGmailTokenSource(&cfg, "User@Example.com")
	if err != nil {
		t.Fatalf("ResolveGmailTokenSource: %v", err)
	}
	src, err := factory(context.Background())
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	if src == nil {
		t.Fatal("expected token source")
	}
}

func TestResolveGmailTokenSource_UserToken(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.CredentialsPath = filepath.Join(dir, "desktop.json")
	cfg.TokenDir = filepath.Join(dir, "tokens")
	cfg.TokenStorage = "file"
	if err := os.WriteFile(cfg.CredentialsPath, []byte(desktopCredentialsJSON()), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := SaveToken(cfg.TokenDir, "gmail", "user@example.com", &oauth2.Token{AccessToken: "tok"}); err != nil {
		t.Fatal(err)
	}

	factory, err := ResolveGmailTokenSource(&cfg, "user@example.com")
	if err != nil {
		t.Fatalf("ResolveGmailTokenSource: %v", err)
	}
	src, err := factory(context.Background())
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	token, err := src.Token()
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	if token.AccessToken != "tok" {
		t.Fatalf("AccessToken = %q, want tok", token.AccessToken)
	}
}

func TestResolveGmailTokenSource_NoStoredToken(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.CredentialsPath = filepath.Join(dir, "desktop.json")
	cfg.TokenDir = filepath.Join(dir, "tokens")
	cfg.TokenStorage = "file"
	if err := os.WriteFile(cfg.CredentialsPath, []byte(desktopCredentialsJSON()), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := ResolveGmailTokenSource(&cfg, "user@example.com")
	if err == nil {
		t.Fatal("expected error when no stored token exists")
	}
	if !strings.Contains(err.Error(), "no stored user token") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestResolveGmailTokenSource_MalformedCredentials(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Default()
	cfg.CredentialsPath = filepath.Join(dir, "bad.json")
	if err := os.WriteFile(cfg.CredentialsPath, []byte("not-json"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := ResolveGmailTokenSource(&cfg, "user@example.com")
	if err == nil {
		t.Fatal("expected error for malformed credentials")
	}
}

func TestResolveGmailTokenSource_ReadCredentialsError(t *testing.T) {
	cfg := config.Default()
	cfg.CredentialsPath = filepath.Join(t.TempDir(), "missing.json")

	_, err := ResolveGmailTokenSource(&cfg, "user@example.com")
	if err == nil || !strings.Contains(err.Error(), "read credentials") {
		t.Fatalf("expected read credentials error, got %v", err)
	}
}

func TestValidateGmailDelegation_Success(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "service-account.json")
	if err := os.WriteFile(p, []byte(serviceAccountCredentialsJSON()), 0o600); err != nil {
		t.Fatal(err)
	}

	orig := gmailProfileFetcher
	gmailProfileFetcher = func(_ context.Context, _ oauth2.TokenSource) (string, error) {
		return "user@example.com", nil
	}
	t.Cleanup(func() { gmailProfileFetcher = orig })

	if err := ValidateGmailDelegation(context.Background(), p, "User@Example.com"); err != nil {
		t.Fatalf("ValidateGmailDelegation: %v", err)
	}
}

func TestValidateGmailDelegation_ProfileMismatch(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "service-account.json")
	if err := os.WriteFile(p, []byte(serviceAccountCredentialsJSON()), 0o600); err != nil {
		t.Fatal(err)
	}

	orig := gmailProfileFetcher
	gmailProfileFetcher = func(_ context.Context, _ oauth2.TokenSource) (string, error) {
		return "other@example.com", nil
	}
	t.Cleanup(func() { gmailProfileFetcher = orig })

	err := ValidateGmailDelegation(context.Background(), p, "user@example.com")
	if err == nil {
		t.Fatal("expected mismatch error")
	}
}

func TestValidateGmailDelegation_ProfileFetchError(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "service-account.json")
	if err := os.WriteFile(p, []byte(serviceAccountCredentialsJSON()), 0o600); err != nil {
		t.Fatal(err)
	}

	orig := gmailProfileFetcher
	gmailProfileFetcher = func(_ context.Context, _ oauth2.TokenSource) (string, error) {
		return "", errors.New("gmail unavailable")
	}
	t.Cleanup(func() { gmailProfileFetcher = orig })

	err := ValidateGmailDelegation(context.Background(), p, "user@example.com")
	if err == nil || !strings.Contains(err.Error(), "validate delegated gmail auth") {
		t.Fatalf("expected wrapped fetch error, got %v", err)
	}
}

func TestLoadCredentials_NotFound(t *testing.T) {
	_, err := LoadCredentials("/nonexistent/credentials.json")
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestLoadCredentials_MalformedJSON(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "credentials.json")
	if err := os.WriteFile(p, []byte("not-valid-json"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := LoadCredentials(p)
	if err == nil {
		t.Error("expected error for malformed JSON")
	}
}

func TestLoadCredentials_MissingInstalled(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "credentials.json")
	if err := os.WriteFile(p, []byte(`{"web":{"client_id":"x"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := LoadCredentials(p)
	if err == nil {
		t.Error("expected error for missing 'installed' key")
	}
	if !strings.Contains(err.Error(), "installed") {
		t.Errorf("expected 'installed' in error, got: %v", err)
	}
}

func TestDetectGmailCredentialsKind(t *testing.T) {
	kind, err := detectGmailCredentialsKind([]byte(desktopCredentialsJSON()))
	if err != nil {
		t.Fatalf("detectGmailCredentialsKind installed: %v", err)
	}
	if kind != gmailCredentialsKindInstalled {
		t.Fatalf("kind: got %q, want %q", kind, gmailCredentialsKindInstalled)
	}

	kind, err = detectGmailCredentialsKind([]byte(serviceAccountCredentialsJSON()))
	if err != nil {
		t.Fatalf("detectGmailCredentialsKind service account: %v", err)
	}
	if kind != gmailCredentialsKindServiceAccount {
		t.Fatalf("kind: got %q, want %q", kind, gmailCredentialsKindServiceAccount)
	}
}

func TestLoadServiceAccountJWTConfigJSON_RejectsInstalledApp(t *testing.T) {
	_, err := loadServiceAccountJWTConfigJSON([]byte(desktopCredentialsJSON()))
	if err == nil {
		t.Fatal("expected installed-app credentials to be rejected")
	}
}

func TestLoadServiceAccountJWTConfig_RejectsInstalledAppFile(t *testing.T) {
	p := filepath.Join(t.TempDir(), "desktop.json")
	if err := os.WriteFile(p, []byte(desktopCredentialsJSON()), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := LoadServiceAccountJWTConfig(p)
	if err == nil || !strings.Contains(err.Error(), "not a service account key") {
		t.Fatalf("expected service account rejection, got %v", err)
	}
}

func TestDetectGmailCredentialsKind_RejectsUnsupportedJSON(t *testing.T) {
	_, err := detectGmailCredentialsKind([]byte(`{"web":{"client_id":"x"}}`))
	if err == nil || !strings.Contains(err.Error(), "desktop OAuth") {
		t.Fatalf("expected unsupported credentials error, got %v", err)
	}
}

// --- TokenPath ---

func TestTokenPath_Deterministic(t *testing.T) {
	p1 := TokenPath("/tokens", "gmail", "user@example.com")
	p2 := TokenPath("/tokens", "gmail", "user@example.com")
	if p1 != p2 {
		t.Errorf("TokenPath not deterministic: %q != %q", p1, p2)
	}
}

func TestTokenPath_CaseInsensitive(t *testing.T) {
	lower := TokenPath("/tokens", "gmail", "user@example.com")
	upper := TokenPath("/tokens", "gmail", "USER@EXAMPLE.COM")
	if lower != upper {
		t.Errorf("expected same path for different case: %q != %q", lower, upper)
	}
}

func TestTokenPath_ContainsProvider(t *testing.T) {
	p := TokenPath("/tokens", "gmail", "user@example.com")
	if !strings.Contains(p, "gmail") {
		t.Errorf("expected provider in path, got: %q", p)
	}
	if !strings.HasSuffix(p, ".json") {
		t.Errorf("expected .json suffix, got: %q", p)
	}
}

// --- SaveToken / LoadToken ---

func TestSaveToken_LoadToken_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	token := &oauth2.Token{
		AccessToken:  "access-token-value",
		RefreshToken: "refresh-token-value",
		TokenType:    "Bearer",
	}
	if err := SaveToken(dir, "gmail", "user@example.com", token); err != nil {
		t.Fatalf("SaveToken: %v", err)
	}
	loaded, err := LoadToken(dir, "gmail", "user@example.com")
	if err != nil {
		t.Fatalf("LoadToken: %v", err)
	}
	if loaded.AccessToken != token.AccessToken {
		t.Errorf("AccessToken: got %q, want %q", loaded.AccessToken, token.AccessToken)
	}
	if loaded.RefreshToken != token.RefreshToken {
		t.Errorf("RefreshToken: got %q, want %q", loaded.RefreshToken, token.RefreshToken)
	}
}

func TestSaveToken_CreatesDirectory(t *testing.T) {
	dir := t.TempDir()
	token := &oauth2.Token{AccessToken: "tok"}
	if err := SaveToken(dir, "gmail", "user@example.com", token); err != nil {
		t.Fatalf("SaveToken: %v", err)
	}
	p := TokenPath(dir, "gmail", "user@example.com")
	if _, err := os.Stat(p); err != nil {
		t.Errorf("token file not created: %v", err)
	}
}

func TestSaveToken_FileMode(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not enforce Unix file permission bits")
	}
	dir := t.TempDir()
	token := &oauth2.Token{AccessToken: "tok"}
	if err := SaveToken(dir, "gmail", "user@example.com", token); err != nil {
		t.Fatal(err)
	}
	p := TokenPath(dir, "gmail", "user@example.com")
	info, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		t.Errorf("token file mode too permissive: %v", info.Mode())
	}
}

func TestLoadToken_NotFound(t *testing.T) {
	dir := t.TempDir()
	_, err := LoadToken(dir, "gmail", "nobody@example.com")
	if err == nil {
		t.Error("expected error for missing token")
	}
}

func TestLoadToken_InvalidJSON(t *testing.T) {
	dir := t.TempDir()
	p := TokenPath(dir, "gmail", "user@example.com")
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("not-json"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := LoadToken(dir, "gmail", "user@example.com")
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

// --- FileTokenStorage ---

func TestFileTokenStorage_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	ts := &FileTokenStorage{TokenDir: dir}
	token := &oauth2.Token{AccessToken: "file-access", RefreshToken: "file-refresh"}
	if err := ts.Save("gmail", "user@example.com", token); err != nil {
		t.Fatalf("FileTokenStorage.Save: %v", err)
	}
	loaded, err := ts.Load("gmail", "user@example.com")
	if err != nil {
		t.Fatalf("FileTokenStorage.Load: %v", err)
	}
	if loaded.AccessToken != token.AccessToken {
		t.Errorf("AccessToken: got %q, want %q", loaded.AccessToken, token.AccessToken)
	}
}

// --- KeyringTokenStorage ---

func TestKeyringTokenStorage_RoundTrip(t *testing.T) {
	keyring.MockInit()
	dir := t.TempDir()
	ks := &KeyringTokenStorage{service: "inboxatlas", fallback: &FileTokenStorage{TokenDir: dir}}
	token := &oauth2.Token{AccessToken: "keyring-access", RefreshToken: "keyring-refresh"}
	if err := ks.Save("gmail", "user@example.com", token); err != nil {
		t.Fatalf("KeyringTokenStorage.Save: %v", err)
	}
	loaded, err := ks.Load("gmail", "user@example.com")
	if err != nil {
		t.Fatalf("KeyringTokenStorage.Load: %v", err)
	}
	if loaded.AccessToken != token.AccessToken {
		t.Errorf("AccessToken: got %q, want %q", loaded.AccessToken, token.AccessToken)
	}
}

func TestKeyringTokenStorage_FallsBackToFile(t *testing.T) {
	// Initialize empty keyring mock — Load will get ErrNotFound and fall back to file.
	keyring.MockInit()
	dir := t.TempDir()
	fallback := &FileTokenStorage{TokenDir: dir}
	ks := &KeyringTokenStorage{service: "inboxatlas", fallback: fallback}

	// Write the token to the file store directly (simulating a pre-keyring token).
	token := &oauth2.Token{AccessToken: "file-fallback-token", RefreshToken: "rf"}
	if err := SaveToken(dir, "gmail", "user@example.com", token); err != nil {
		t.Fatal(err)
	}

	// Load via KeyringTokenStorage — keyring has no entry, so it falls back to file.
	loaded, err := ks.Load("gmail", "user@example.com")
	if err != nil {
		t.Fatalf("KeyringTokenStorage.Load (fallback): %v", err)
	}
	if loaded.AccessToken != token.AccessToken {
		t.Errorf("AccessToken: got %q, want %q (expected fallback to file)", loaded.AccessToken, token.AccessToken)
	}
}

func TestKeyringTokenStorage_Load_InvalidJSON(t *testing.T) {
	keyring.MockInit()
	dir := t.TempDir()
	ks := &KeyringTokenStorage{service: "inboxatlas", fallback: &FileTokenStorage{TokenDir: dir}}
	key := keyringKey("gmail", "user@example.com")
	if err := keyring.Set("inboxatlas", key, "not-json"); err != nil {
		t.Fatalf("keyring.Set: %v", err)
	}

	_, err := ks.Load("gmail", "user@example.com")
	if err == nil || !strings.Contains(err.Error(), "parse keyring token") {
		t.Fatalf("expected parse keyring token error, got %v", err)
	}
}

// --- NewTokenStorage ---

func TestNewTokenStorage_ReturnsFile(t *testing.T) {
	cfg := &config.Config{TokenDir: t.TempDir(), TokenStorage: "file"}
	ts := NewTokenStorage(cfg)
	if _, ok := ts.(*FileTokenStorage); !ok {
		t.Errorf("expected *FileTokenStorage, got %T", ts)
	}
}

func TestNewTokenStorage_ReturnsKeyring(t *testing.T) {
	cfg := &config.Config{TokenDir: t.TempDir(), TokenStorage: "keyring"}
	ts := NewTokenStorage(cfg)
	if _, ok := ts.(*KeyringTokenStorage); !ok {
		t.Errorf("expected *KeyringTokenStorage, got %T", ts)
	}
}

// --- saveFromSource (RefreshAndSave testable core) ---

type mockTokenSource struct {
	token *oauth2.Token
	err   error
}

type errReader struct{}

func (errReader) Read(_ []byte) (int, error) {
	return 0, errors.New("entropy failed")
}

func (m *mockTokenSource) Token() (*oauth2.Token, error) {
	return m.token, m.err
}

func TestSaveFromSource_Success(t *testing.T) {
	dir := t.TempDir()
	newToken := &oauth2.Token{AccessToken: "refreshed-token", RefreshToken: "refresh"}
	src := &mockTokenSource{token: newToken}

	got, err := saveFromSource(src, &FileTokenStorage{TokenDir: dir}, "gmail", "user@example.com")
	if err != nil {
		t.Fatalf("saveFromSource: %v", err)
	}
	if got.AccessToken != newToken.AccessToken {
		t.Errorf("got %q, want %q", got.AccessToken, newToken.AccessToken)
	}
	loaded, err := LoadToken(dir, "gmail", "user@example.com")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.AccessToken != newToken.AccessToken {
		t.Errorf("persisted %q, want %q", loaded.AccessToken, newToken.AccessToken)
	}
}

func TestSaveFromSource_TokenError(t *testing.T) {
	dir := t.TempDir()
	src := &mockTokenSource{err: fmt.Errorf("token refresh failed")}
	_, err := saveFromSource(src, &FileTokenStorage{TokenDir: dir}, "gmail", "user@example.com")
	if err == nil {
		t.Error("expected error when token source fails")
	}
}

func TestSaveFromSource_SaveTokenError(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "gmail"), []byte("block"), 0o600); err != nil {
		t.Fatal(err)
	}

	src := &mockTokenSource{token: &oauth2.Token{AccessToken: "refreshed-token"}}
	if _, err := saveFromSource(src, &FileTokenStorage{TokenDir: dir}, "gmail", "user@example.com"); err == nil {
		t.Fatal("expected error when refreshed token cannot be persisted")
	}
}

func TestRefreshAndSave_LoadError(t *testing.T) {
	dir := t.TempDir()
	cfg := &oauth2.Config{}
	_, err := RefreshAndSave(context.Background(), cfg, &FileTokenStorage{TokenDir: dir}, "gmail", "nobody@example.com")
	if err == nil {
		t.Error("expected error when token file does not exist")
	}
}

// --- runFlow ---

func TestRunFlow_Success(t *testing.T) {
	listener := mustListenTCP4(t)
	port := listener.Addr().(*net.TCPAddr).Port
	state := "test-state-abc"

	wantToken := &oauth2.Token{AccessToken: "flow-token"}
	ctx := context.Background()

	done := make(chan error, 1)
	var got *oauth2.Token
	go func() {
		var flowErr error
		got, flowErr = runFlow(ctx, state, listener, nil, func(_ context.Context, code string) (*oauth2.Token, error) {
			if code != "authcode123" {
				return nil, fmt.Errorf("unexpected code: %q", code)
			}
			return wantToken, nil
		})
		done <- flowErr
	}()

	waitForListener(t, listener.Addr().String())
	url := fmt.Sprintf("http://127.0.0.1:%d/?state=%s&code=authcode123", port, state)
	resp, err := http.Get(url) //nolint:noctx
	if err != nil {
		t.Fatalf("GET callback: %v", err)
	}
	_ = resp.Body.Close()

	if err := <-done; err != nil {
		t.Fatalf("runFlow: %v", err)
	}
	if got.AccessToken != wantToken.AccessToken {
		t.Errorf("got %q, want %q", got.AccessToken, wantToken.AccessToken)
	}
}

func TestRunFlow_StateMismatch(t *testing.T) {
	listener := mustListenTCP4(t)
	port := listener.Addr().(*net.TCPAddr).Port
	ctx := context.Background()

	done := make(chan error, 1)
	go func() {
		_, flowErr := runFlow(ctx, "correct-state", listener, nil, func(_ context.Context, _ string) (*oauth2.Token, error) {
			return &oauth2.Token{}, nil
		})
		done <- flowErr
	}()

	waitForListener(t, listener.Addr().String())
	url := fmt.Sprintf("http://127.0.0.1:%d/?state=wrong-state&code=x", port)
	resp, _ := http.Get(url) //nolint:noctx
	if resp != nil {
		_ = resp.Body.Close()
	}

	if err := <-done; err == nil {
		t.Error("expected state mismatch error")
	}
}

func TestRunFlow_OAuthError(t *testing.T) {
	listener := mustListenTCP4(t)
	port := listener.Addr().(*net.TCPAddr).Port
	ctx := context.Background()

	done := make(chan error, 1)
	go func() {
		_, flowErr := runFlow(ctx, "state", listener, nil, func(_ context.Context, _ string) (*oauth2.Token, error) {
			return &oauth2.Token{}, nil
		})
		done <- flowErr
	}()

	waitForListener(t, listener.Addr().String())
	url := fmt.Sprintf("http://127.0.0.1:%d/?state=state&error=access_denied", port)
	resp, _ := http.Get(url) //nolint:noctx
	if resp != nil {
		_ = resp.Body.Close()
	}

	if err := <-done; err == nil {
		t.Error("expected OAuth error")
	}
}

func TestRunFlow_NoCode(t *testing.T) {
	listener := mustListenTCP4(t)
	port := listener.Addr().(*net.TCPAddr).Port
	ctx := context.Background()

	done := make(chan error, 1)
	go func() {
		_, flowErr := runFlow(ctx, "state", listener, nil, func(_ context.Context, _ string) (*oauth2.Token, error) {
			return &oauth2.Token{}, nil
		})
		done <- flowErr
	}()

	waitForListener(t, listener.Addr().String())
	url := fmt.Sprintf("http://127.0.0.1:%d/?state=state", port)
	resp, _ := http.Get(url) //nolint:noctx
	if resp != nil {
		_ = resp.Body.Close()
	}

	if err := <-done; err == nil {
		t.Error("expected error for missing code")
	}
}

func TestRunFlow_ContextCancelled(t *testing.T) {
	listener := mustListenTCP4(t)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		_, flowErr := runFlow(ctx, "state", listener, nil, func(_ context.Context, _ string) (*oauth2.Token, error) {
			return &oauth2.Token{}, nil
		})
		done <- flowErr
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	if err := <-done; err == nil {
		t.Error("expected context cancelled error")
	}
}

// --- generateState ---

func TestGenerateState_NonEmpty(t *testing.T) {
	s, err := generateState()
	if err != nil {
		t.Fatalf("generateState: %v", err)
	}
	if s == "" {
		t.Error("expected non-empty state")
	}
}

func TestGenerateState_Unique(t *testing.T) {
	s1, err := generateState()
	if err != nil {
		t.Fatalf("generateState: %v", err)
	}
	s2, err := generateState()
	if err != nil {
		t.Fatalf("generateState: %v", err)
	}
	if s1 == s2 {
		t.Error("expected unique states")
	}
}

func TestGenerateState_Error(t *testing.T) {
	original := stateEntropyReader
	stateEntropyReader = errReader{}
	t.Cleanup(func() { stateEntropyReader = original })

	if _, err := generateState(); err == nil {
		t.Fatal("expected error when entropy reader fails")
	}
}

// --- SaveToken token value not exposed in JSON key names ---

func TestSaveToken_TokenValueNotInPath(t *testing.T) {
	dir := t.TempDir()
	token := &oauth2.Token{AccessToken: "super-secret-access-token"}
	if err := SaveToken(dir, "gmail", "user@example.com", token); err != nil {
		t.Fatal(err)
	}
	p := TokenPath(dir, "gmail", "user@example.com")
	if strings.Contains(p, "super-secret") {
		t.Errorf("token value leaked into path: %q", p)
	}
}

// --- RefreshAndSave integration (happy path via saveFromSource) ---

func TestRefreshAndSave_PersistsUpdatedToken(t *testing.T) {
	dir := t.TempDir()
	initial := &oauth2.Token{AccessToken: "old", RefreshToken: "rf", Expiry: time.Now().Add(-time.Hour)}
	if err := SaveToken(dir, "gmail", "user@example.com", initial); err != nil {
		t.Fatal(err)
	}
	newTok := &oauth2.Token{AccessToken: "new", RefreshToken: "rf"}
	src := &mockTokenSource{token: newTok}

	got, err := saveFromSource(src, &FileTokenStorage{TokenDir: dir}, "gmail", "user@example.com")
	if err != nil {
		t.Fatal(err)
	}
	if got.AccessToken != "new" {
		t.Errorf("got %q, want %q", got.AccessToken, "new")
	}

	loaded, _ := LoadToken(dir, "gmail", "user@example.com")
	if loaded == nil || loaded.AccessToken != "new" {
		t.Error("token not persisted after refresh")
	}
}

// --- RefreshAndSave happy path ---

func TestRefreshAndSave_ValidToken(t *testing.T) {
	dir := t.TempDir()
	tok := &oauth2.Token{
		AccessToken:  "valid-access",
		RefreshToken: "refresh",
		Expiry:       time.Now().Add(time.Hour), // still valid
	}
	if err := SaveToken(dir, "gmail", "user@example.com", tok); err != nil {
		t.Fatal(err)
	}
	got, err := RefreshAndSave(context.Background(), &oauth2.Config{}, &FileTokenStorage{TokenDir: dir}, "gmail", "user@example.com")
	if err != nil {
		t.Fatalf("RefreshAndSave: %v", err)
	}
	if got.AccessToken != tok.AccessToken {
		t.Errorf("got %q, want %q", got.AccessToken, tok.AccessToken)
	}
}

// --- SaveToken mkdir error ---

func TestSaveToken_MkdirError(t *testing.T) {
	dir := t.TempDir()
	// Create a regular file at the path where the provider directory would go,
	// so MkdirAll cannot create it as a directory.
	if err := os.WriteFile(filepath.Join(dir, "gmail"), []byte("block"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := SaveToken(dir, "gmail", "user@example.com", &oauth2.Token{AccessToken: "tok"})
	if err == nil {
		t.Error("expected error when provider directory cannot be created")
	}
}

// --- RunFlow via exported entry-point with cancelled context ---

func TestRunFlow_ViaRunFlow_CancelContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel so the flow exits immediately

	var buf strings.Builder
	_, err := RunFlow(ctx, &oauth2.Config{}, &buf)
	if err == nil {
		t.Error("expected error for pre-cancelled context")
	}
}

func TestRunFlow_WithPipeListener(t *testing.T) {
	listener, client := newPipeListener()
	defer func() { _ = client.Close() }()

	done := make(chan struct{})
	var (
		gotToken *oauth2.Token
		gotErr   error
	)
	go func() {
		gotToken, gotErr = runFlow(context.Background(), "pipe-state", listener, nil, func(_ context.Context, code string) (*oauth2.Token, error) {
			if code != "pipe-code" {
				return nil, fmt.Errorf("unexpected code %q", code)
			}
			return &oauth2.Token{AccessToken: "pipe-token"}, nil
		})
		close(done)
	}()

	if err := client.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("SetDeadline: %v", err)
	}
	if _, err := io.WriteString(client, "GET /?state=pipe-state&code=pipe-code HTTP/1.1\r\nHost: localhost\r\n\r\n"); err != nil {
		t.Fatalf("WriteString: %v", err)
	}
	if _, err := io.ReadAll(client); err != nil && !strings.Contains(err.Error(), "closed pipe") {
		t.Fatalf("ReadAll: %v", err)
	}

	<-done
	if gotErr != nil {
		t.Fatalf("runFlow: %v", gotErr)
	}
	if gotToken == nil || gotToken.AccessToken != "pipe-token" {
		t.Fatalf("unexpected token: %+v", gotToken)
	}
}

func TestRunFlow_WithPipeListenerStateMismatch(t *testing.T) {
	listener, client := newPipeListener()
	defer func() { _ = client.Close() }()

	done := make(chan error, 1)
	go func() {
		_, err := runFlow(context.Background(), "expected-state", listener, nil, func(_ context.Context, _ string) (*oauth2.Token, error) {
			return &oauth2.Token{}, nil
		})
		done <- err
	}()

	if err := client.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("SetDeadline: %v", err)
	}
	if _, err := io.WriteString(client, "GET /?state=wrong-state&code=pipe-code HTTP/1.1\r\nHost: localhost\r\n\r\n"); err != nil {
		t.Fatalf("WriteString: %v", err)
	}
	_, _ = io.ReadAll(client)

	if err := <-done; err == nil || !strings.Contains(err.Error(), "state mismatch") {
		t.Fatalf("expected state mismatch error, got %v", err)
	}
}

func TestRunFlow_GenerateStateError(t *testing.T) {
	original := stateEntropyReader
	stateEntropyReader = errReader{}
	t.Cleanup(func() { stateEntropyReader = original })

	var buf strings.Builder
	_, err := RunFlow(context.Background(), &oauth2.Config{}, &buf)
	if err == nil {
		t.Fatal("expected error when state generation fails")
	}
}

func TestListenerHost_Nil(t *testing.T) {
	if got := listenerHost(nil); got != "127.0.0.1" {
		t.Fatalf("listenerHost nil: got %q, want %q", got, "127.0.0.1")
	}
}

func TestListenerHost_Fallback(t *testing.T) {
	if got := listenerHost(&net.TCPAddr{}); got != "127.0.0.1" {
		t.Fatalf("listenerHost fallback: got %q, want %q", got, "127.0.0.1")
	}
}

func TestListenerHost_IP(t *testing.T) {
	if got := listenerHost(&net.TCPAddr{IP: net.ParseIP("127.0.0.1")}); got != "127.0.0.1" {
		t.Fatalf("listenerHost IP: got %q, want %q", got, "127.0.0.1")
	}
}

// --- openBrowser smoke test ---

func TestOpenBrowser_NoError(t *testing.T) {
	openBrowser("about:blank") // must not panic
}

func waitForListener(t *testing.T, address string) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", address, 50*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("listener %q was not ready before timeout", address)
}

func mustListenTCP4(t *testing.T) net.Listener {
	t.Helper()

	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		if strings.Contains(err.Error(), "operation not permitted") {
			t.Skipf("local TCP listeners unavailable in this environment: %v", err)
		}
		t.Fatalf("Listen: %v", err)
	}
	return listener
}

func newAuthHTTPServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()

	listener := mustListenTCP4(t)
	server := httptest.NewUnstartedServer(handler)
	server.Listener = listener
	server.Start()
	t.Cleanup(server.Close)
	return server
}

func TestRunFlow_ExportedSuccess(t *testing.T) {
	tokenServer := newAuthHTTPServer(t, func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("ParseForm: %v", err)
		}
		if r.Form.Get("code") != "flow-code" {
			t.Fatalf("code: got %q", r.Form.Get("code"))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"access_token":"flow-token","token_type":"Bearer","refresh_token":"refresh-token","expires_in":3600}`)
	})

	cfg := &oauth2.Config{
		ClientID: "client-id",
		Endpoint: oauth2.Endpoint{
			AuthURL:  "https://accounts.example.test/o/oauth2/auth",
			TokenURL: tokenServer.URL,
		},
		Scopes: []string{gmailScope},
	}

	var buf strings.Builder
	done := make(chan struct{})
	var (
		gotToken *oauth2.Token
		gotErr   error
	)
	go func() {
		gotToken, gotErr = RunFlow(context.Background(), cfg, &buf)
		close(done)
	}()

	deadline := time.Now().Add(2 * time.Second)
	var authURL string
	for time.Now().Before(deadline) {
		output := buf.String()
		if strings.Contains(output, "\n") {
			lines := strings.Split(strings.TrimSpace(output), "\n")
			authURL = lines[len(lines)-1]
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if authURL == "" {
		t.Fatal("expected auth URL in RunFlow output")
	}

	parsed, err := url.Parse(authURL)
	if err != nil {
		t.Fatalf("ParseURL: %v", err)
	}
	redirectURL := parsed.Query().Get("redirect_uri")
	state := parsed.Query().Get("state")
	if redirectURL == "" || state == "" {
		t.Fatalf("expected redirect_uri and state in auth URL: %s", authURL)
	}

	resp, err := http.Get(redirectURL + "?state=" + state + "&code=flow-code") //nolint:noctx
	if err != nil {
		t.Fatalf("GET callback: %v", err)
	}
	_ = resp.Body.Close()

	<-done
	if gotErr != nil {
		t.Fatalf("RunFlow: %v", gotErr)
	}
	if gotToken == nil || gotToken.AccessToken != "flow-token" {
		t.Fatalf("unexpected token: %+v", gotToken)
	}
}
