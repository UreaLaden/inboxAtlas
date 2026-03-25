package auth

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
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

// --- LoadCredentials ---

func TestLoadCredentials_Valid(t *testing.T) {
	dir := t.TempDir()
	creds := `{"installed":{"client_id":"test-id","client_secret":"test-secret","redirect_uris":["http://localhost"]}}`
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
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
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
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
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
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
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
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
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
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
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
