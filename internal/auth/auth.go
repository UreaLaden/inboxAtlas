// Package auth handles OAuth flows, token refresh, and secure token persistence.
package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

// gmailScope is the read-only Gmail scope used for all auth flows.
const gmailScope = "https://www.googleapis.com/auth/gmail.readonly"

// credentialsFile mirrors the structure of a Google OAuth credentials JSON file.
type credentialsFile struct {
	Installed *installedApp `json:"installed"`
}

// installedApp holds the OAuth client credentials for a desktop/installed app.
type installedApp struct {
	ClientID     string   `json:"client_id"`
	ClientSecret string   `json:"client_secret"`
	RedirectURIs []string `json:"redirect_uris"`
}

// LoadCredentials reads the Google OAuth credentials JSON at path and returns
// a configured oauth2.Config for the Gmail readonly scope.
func LoadCredentials(path string) (*oauth2.Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read credentials: %w", err)
	}
	var cf credentialsFile
	if err := json.Unmarshal(data, &cf); err != nil {
		return nil, fmt.Errorf("parse credentials: %w", err)
	}
	if cf.Installed == nil {
		return nil, fmt.Errorf("credentials file missing 'installed' key")
	}
	return &oauth2.Config{
		ClientID:     cf.Installed.ClientID,
		ClientSecret: cf.Installed.ClientSecret,
		Endpoint:     google.Endpoint,
		Scopes:       []string{gmailScope},
	}, nil
}

// codeExchanger exchanges an authorization code for an OAuth token.
type codeExchanger func(ctx context.Context, code string) (*oauth2.Token, error)

// RunFlow starts a local HTTP listener, opens the browser to the OAuth consent
// page, waits for the redirect callback, and exchanges the code for a token.
// The auth URL is always printed to w in case the browser does not open.
func RunFlow(ctx context.Context, cfg *oauth2.Config, w io.Writer) (*oauth2.Token, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("start local listener: %w", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port

	flowCfg := *cfg
	flowCfg.RedirectURL = fmt.Sprintf("http://localhost:%d/callback", port)

	state := generateState()
	authURL := flowCfg.AuthCodeURL(state, oauth2.AccessTypeOffline)

	_, _ = fmt.Fprintf(w, "Opening browser for authentication. If it does not open, visit:\n%s\n", authURL)
	openBrowser(authURL)

	return runFlow(ctx, state, listener, w, func(c context.Context, code string) (*oauth2.Token, error) {
		return flowCfg.Exchange(c, code)
	})
}

// runFlow listens for the OAuth redirect on listener, validates state, and
// exchanges the code. It is separated from RunFlow for testability.
func runFlow(ctx context.Context, state string, listener net.Listener, _ io.Writer, exchange codeExchanger) (*oauth2.Token, error) {
	codeCh := make(chan string, 1)
	errCh := make(chan error, 1)

	mux := http.NewServeMux()
	mux.HandleFunc("/callback", func(rw http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("state") != state {
			select {
			case errCh <- fmt.Errorf("OAuth state mismatch — possible CSRF"):
			default:
			}
			http.Error(rw, "state mismatch", http.StatusBadRequest)
			return
		}
		if errParam := q.Get("error"); errParam != "" {
			select {
			case errCh <- fmt.Errorf("OAuth error: %s", errParam):
			default:
			}
			http.Error(rw, errParam, http.StatusBadRequest)
			return
		}
		code := q.Get("code")
		if code == "" {
			select {
			case errCh <- fmt.Errorf("no authorization code in callback"):
			default:
			}
			http.Error(rw, "no code", http.StatusBadRequest)
			return
		}
		_, _ = fmt.Fprintln(rw, "Authentication successful. You can close this tab.")
		codeCh <- code
	})

	srv := &http.Server{Handler: mux}
	go func() {
		if err := srv.Serve(listener); err != nil && err != http.ErrServerClosed {
			select {
			case errCh <- err:
			default:
			}
		}
	}()
	defer func() { _ = srv.Close() }()

	select {
	case code := <-codeCh:
		return exchange(ctx, code)
	case err := <-errCh:
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// TokenPath returns the path where a token for email is stored under tokenDir.
// The filename is the lowercase hex SHA-256 of the canonical email address.
func TokenPath(tokenDir, provider, email string) string {
	sum := sha256.Sum256([]byte(strings.ToLower(email)))
	hash := hex.EncodeToString(sum[:])
	return filepath.Join(tokenDir, provider, hash+".json")
}

// SaveToken serialises token to JSON and writes it to TokenPath with mode 0600.
// The directory is created if it does not exist.
func SaveToken(tokenDir, provider, email string, token *oauth2.Token) error {
	p := TokenPath(tokenDir, provider, email)
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		return fmt.Errorf("create token dir: %w", err)
	}
	data, err := json.Marshal(token)
	if err != nil {
		return fmt.Errorf("marshal token: %w", err)
	}
	if err := os.WriteFile(p, data, 0o600); err != nil {
		return fmt.Errorf("write token: %w", err)
	}
	return nil
}

// LoadToken reads and deserialises the token stored at TokenPath for email.
func LoadToken(tokenDir, provider, email string) (*oauth2.Token, error) {
	p := TokenPath(tokenDir, provider, email)
	data, err := os.ReadFile(p)
	if err != nil {
		return nil, fmt.Errorf("read token: %w", err)
	}
	var token oauth2.Token
	if err := json.Unmarshal(data, &token); err != nil {
		return nil, fmt.Errorf("parse token: %w", err)
	}
	return &token, nil
}

// RefreshAndSave loads the stored token for email, obtains a (possibly
// refreshed) token via cfg.TokenSource, and persists the result.
func RefreshAndSave(ctx context.Context, cfg *oauth2.Config, tokenDir, provider, email string) (*oauth2.Token, error) {
	existing, err := LoadToken(tokenDir, provider, email)
	if err != nil {
		return nil, err
	}
	src := cfg.TokenSource(ctx, existing)
	return saveFromSource(src, tokenDir, provider, email)
}

// saveFromSource retrieves a token from src and persists it. It is separated
// from RefreshAndSave for testability.
func saveFromSource(src oauth2.TokenSource, tokenDir, provider, email string) (*oauth2.Token, error) {
	token, err := src.Token()
	if err != nil {
		return nil, fmt.Errorf("refresh token: %w", err)
	}
	if err := SaveToken(tokenDir, provider, email, token); err != nil {
		return nil, err
	}
	return token, nil
}

// generateState returns a random hex string for use as the OAuth state parameter.
func generateState() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// openBrowser attempts to open url in the default system browser.
// Errors are silently ignored — the URL is always printed to stdout as fallback.
func openBrowser(url string) {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "windows":
		cmd = exec.Command("cmd", "/c", "start", url)
	case "darwin":
		cmd = exec.Command("open", url)
	default:
		cmd = exec.Command("xdg-open", url)
	}
	_ = cmd.Start()
}
