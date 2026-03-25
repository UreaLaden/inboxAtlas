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

	keyring "github.com/zalando/go-keyring"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
	gmailapi "google.golang.org/api/gmail/v1"
	"google.golang.org/api/option"

	"github.com/UreaLaden/inboxatlas/internal/config"
)

var stateEntropyReader io.Reader = rand.Reader

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

type serviceAccountFile struct {
	Type        string `json:"type"`
	ClientEmail string `json:"client_email"`
	PrivateKey  string `json:"private_key"`
	TokenURI    string `json:"token_uri"`
}

type gmailCredentialsKind string

const (
	gmailCredentialsKindInstalled      gmailCredentialsKind = "desktop_oauth"
	gmailCredentialsKindServiceAccount gmailCredentialsKind = "service_account"
)

// TokenStorage abstracts OAuth token persistence. Callers use this interface;
// FileTokenStorage and KeyringTokenStorage provide the concrete implementations.
type TokenStorage interface {
	Save(provider, email string, token *oauth2.Token) error
	Load(provider, email string) (*oauth2.Token, error)
}

// FileTokenStorage persists tokens as mode-0600 JSON files under TokenDir.
// This is the fallback implementation when the OS keychain is unavailable.
type FileTokenStorage struct {
	TokenDir string
}

// Save serialises token to JSON and writes it to the token file path with mode 0600.
func (f *FileTokenStorage) Save(provider, email string, token *oauth2.Token) error {
	return SaveToken(f.TokenDir, provider, email, token)
}

// Load reads and deserialises the token stored for the given provider and email.
func (f *FileTokenStorage) Load(provider, email string) (*oauth2.Token, error) {
	return LoadToken(f.TokenDir, provider, email)
}

// KeyringTokenStorage persists tokens in the OS native credential store.
// If the keychain is unavailable, Save and Load automatically fall back to
// the provided FileTokenStorage.
type KeyringTokenStorage struct {
	service  string
	fallback *FileTokenStorage
}

// Save marshals token to JSON and stores it in the OS keyring under service/key.
// On any keyring error it falls back to writing via FileTokenStorage.
func (k *KeyringTokenStorage) Save(provider, email string, token *oauth2.Token) error {
	data, err := json.Marshal(token)
	if err != nil {
		return fmt.Errorf("marshal token: %w", err)
	}
	key := keyringKey(provider, email)
	if err := keyring.Set(k.service, key, string(data)); err != nil {
		return k.fallback.Save(provider, email, token)
	}
	return nil
}

// Load retrieves and unmarshals the token from the OS keyring.
// On any keyring error it falls back to reading via FileTokenStorage.
func (k *KeyringTokenStorage) Load(provider, email string) (*oauth2.Token, error) {
	key := keyringKey(provider, email)
	data, err := keyring.Get(k.service, key)
	if err != nil {
		return k.fallback.Load(provider, email)
	}
	var token oauth2.Token
	if err := json.Unmarshal([]byte(data), &token); err != nil {
		return nil, fmt.Errorf("parse keyring token: %w", err)
	}
	return &token, nil
}

// keyringKey returns the keyring user key for a given provider and email.
// Uses the same SHA-256 hash as TokenPath to avoid token values in the key.
func keyringKey(provider, email string) string {
	sum := sha256.Sum256([]byte(strings.ToLower(email)))
	return provider + ":" + hex.EncodeToString(sum[:])
}

// NewTokenStorage constructs the appropriate TokenStorage for cfg.
// If cfg.TokenStorage is "file", returns FileTokenStorage.
// Otherwise (default "keyring"), returns KeyringTokenStorage with
// FileTokenStorage as automatic fallback.
func NewTokenStorage(cfg *config.Config) TokenStorage {
	file := &FileTokenStorage{TokenDir: cfg.TokenDir}
	if cfg.TokenStorage == "file" {
		return file
	}
	return &KeyringTokenStorage{service: "inboxatlas", fallback: file}
}

var gmailProfileFetcher = func(ctx context.Context, src oauth2.TokenSource) (string, error) {
	svc, err := gmailapi.NewService(ctx, option.WithTokenSource(src))
	if err != nil {
		return "", fmt.Errorf("build gmail service: %w", err)
	}
	profile, err := svc.Users.GetProfile("me").Context(ctx).Do()
	if err != nil {
		return "", fmt.Errorf("get gmail profile: %w", err)
	}
	return strings.ToLower(profile.EmailAddress), nil
}

// LoadInstalledAppCredentials reads the Google desktop OAuth credentials JSON
// at path and returns a configured oauth2.Config for the Gmail readonly scope.
func LoadInstalledAppCredentials(path string) (*oauth2.Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read credentials: %w", err)
	}
	return loadInstalledAppCredentialsJSON(data)
}

// LoadServiceAccountJWTConfig reads the Google service-account credentials JSON
// at path and returns a jwt.Config for the Gmail readonly scope.
func LoadServiceAccountJWTConfig(path string) (*jwt.Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read credentials: %w", err)
	}
	return loadServiceAccountJWTConfigJSON(data)
}

// LoadCredentials reads the Google OAuth credentials JSON at path and returns
// a configured oauth2.Config for the Gmail readonly scope.
func LoadCredentials(path string) (*oauth2.Config, error) {
	return LoadInstalledAppCredentials(path)
}

// ResolveGmailTokenSource selects the Gmail auth mode for mailboxID and returns
// a token-source factory for the chosen mode.
func ResolveGmailTokenSource(cfg *config.Config, mailboxID string) (func(context.Context) (oauth2.TokenSource, error), error) {
	data, err := os.ReadFile(cfg.CredentialsPath)
	if err != nil {
		return nil, fmt.Errorf("read credentials: %w", err)
	}
	canonicalMailbox := strings.ToLower(mailboxID)
	kind, err := detectGmailCredentialsKind(data)
	if err != nil {
		return nil, err
	}

	switch kind {
	case gmailCredentialsKindServiceAccount:
		jwtCfg, err := loadServiceAccountJWTConfigJSON(data)
		if err != nil {
			return nil, err
		}
		return func(ctx context.Context) (oauth2.TokenSource, error) {
			delegatedCfg := *jwtCfg
			delegatedCfg.Subject = canonicalMailbox
			return delegatedCfg.TokenSource(ctx), nil
		}, nil
	case gmailCredentialsKindInstalled:
		oauthCfg, err := loadInstalledAppCredentialsJSON(data)
		if err != nil {
			return nil, err
		}
		ts := NewTokenStorage(cfg)
		if _, err := ts.Load("gmail", canonicalMailbox); err != nil {
			return nil, fmt.Errorf("gmail auth: no stored user token for %s — run 'inboxatlas auth gmail --account %s' first: %w", canonicalMailbox, canonicalMailbox, err)
		}
		return func(ctx context.Context) (oauth2.TokenSource, error) {
			token, err := ts.Load("gmail", canonicalMailbox)
			if err != nil {
				return nil, fmt.Errorf("gmail auth: no stored user token for %s — run 'inboxatlas auth gmail --account %s' first: %w", canonicalMailbox, canonicalMailbox, err)
			}
			return oauthCfg.TokenSource(ctx, token), nil
		}, nil
	default:
		return nil, fmt.Errorf("unsupported gmail credentials kind %q", kind)
	}
}

// ValidateGmailDelegation validates that the service-account key at path can
// impersonate mailboxID for Gmail readonly access.
func ValidateGmailDelegation(ctx context.Context, path, mailboxID string) error {
	jwtCfg, err := LoadServiceAccountJWTConfig(path)
	if err != nil {
		return err
	}
	canonicalMailbox := strings.ToLower(mailboxID)
	delegatedCfg := *jwtCfg
	delegatedCfg.Subject = canonicalMailbox
	email, err := gmailProfileFetcher(ctx, delegatedCfg.TokenSource(ctx))
	if err != nil {
		return fmt.Errorf("validate delegated gmail auth for %s: %w", canonicalMailbox, err)
	}
	if email != canonicalMailbox {
		return fmt.Errorf("delegated gmail profile mismatch: got %s, want %s", email, canonicalMailbox)
	}
	return nil
}

func detectGmailCredentialsKind(data []byte) (gmailCredentialsKind, error) {
	var cf credentialsFile
	if err := json.Unmarshal(data, &cf); err != nil {
		return "", fmt.Errorf("parse credentials: %w", err)
	}

	if cf.Installed != nil {
		return gmailCredentialsKindInstalled, nil
	}

	var sa serviceAccountFile
	if err := json.Unmarshal(data, &sa); err != nil {
		return "", fmt.Errorf("parse credentials: %w", err)
	}
	if sa.Type == "service_account" && sa.ClientEmail != "" && sa.PrivateKey != "" && sa.TokenURI != "" {
		return gmailCredentialsKindServiceAccount, nil
	}

	return "", fmt.Errorf("credentials file must be desktop OAuth ('installed') or service account JSON")
}

func loadInstalledAppCredentialsJSON(data []byte) (*oauth2.Config, error) {
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

func loadServiceAccountJWTConfigJSON(data []byte) (*jwt.Config, error) {
	kind, err := detectGmailCredentialsKind(data)
	if err != nil {
		return nil, err
	}
	if kind != gmailCredentialsKindServiceAccount {
		return nil, fmt.Errorf("credentials file is not a service account key")
	}
	jwtCfg, err := google.JWTConfigFromJSON(data, gmailScope)
	if err != nil {
		return nil, fmt.Errorf("parse service account credentials: %w", err)
	}
	jwtCfg.Subject = ""
	return jwtCfg, nil
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
	addr := listener.Addr().(*net.TCPAddr)
	port := addr.Port

	flowCfg := *cfg
	flowCfg.RedirectURL = fmt.Sprintf("http://%s:%d", listenerHost(addr), port)

	state, err := generateState()
	if err != nil {
		_ = listener.Close()
		return nil, err
	}
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
	mux.HandleFunc("/", func(rw http.ResponseWriter, r *http.Request) {
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

// RefreshAndSave loads the stored token via ts for email, obtains a (possibly
// refreshed) token via cfg.TokenSource, and persists the result via ts.
func RefreshAndSave(ctx context.Context, cfg *oauth2.Config, ts TokenStorage, provider, email string) (*oauth2.Token, error) {
	existing, err := ts.Load(provider, email)
	if err != nil {
		return nil, err
	}
	src := cfg.TokenSource(ctx, existing)
	return saveFromSource(src, ts, provider, email)
}

// saveFromSource retrieves a token from src and persists it via ts. It is separated
// from RefreshAndSave for testability.
func saveFromSource(src oauth2.TokenSource, ts TokenStorage, provider, email string) (*oauth2.Token, error) {
	token, err := src.Token()
	if err != nil {
		return nil, fmt.Errorf("refresh token: %w", err)
	}
	if err := ts.Save(provider, email, token); err != nil {
		return nil, err
	}
	return token, nil
}

// generateState returns a random hex string for use as the OAuth state parameter.
func generateState() (string, error) {
	b := make([]byte, 16)
	if _, err := io.ReadFull(stateEntropyReader, b); err != nil {
		return "", fmt.Errorf("generate oauth state: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// openBrowser attempts to open url in the default system browser.
// Errors are silently ignored — the URL is always printed to stdout as fallback.
func openBrowser(url string) {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	case "darwin":
		cmd = exec.Command("open", url)
	default:
		cmd = exec.Command("xdg-open", url)
	}
	_ = cmd.Start()
}

func listenerHost(addr *net.TCPAddr) string {
	if addr == nil || addr.IP == nil || addr.IP.IsUnspecified() {
		return "127.0.0.1"
	}
	host := addr.IP.String()
	if host == "" {
		return "127.0.0.1"
	}
	return host
}
