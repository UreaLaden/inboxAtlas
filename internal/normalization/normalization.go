// Package normalization standardizes message fields such as sender domain and display name.
package normalization

import (
	"net/mail"
	"strings"

	"github.com/UreaLaden/inboxatlas/pkg/models"
)

// NormalizeMessage returns a normalized copy of msg per spec §8.3.
// The input is not mutated. Labels are preserved verbatim (canonical
// mapping is deferred to Epic 6).
func NormalizeMessage(msg models.MessageMeta) models.MessageMeta {
	name, email := ParseFrom(msg.FromEmail)
	msg.FromName = strings.TrimSpace(name)
	msg.FromEmail = strings.TrimSpace(strings.ToLower(email))
	msg.Domain = ExtractDomain(msg.FromEmail)
	msg.Subject = strings.TrimSpace(msg.Subject)
	msg.Snippet = strings.TrimSpace(msg.Snippet)
	return msg
}

// ParseFrom parses an RFC 5322 From header value into display name and email address.
// On parse failure, the raw string is returned as the address with an empty name.
func ParseFrom(from string) (name, email string) {
	addr, err := mail.ParseAddress(from)
	if err != nil {
		return "", strings.TrimSpace(from)
	}
	return addr.Name, addr.Address
}

// ExtractDomain returns the lowercased domain part of an email address.
// Returns an empty string if the address contains no "@".
func ExtractDomain(email string) string {
	parts := strings.SplitN(email, "@", 2)
	if len(parts) < 2 {
		return ""
	}
	return strings.ToLower(parts[1])
}
