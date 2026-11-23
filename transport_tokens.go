package quic

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"time"

	"github.com/getlantern/photon/internal/token"
)

// TokenGeneratorKey is a secret key for server address-validation tokens.
// Generate it with crypto/rand. Servers sharing tokens must use the same key
// and token format. Tokens from other QUIC implementations are not interchangeable.
type TokenGeneratorKey [32]byte

func newTransportTokens(configured *TokenGeneratorKey, maxAge time.Duration) (*token.RetryTokens, *token.AddressTokens, error) {
	if maxAge < 0 {
		return nil, nil, errors.New("quic: MaxTokenAge must not be negative")
	}
	if maxAge == 0 {
		maxAge = 24 * time.Hour
	}
	var master TokenGeneratorKey
	if configured != nil {
		master = *configured
	} else if _, err := rand.Read(master[:]); err != nil {
		return nil, nil, err
	}
	derive := func(label string) [32]byte {
		h := hmac.New(sha256.New, master[:])
		h.Write([]byte("independent-quic/transport-token-key/v1/" + label))
		return [32]byte(h.Sum(nil))
	}
	retries, err := token.NewRetryTokens(derive("retry"), nil, time.Minute)
	if err != nil {
		return nil, nil, err
	}
	addresses, err := token.NewAddressTokens(derive("new-token"), nil, maxAge)
	return retries, addresses, err
}
