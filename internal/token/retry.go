// Package token protects server address-validation tokens independently of TLS.
package token

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"net/netip"
	"strconv"
	"time"

	"crypto/cipher"
	"golang.org/x/crypto/chacha20poly1305"

	"github.com/getlantern/photon/internal/wire"
)

var ErrRetryToken = errors.New("quic: invalid or expired Retry token")

// ErrReadableRetryToken identifies authenticated Retry evidence whose contents
// fail validation. Unreadable tokens must not be assumed to originate in Retry.
var ErrReadableRetryToken = fmt.Errorf("%w: authenticated contents rejected", ErrRetryToken)

// RetryTokens is immutable and safe for concurrent use. The first key issues
// tokens; an optional previous key permits rotation without invalidating recent
// handshakes. The owner replaces this object to rotate and eventually retire
// the previous key. Keys must be independently generated secret random bytes.
// Tokens are replayable within their lifetime, but only for their bound address
// and Retry CID. They are not NEW_TOKEN or 0-RTT replay protection.
type RetryTokens struct {
	keys     []cipher.AEAD
	lifetime time.Duration
}

func NewRetryTokens(current [32]byte, previous *[32]byte, lifetime time.Duration) (*RetryTokens, error) {
	if lifetime < time.Second || lifetime > 10*time.Minute || lifetime%time.Second != 0 {
		return nil, errors.New("quic: Retry token lifetime must be whole seconds from 1 to 600")
	}
	aead, err := chacha20poly1305.NewX(current[:])
	if err != nil {
		return nil, err
	}
	t := &RetryTokens{keys: []cipher.AEAD{aead}, lifetime: lifetime}
	if previous != nil {
		old, err := chacha20poly1305.NewX(previous[:])
		if err != nil {
			return nil, err
		}
		t.keys = append(t.keys, old)
	}
	return t, nil
}

func retryContext(address netip.AddrPort, v wire.Version, retryCID []byte) ([]byte, error) {
	if !address.IsValid() || address.Port() == 0 || !v.Supported() || len(retryCID) > 20 {
		return nil, ErrRetryToken
	}
	// Treat IPv4 and IPv4-mapped IPv6 socket representations identically.
	address = netip.AddrPortFrom(address.Addr().Unmap(), address.Port())
	text := address.String()
	if len(text) > 256 {
		return nil, ErrRetryToken
	}
	b := []byte("independent-quic/retry-token/v1/")
	b = binary.BigEndian.AppendUint32(b, uint32(v))
	b = append(b, byte(len(retryCID)))
	b = append(b, retryCID...)
	b = append(b, []byte(strconv.Itoa(len(text))+":"+text)...)
	return b, nil
}

// Issue binds the token to the complete client address (including port), QUIC
// version and Retry source CID. OriginalDCID is recovered only after validation.
func (t *RetryTokens) Issue(address netip.AddrPort, v wire.Version, originalDCID, retryCID []byte, now time.Time) ([]byte, error) {
	context, err := retryContext(address, v, retryCID)
	if err != nil || len(originalDCID) < 8 || len(originalDCID) > 20 || bytes.Equal(originalDCID, retryCID) || now.Unix() < 0 {
		return nil, ErrRetryToken
	}
	plain := binary.BigEndian.AppendUint64(nil, uint64(now.Unix()))
	plain = binary.BigEndian.AppendUint16(plain, uint16(t.lifetime/time.Second))
	plain = append(plain, originalDCID...)
	// XChaCha's 192-bit random nonce avoids a global per-server nonce counter.
	nonce := make([]byte, chacha20poly1305.NonceSizeX)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return t.keys[0].Seal(nonce, nonce, plain, context), nil
}

// Validate returns an owned original destination CID. Callers must use the
// received Initial's destination CID as retryCID, not a value from the token.
func (t *RetryTokens) Validate(token []byte, address netip.AddrPort, v wire.Version, retryCID []byte, now time.Time) ([]byte, error) {
	context, err := retryContext(address, v, retryCID)
	const overhead = chacha20poly1305.NonceSizeX + chacha20poly1305.Overhead + 10
	if err != nil || len(token) < overhead+8 || len(token) > overhead+20 || now.Unix() < 0 {
		return nil, ErrRetryToken
	}
	for _, key := range t.keys {
		plain, err := key.Open(nil, token[:chacha20poly1305.NonceSizeX], token[chacha20poly1305.NonceSizeX:], context)
		if err != nil {
			continue
		}
		issued := binary.BigEndian.Uint64(plain[:8])
		current := uint64(now.Unix())
		if issued > current || current-issued >= min(uint64(binary.BigEndian.Uint16(plain[8:10])), uint64(t.lifetime/time.Second)) || bytes.Equal(plain[10:], retryCID) {
			return nil, ErrReadableRetryToken
		}
		return plain[10:], nil
	}
	return nil, ErrRetryToken
}
