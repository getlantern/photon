package token

import (
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"math"
	"net/netip"
	"time"

	"github.com/getlantern/photon/internal/wire"
	"golang.org/x/crypto/chacha20poly1305"
)

var ErrAddressToken = errors.New("quic: invalid or expired address token")

// AddressTokens protects NEW_TOKEN values for subsequent connections. Tokens
// bind the client IP and QUIC version, but not an ephemeral port or connection
// ID. They are replayable address evidence, not TLS or 0-RTT replay protection.
// Instances are immutable and safe for concurrent use. Replace the instance to
// rotate keys; the optional previous key allows a bounded overlap.
type AddressTokens struct {
	keys     []cipher.AEAD
	lifetime time.Duration
}

func NewAddressTokens(current [32]byte, previous *[32]byte, lifetime time.Duration) (*AddressTokens, error) {
	if lifetime <= 0 {
		return nil, errors.New("quic: address token lifetime must be positive")
	}
	key, err := chacha20poly1305.NewX(current[:])
	if err != nil {
		return nil, err
	}
	t := &AddressTokens{keys: []cipher.AEAD{key}, lifetime: lifetime}
	if previous != nil {
		old, err := chacha20poly1305.NewX(previous[:])
		if err != nil {
			return nil, err
		}
		t.keys = append(t.keys, old)
	}
	return t, nil
}

const legacyAddressTokenSize = 2 + chacha20poly1305.NonceSizeX + 12 + chacha20poly1305.Overhead

// Reserved authenticated bytes keep the new format outside Retry's 58–70 byte
// envelope range, preserving classification even with damaged format bytes.
const addressTokenSize = 2 + chacha20poly1305.NonceSizeX + 32 + chacha20poly1305.Overhead

func addressContext(address netip.AddrPort, version wire.Version) ([]byte, error) {
	if !address.IsValid() || address.Port() == 0 || !version.Supported() {
		return nil, ErrAddressToken
	}
	ip := address.Addr().Unmap().String()
	if len(ip) > 256 {
		return nil, ErrAddressToken
	}
	aad := []byte("independent-quic/new-token/v1/")
	aad = binary.BigEndian.AppendUint32(aad, uint32(version))
	return append(aad, ip...), nil
}

func (t *AddressTokens) Issue(address netip.AddrPort, version wire.Version, now time.Time) ([]byte, error) {
	aad, err := addressContext(address, version)
	if err != nil || now.Unix() < 0 {
		return nil, ErrAddressToken
	}
	out := make([]byte, 2+chacha20poly1305.NonceSizeX, addressTokenSize)
	out[0], out[1] = 0x4e, 2
	nonce := out[2:]
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	plain := binary.BigEndian.AppendUint64(nil, uint64(now.Unix()))
	plain = binary.BigEndian.AppendUint32(plain, uint32(now.Nanosecond()))
	plain = binary.BigEndian.AppendUint64(plain, uint64(t.lifetime))
	plain = append(plain, make([]byte, 12)...)
	return t.keys[0].Seal(out, nonce, plain, aad), nil
}

func (t *AddressTokens) Validate(token []byte, address netip.AddrPort, version wire.Version, now time.Time) error {
	aad, err := addressContext(address, version)
	if err != nil || now.Unix() < 0 || !IsAddressToken(token) || token[0] != 0x4e ||
		(len(token) == legacyAddressTokenSize && token[1] != 1) ||
		(len(token) == addressTokenSize && token[1] != 2) {
		return ErrAddressToken
	}
	for _, key := range t.keys {
		plain, err := key.Open(nil, token[2:2+chacha20poly1305.NonceSizeX], token[2+chacha20poly1305.NonceSizeX:], aad)
		if err != nil {
			continue
		}
		seconds := binary.BigEndian.Uint64(plain[:8])
		if seconds > math.MaxInt64 {
			return ErrAddressToken
		}
		var nanos uint32
		var lifetime time.Duration
		if token[1] == 1 {
			lifetime = time.Duration(binary.BigEndian.Uint32(plain[8:])) * time.Second
		} else {
			nanos = binary.BigEndian.Uint32(plain[8:12])
			encodedLifetime := binary.BigEndian.Uint64(plain[12:20])
			if nanos >= 1e9 || encodedLifetime > math.MaxInt64 {
				return ErrAddressToken
			}
			lifetime = time.Duration(encodedLifetime)
			for _, b := range plain[20:] {
				if b != 0 {
					return ErrAddressToken
				}
			}
		}
		issued := time.Unix(int64(seconds), int64(nanos))
		if lifetime <= 0 || now.Before(issued) || now.Sub(issued) >= min(lifetime, t.lifetime) {
			return ErrAddressToken
		}
		return nil
	}
	return ErrAddressToken
}

// IsAddressToken classifies the fixed-size address-token envelope, not its
// validity. Retry tokens have a disjoint size range (58–70 bytes). Recognize
// even damaged format bytes here so admission can issue Retry for invalid
// address evidence. Validate still checks the format and authenticates it.
func IsAddressToken(b []byte) bool {
	return len(b) == addressTokenSize || len(b) == legacyAddressTokenSize
}
