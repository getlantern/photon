// Package packet implements QUIC packet protection (RFC 9001 and RFC 9369).
// Connection state owns packet numbers, replay detection, key retirement,
// confidentiality/integrity usage limits, and key update coordination.
package packet

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash"

	"github.com/getlantern/photon/internal/wire"
	"golang.org/x/crypto/chacha20"
	"golang.org/x/crypto/chacha20poly1305"
)

var (
	ErrPacket         = errors.New("quic: invalid protected packet")
	ErrReservedBits   = errors.New("quic: authenticated packet has nonzero reserved bits")
	ErrAuthentication = errors.New("quic: packet authentication failed")
	ErrCipherSuite    = errors.New("quic: unsupported TLS cipher suite")
)

// Keys are immutable and safe for concurrent use. A caller MUST NOT seal two
// packets with the same packet number, path ID, and keys. Seal uses path ID zero.
// Path IDs must never be reused for another path in the same connection.
type Keys struct {
	aead     cipher.AEAD
	iv       [12]byte
	hp       cipher.Block
	chachaHP []byte
	secret   []byte
	suite    uint16
	version  wire.Version
}

func unhex(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	} // Constants only.
	return b
}

func expandLabel(h func() hash.Hash, secret []byte, label string, n int) ([]byte, error) {
	label = "tls13 " + label
	info := binary.BigEndian.AppendUint16(nil, uint16(n))
	info = append(info, byte(len(label)))
	info = append(info, label...)
	info = append(info, 0) // Empty context.
	return hkdf.Expand(h, secret, string(info), n)
}

func suiteParams(suite uint16) (func() hash.Hash, int, error) {
	switch suite {
	case tls.TLS_AES_128_GCM_SHA256:
		return sha256.New, 16, nil
	case tls.TLS_AES_256_GCM_SHA384:
		return sha512.New384, 32, nil
	case tls.TLS_CHACHA20_POLY1305_SHA256:
		return sha256.New, 32, nil
	default:
		return nil, 0, ErrCipherSuite
	}
}

func prefix(v wire.Version) string {
	if v == wire.Version2 {
		return "quicv2 "
	}
	return "quic "
}

// InitialKeys derives client-write and server-write keys from the destination
// connection ID of the client's Initial, including after a Retry.
func InitialKeys(v wire.Version, dcid []byte) (client, server *Keys, err error) {
	if !v.Supported() {
		return nil, nil, wire.ErrVersion
	}
	if len(dcid) > 20 {
		return nil, nil, ErrPacket
	}
	salt := "38762cf7f55934b34d179ae6a4c80cadccbb7f0a"
	if v == wire.Version2 {
		salt = "0dede3def700a6db819381be6e269dcbf9bd2ed9"
	}
	secret, err := hkdf.Extract(sha256.New, dcid, unhex(salt))
	if err != nil {
		return nil, nil, err
	}
	c, err := expandLabel(sha256.New, secret, "client in", 32)
	if err != nil {
		return nil, nil, err
	}
	s, err := expandLabel(sha256.New, secret, "server in", 32)
	if err != nil {
		return nil, nil, err
	}
	client, err = TrafficKeys(v, tls.TLS_AES_128_GCM_SHA256, c)
	if err != nil {
		return nil, nil, err
	}
	server, err = TrafficKeys(v, tls.TLS_AES_128_GCM_SHA256, s)
	return client, server, err
}

// TrafficKeys derives protection keys from a TLS traffic secret, not a TLS key.
func TrafficKeys(v wire.Version, suite uint16, secret []byte) (*Keys, error) {
	if !v.Supported() {
		return nil, wire.ErrVersion
	}
	h, size, err := suiteParams(suite)
	if err != nil {
		return nil, err
	}
	if len(secret) != h().Size() {
		return nil, errors.New("quic: invalid traffic secret length")
	}
	key, err := expandLabel(h, secret, prefix(v)+"key", size)
	if err != nil {
		return nil, err
	}
	iv, err := expandLabel(h, secret, prefix(v)+"iv", 12)
	if err != nil {
		return nil, err
	}
	hp, err := expandLabel(h, secret, prefix(v)+"hp", size)
	if err != nil {
		return nil, err
	}
	k := &Keys{secret: append([]byte(nil), secret...), suite: suite, version: v}
	copy(k.iv[:], iv)
	if suite == tls.TLS_CHACHA20_POLY1305_SHA256 {
		k.aead, err = chacha20poly1305.New(key)
		k.chachaHP = hp
	} else {
		var block cipher.Block
		block, err = aes.NewCipher(key)
		if err != nil {
			return nil, err
		}
		k.aead, err = cipher.NewGCM(block)
		if err != nil {
			return nil, err
		}
		k.hp, err = aes.NewCipher(hp)
	}
	return k, err
}

// Next derives the next application traffic keys. Header protection does not
// change during key updates (RFC 9001 §6). The connection decides when an update
// is permitted and how long old keys remain available.
func (k *Keys) Next() (*Keys, error) {
	h, _, err := suiteParams(k.suite)
	if err != nil {
		return nil, err
	}
	secret, err := expandLabel(h, k.secret, prefix(k.version)+"ku", h().Size())
	if err != nil {
		return nil, err
	}
	next, err := TrafficKeys(k.version, k.suite, secret)
	if err != nil {
		return nil, err
	}
	next.hp, next.chachaHP = k.hp, k.chachaHP
	return next, nil
}

func (k *Keys) nonce(pn uint64) [12]byte {
	return k.nonceForPath(0, pn)
}

// nonceForPath implements RFC 10069 section 2.4.
func (k *Keys) nonceForPath(pathID uint32, pn uint64) [12]byte {
	n := k.iv
	binary.BigEndian.PutUint32(n[:4], binary.BigEndian.Uint32(n[:4])^pathID)
	x := binary.BigEndian.Uint64(n[4:]) ^ pn
	binary.BigEndian.PutUint64(n[4:], x)
	return n
}

// Both interface calls retain their slice arguments according to escape
// analysis. Keep per-packet mask and nonce scratch in one allocation instead
// of two; never put mutable scratch on the concurrently usable Keys object.
type protectionScratch struct {
	mask  [16]byte
	nonce [12]byte
}

func (k *Keys) mask(sample []byte) [16]byte {
	var mask [16]byte
	k.maskInto(&mask, sample)
	return mask
}

func (k *Keys) maskInto(mask *[16]byte, sample []byte) {
	if k.hp != nil {
		k.hp.Encrypt(mask[:], sample[:16])
	} else {
		c, _ := chacha20.NewUnauthenticatedCipher(k.chachaHP, sample[4:16])
		c.SetCounter(binary.LittleEndian.Uint32(sample[:4]))
		c.XORKeyStream(mask[:5], mask[:5])
	}
}

// Seal returns a new protected packet. header includes the unprotected truncated
// packet number; pn is the full number. The caller supplies PADDING if needed to
// make a header protection sample available and to meet datagram size rules.
func (k *Keys) Seal(header, payload []byte, pn uint64) ([]byte, error) {
	return k.seal(nil, header, payload, pn, 0)
}

// SealPath protects a multipath 1-RTT packet. The caller must have negotiated
// RFC 10069 multipath and assigned this path ID to the destination CID.
// Initial, Handshake, and 0-RTT packets must use Seal instead. Packet numbers
// must be unique within each path; key updates and usage limits span all paths.
func (k *Keys) SealPath(header, payload []byte, pn uint64, pathID uint32) ([]byte, error) {
	if len(header) == 0 || header[0]&0x80 != 0 {
		return nil, ErrPacket
	}
	return k.seal(nil, header, payload, pn, pathID)
}

// SealInto uses caller-owned output storage when capacity permits. dst must
// not overlap header or payload. Output is valid until dst is reused. Like
// Seal, the caller must ensure packet-number uniqueness even with reused storage.
func (k *Keys) SealInto(dst, header, payload []byte, pn uint64) ([]byte, error) {
	return k.seal(dst, header, payload, pn, 0)
}

// SealPathInto is SealInto with the same path binding requirements as SealPath.
func (k *Keys) SealPathInto(dst, header, payload []byte, pn uint64, pathID uint32) ([]byte, error) {
	if len(header) == 0 || header[0]&0x80 != 0 {
		return nil, ErrPacket
	}
	return k.seal(dst, header, payload, pn, pathID)
}

func (k *Keys) seal(dst, header, payload []byte, pn uint64, pathID uint32) ([]byte, error) {
	if len(header) == 0 || pn > wire.MaxVarint {
		return nil, ErrPacket
	}
	n := int(header[0]&3) + 1
	offset := len(header) - n
	if offset < 1 || len(payload)+n+k.aead.Overhead() < 20 {
		return nil, ErrPacket
	}
	var truncated uint64
	for _, b := range header[offset:] {
		truncated = truncated<<8 | uint64(b)
	}
	if truncated != pn&((uint64(1)<<(8*n))-1) {
		return nil, ErrPacket
	}
	if header[0]&0x80 != 0 {
		if header[0]&0x0c != 0 {
			return nil, ErrReservedBits
		}
	} else if header[0]&0x18 != 0 {
		return nil, ErrReservedBits
	}
	scratch := protectionScratch{nonce: k.nonceForPath(pathID, pn)}
	out := dst
	if cap(out) < len(header)+len(payload)+k.aead.Overhead() {
		out = make([]byte, len(header), len(header)+len(payload)+k.aead.Overhead())
	} else {
		out = out[:len(header)]
	}
	copy(out, header)
	out = k.aead.Seal(out, scratch.nonce[:], payload, header)
	k.maskInto(&scratch.mask, out[offset+4:])
	bits := byte(0x1f)
	if out[0]&0x80 != 0 {
		bits = 0x0f
	}
	out[0] ^= scratch.mask[0] & bits
	for i := 0; i < n; i++ {
		out[offset+i] ^= scratch.mask[1+i]
	}
	return out, nil
}

// Open authenticates one packet without mutating input. pnOffset must come from
// the invariant header parser; packet must exclude subsequent coalesced packets.
// expected is the next expected packet number in the matching number space.
// Header and payload own disjoint, capacity-limited regions of one allocation.
// Reserved bits are checked only after authentication succeeds.
func (k *Keys) Open(packet []byte, pnOffset int, expected uint64) (header, payload []byte, pn uint64, err error) {
	return k.open(nil, packet, pnOffset, expected, 0)
}

// OpenPath authenticates a negotiated multipath 1-RTT packet. pathID comes
// from the destination CID's path binding; expected belongs to that path's
// application packet-number space. The header protection key is shared.
func (k *Keys) OpenPath(packet []byte, pnOffset int, expected uint64, pathID uint32) (header, payload []byte, pn uint64, err error) {
	if len(packet) == 0 || packet[0]&0x80 != 0 {
		return nil, nil, 0, ErrPacket
	}
	return k.open(nil, packet, pnOffset, expected, pathID)
}

// OpenInto authenticates using caller-owned decode storage when capacity permits.
// dst must not overlap packet. Returned views remain valid only until dst is
// reused; header capacity is limited to protect its payload sibling. On error,
// neither view is returned, but dst contents are unspecified. Input is immutable.
func (k *Keys) OpenInto(dst, packet []byte, pnOffset int, expected uint64) (header, payload []byte, pn uint64, err error) {
	return k.open(dst, packet, pnOffset, expected, 0)
}

func (k *Keys) open(dst, packet []byte, pnOffset int, expected uint64, pathID uint32) (header, payload []byte, pn uint64, err error) {
	if pnOffset < 1 || pnOffset > len(packet) || len(packet)-pnOffset < 20 {
		return nil, nil, 0, ErrPacket
	}
	var scratch protectionScratch
	k.maskInto(&scratch.mask, packet[pnOffset+4:])
	bits := byte(0x1f)
	if packet[0]&0x80 != 0 {
		bits = 0x0f
	}
	first := packet[0] ^ (scratch.mask[0] & bits)
	n := int(first&3) + 1
	// Header and plaintext own disjoint regions of one allocation. Limit the
	// header capacity so a caller appending to it cannot overwrite plaintext.
	headerLen := pnOffset + n
	if len(packet)-headerLen < k.aead.Overhead() {
		return nil, nil, 0, ErrPacket
	}
	decoded := dst
	decodedLen := len(packet) - k.aead.Overhead()
	if cap(decoded) < decodedLen {
		decoded = make([]byte, decodedLen)
	} else {
		decoded = decoded[:decodedLen]
	}
	header = decoded[:headerLen:headerLen]
	copy(header, packet[:headerLen])
	header[0] = first
	var truncated uint64
	for i := 0; i < n; i++ {
		header[pnOffset+i] ^= scratch.mask[1+i]
		truncated = truncated<<8 | uint64(header[pnOffset+i])
	}
	pn, err = wire.DecodePacketNumber(truncated, n, expected)
	if err != nil {
		return nil, nil, 0, err
	}
	scratch.nonce = k.nonceForPath(pathID, pn)
	payload, err = k.aead.Open(decoded[headerLen:headerLen], scratch.nonce[:], packet[headerLen:], header)
	if err != nil {
		return nil, nil, 0, ErrAuthentication
	}
	reserved := byte(0x18)
	if first&0x80 != 0 {
		reserved = 0x0c
	}
	if first&reserved != 0 {
		return nil, nil, 0, ErrReservedBits
	}
	return header, payload, pn, nil
}

// UsageLimits returns RFC 9001 section 6.6 packet bounds. Confidentiality is
// per write key; integrity is counted across the connection, not per read key.
func (k *Keys) UsageLimits() (confidentiality, integrity uint64) {
	if k.suite == tls.TLS_CHACHA20_POLY1305_SHA256 {
		return 1 << 62, 1 << 36
	}
	return 1 << 23, 1 << 52
}
