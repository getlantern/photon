package packet

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/subtle"
	"encoding/binary"

	"github.com/getlantern/photon/internal/wire"
)

// BuildRetry encodes a server Retry packet with its version-specific integrity
// tag. The caller supplies an opaque, address-bound token and enforces the UDP
// path's size and amplification limits. The public integrity tag does not make
// an unauthenticated token trustworthy.
func BuildRetry(v wire.Version, originalDCID, clientSCID, retrySCID, token []byte) ([]byte, error) {
	if len(originalDCID) < 8 || len(originalDCID) > 20 || len(clientSCID) > 20 || len(retrySCID) > 20 || len(token) == 0 || bytes.Equal(originalDCID, retrySCID) {
		return nil, ErrPacket
	}
	bits, err := v.TypeBits(wire.Retry)
	if err != nil {
		return nil, err
	}
	if 7+len(clientSCID)+len(retrySCID)+len(token)+16 > 65527 {
		return nil, ErrPacket
	}
	b := binary.BigEndian.AppendUint32([]byte{0xc0 | bits<<4}, uint32(v))
	b = append(b, byte(len(clientSCID)))
	b = append(b, clientSCID...)
	b = append(b, byte(len(retrySCID)))
	b = append(b, retrySCID...)
	b = append(b, token...)
	tag, err := RetryTag(v, originalDCID, b)
	if err != nil {
		return nil, err
	}
	return append(b, tag[:]...), nil
}

// RetryTag computes the integrity tag over a Retry packet excluding its tag.
// This publicly keyed tag binds the Retry to the original destination CID;
// it does not authenticate the server's identity.
func RetryTag(v wire.Version, originalDCID, retry []byte) ([16]byte, error) {
	var tag [16]byte
	if !v.Supported() {
		return tag, wire.ErrVersion
	}
	if len(originalDCID) > 20 {
		return tag, ErrPacket
	}
	key, nonce := "be0c690b9f66575a1d766b54e368c84e", "461599d35d632bf2239825bb"
	if v == wire.Version2 {
		key, nonce = "8fb4b01b56ac48e260fbcbcead7ccc92", "d86969bc2d7c6d9990efb04a"
	}
	block, err := aes.NewCipher(unhex(key))
	if err != nil {
		return tag, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return tag, err
	}
	pseudo := make([]byte, 0, 1+len(originalDCID)+len(retry))
	pseudo = append(pseudo, byte(len(originalDCID)))
	pseudo = append(pseudo, originalDCID...)
	pseudo = append(pseudo, retry...)
	copy(tag[:], aead.Seal(nil, unhex(nonce), nil, pseudo))
	return tag, nil
}

func VerifyRetry(v wire.Version, originalDCID, retry []byte) bool {
	if len(retry) < 16 {
		return false
	}
	tag, err := RetryTag(v, originalDCID, retry[:len(retry)-16])
	return err == nil && subtle.ConstantTimeCompare(tag[:], retry[len(retry)-16:]) == 1
}
