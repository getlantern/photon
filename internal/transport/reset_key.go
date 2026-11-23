package transport

import (
	"crypto/hmac"
	"crypto/sha256"
)

// ResetKey derives reproducible stateless-reset tokens from locally issued CIDs.
// The owner must keep the key secret and never reuse a CID with the same key.
// A key does not by itself authorize reset transmission for a routed connection.
type ResetKey [32]byte

func (key *ResetKey) token(cid []byte) [16]byte {
	h := hmac.New(sha256.New, key[:])
	h.Write(cid)
	return [16]byte(h.Sum(nil)[:16])
}
