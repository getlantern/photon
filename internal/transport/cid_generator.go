package transport

import (
	"crypto/rand"
	"errors"
)

// generateLocalCID validates and takes ownership of generator output. Nil uses
// cryptographic randomness. Callers own uniqueness/routing checks separately.
func generateLocalCID(length int, generate func() ([]byte, error)) ([]byte, error) {
	if length < 0 || length > 20 {
		return nil, errors.New("quic: invalid generated CID length")
	}
	if generate != nil {
		cid, err := generate()
		if err != nil {
			return nil, err
		}
		if len(cid) != length {
			return nil, errors.New("quic: generator returned inconsistent CID length")
		}
		return clone(cid), nil
	}
	cid := make([]byte, length)
	_, err := rand.Read(cid)
	return cid, err
}
