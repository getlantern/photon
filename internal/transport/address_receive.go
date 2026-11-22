package transport

import (
	"crypto/sha256"
	"time"
)

// Storage is optional. Cap both token size and per-connection duplicate history;
// after the history fills, ignore further tokens rather than evicting evidence
// that would allow a retransmission to resurrect a consumed token.
func (c *Conn) receiveAddressToken(data []byte, now time.Time) {
	if c.config.Server || !c.complete || c.config.OnToken == nil || len(data) == 0 || len(data) > 512 {
		return
	}
	hash := sha256.Sum256(data)
	if _, seen := c.receivedTokens[hash]; seen || len(c.receivedTokens) == 64 {
		return
	}
	if c.receivedTokens == nil {
		c.receivedTokens = make(map[[32]byte]struct{})
	}
	c.receivedTokens[hash] = struct{}{}
	c.config.OnToken(clone(data), now)
}
