package transport

import (
	"crypto/rand"
	"net/netip"
	"time"
)

// resetUnknown is called only after CID and peer reset-token routing failed.
// Responses shrink strictly to prevent loops. A bounded endpoint-wide rate
// limit avoids retaining attacker-selected addresses or unbounded history.
func (e *ServerEndpoint) resetUnknown(data []byte, local, peer netip.AddrPort, now time.Time) (*Outbound, error) {
	if e.ResetKey == nil || e.router.cidLength == 0 || len(data) <= 21 || len(data) < 1+e.router.cidLength || data[0]&0xc0 != 0x40 || !peer.IsValid() || peer.Port() == 0 {
		return nil, nil
	}
	// A CID belonging to a live connection must never reveal its token, even if
	// address policy rejected the packet before normal connection processing.
	cid := data[1 : 1+e.router.cidLength]
	if _, known := e.router.ids[string(cid)]; known {
		return nil, nil
	}
	if !e.resetWindow.IsZero() && now.Before(e.resetWindow.Add(time.Second)) {
		if e.resetCount >= 100 {
			return nil, nil
		}
	} else {
		e.resetWindow = now
		e.resetCount = 0
	}
	size := min(42, len(data)-1)
	out := make([]byte, size)
	if _, err := rand.Read(out[:size-16]); err != nil {
		return nil, err
	}
	out[0] = (out[0] & 0x3f) | 0x40
	token := e.ResetKey.token(cid)
	copy(out[size-16:], token[:])
	e.resetCount++
	return &Outbound{Local: local, Peer: peer, Data: out}, nil
}
