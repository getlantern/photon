package transport

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/netip"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/token"
)

// CongestionStates keeps saved congestion state for Careful Resume, by
// client. Take removes what it returns, so that one connection at a time
// resumes from a saved state, and Put replaces a state measured no later,
// so that a client has one, the latest. Its methods may be called from many
// connections at once.
type CongestionStates interface {
	Take(key string) (recovery.ResumeState, bool)
	Put(key string, s recovery.ResumeState)
}

type carefulResumeState struct {
	key   string // the key the connection took its state by, or tried to
	taken *recovery.ResumeState
	// admitted is the client's address at admission, for a listener that
	// doesn't track its connections' addresses, as a wildcard one doesn't.
	admitted netip.AddrPort
	// id is the client's resume ID, from the address token it presented
	// (fromToken) or new, which the connection's own tokens carry.
	id        token.ResumeID
	fromToken bool
	enabled   bool
}

// congestionStateKey keys a client's saved state by its IP address,
// without the port, which a NAT rebinding can change.
func congestionStateKey(address netip.AddrPort) string {
	return address.Addr().Unmap().String()
}

// resumeIDKey keys a client's saved state by the resume ID in its address
// tokens, apart from other clients behind its address.
func resumeIDKey(id token.ResumeID) string {
	return "id:" + hex.EncodeToString(id[:])
}

// resumeCongestionState starts Careful Resume on a newly admitted server
// connection whose client has saved state, before it sends anything. Only
// a verified address takes a state: an unverified Initial's source may be
// spoofed, and would use up its victim's. A client whose address token
// carries a resume ID takes the state saved under it, its own, even behind
// an address other clients share, or, when nothing is saved under the ID,
// as on its first redemption or after expiry or eviction, its address's;
// others take their address's, and get a resume ID in this connection's
// tokens.
func (c *Conn) resumeCongestionState(address netip.AddrPort, verified bool, id token.ResumeID, fromToken bool) {
	states := c.config.CongestionStates
	if states == nil || !c.config.Server {
		return
	}
	r, ok := c.congestion.(recovery.Resumer)
	if !ok {
		return
	}
	c.carefulResume.admitted, c.carefulResume.enabled = address, true
	c.carefulResume.id, c.carefulResume.fromToken = id, fromToken
	c.carefulResume.key = congestionStateKey(address)
	if !fromToken {
		if _, err := rand.Read(c.carefulResume.id[:]); err != nil {
			c.carefulResume.enabled = false
		}
	}
	if !verified {
		return
	}
	keys := []string{c.carefulResume.key}
	if fromToken {
		keys = []string{resumeIDKey(id), c.carefulResume.key}
	}
	for _, key := range keys {
		if s, ok := states.Take(key); ok {
			r.Resume(s, c.config.CarefulResumeMaxJump)
			c.carefulResume.key, c.carefulResume.taken = key, &s
			return
		}
	}
	c.carefulResume.key = keys[0]
}

// saveCongestionState runs once as a server connection ends, by Close or
// when it finishes draining. A failed jump doesn't put back the state it
// resumed from; then the path's state, if worth it, is saved under the
// resume ID its client presented, or, for a client that came without one,
// under its current address, as a migrated connection's controller
// measured the new path. A client gets an entry of its own only once it
// comes back with its token: one that doesn't keep them can't fill the
// cache with entries for IDs never used. A state is saved if the handshake
// completed, which shows the client could receive
// there, and the connection didn't fail on a protocol violation it
// detected, such as an ACK of a packet never sent: a client inflating its
// measured bandwidth so must not set a jump for others behind its address.
// A connection that saves nothing, too short to measure the path, say,
// puts back the state it took, unless its jump failed, so that a short
// connection doesn't use it up; its saved time stays, so this doesn't
// extend its lifetime. Connections that negotiated multipath save nothing;
// a single-path connection whose path an address-aware listener owns does.
func (c *Conn) saveCongestionState() {
	states := c.config.CongestionStates
	if states == nil || !c.config.Server || c.carefulResume.key == "" {
		return
	}
	key := c.carefulResume.key
	c.carefulResume.key = ""
	r, ok := c.congestion.(recovery.Resumer)
	if !ok {
		return
	}
	taken := c.carefulResume.taken
	c.carefulResume.taken = nil
	if r.ResumeFailed() {
		// The state it took is gone with it: Take removed it, and it isn't
		// put back. Another connection's, saved since, stays.
		taken = nil
	}
	if !c.saveSnapshot(states, r) && taken != nil {
		states.Put(key, *taken)
	}
}

// saveSnapshot saves the path's state, as saveCongestionState describes, and
// reports whether it did.
func (c *Conn) saveSnapshot(states CongestionStates, r recovery.Resumer) bool {
	if c.multipathNegotiated() || !c.complete {
		return false
	}
	var failure *ConnectionTransportError
	if errors.As(c.err, &failure) && !failure.Remote && failure.ErrorCode != 0 {
		return false
	}
	remote := c.CurrentRemoteAddr()
	if !remote.IsValid() {
		remote = c.carefulResume.admitted
	}
	if !remote.IsValid() {
		return false
	}
	s, ok := r.Snapshot()
	if !ok {
		return false
	}
	if c.carefulResume.fromToken {
		states.Put(resumeIDKey(c.carefulResume.id), s)
	} else {
		states.Put(congestionStateKey(remote), s)
	}
	return true
}
