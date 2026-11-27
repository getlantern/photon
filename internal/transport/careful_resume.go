package transport

import (
	"errors"
	"net/netip"

	"github.com/getlantern/photon/internal/recovery"
)

// CongestionStates keeps saved congestion state for Careful Resume, by
// client. Take removes what it returns, so that one connection at a time
// resumes from a saved state, and Put replaces, so that a client has one.
// Its methods may be called from many connections at once.
type CongestionStates interface {
	Take(key string) (recovery.ResumeState, bool)
	Put(key string, s recovery.ResumeState)
	Delete(key string)
}

type carefulResumeState struct {
	key   string // the client the connection resumed for, or tried to
	taken *recovery.ResumeState
	// admitted is the client's address at admission, for a listener that
	// doesn't track its connections' addresses, as a wildcard one doesn't.
	admitted netip.AddrPort
}

// congestionStateKey keys a client's saved state: its IP address, without
// the port, which a NAT rebinding can change.
func congestionStateKey(address netip.AddrPort) string {
	return address.Addr().Unmap().String()
}

// resumeCongestionState starts Careful Resume on a newly admitted server
// connection whose client has saved state, before it sends anything. Only
// a verified address takes a state: an unverified Initial's source may be
// spoofed, and would use up its victim's.
func (c *Conn) resumeCongestionState(address netip.AddrPort, verified bool) {
	states := c.config.CongestionStates
	if states == nil || !c.config.Server {
		return
	}
	r, ok := c.congestion.(recovery.Resumer)
	if !ok {
		return
	}
	c.carefulResume.key, c.carefulResume.admitted = congestionStateKey(address), address
	if !verified {
		return
	}
	if s, ok := states.Take(c.carefulResume.key); ok {
		r.Resume(s, c.config.CarefulResumeMaxJump)
		c.carefulResume.taken = &s
	}
}

// saveCongestionState runs once as a server connection ends, by Close or
// when it finishes draining. A failed jump drops the state it resumed
// from; then the path's state, if worth it, is saved under its client's
// current address, as a migrated connection's controller measured the new
// path, if the handshake completed, which shows the client could receive
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
	if taken != nil && r.ResumeFailed() {
		states.Delete(key)
		taken = nil
	}
	if !c.saveSnapshot(states, r) && taken != nil {
		states.Put(key, *taken)
	}
}

// saveSnapshot saves the path's state, as saveCongestionState describes, and
// reports whether it did.
func (c *Conn) saveSnapshot(states CongestionStates, r recovery.Resumer) bool {
	if c.multipath != nil && !c.multipath.baseOnly || !c.complete {
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
	if ok {
		states.Put(congestionStateKey(remote), s)
	}
	return ok
}
