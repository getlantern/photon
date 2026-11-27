package transport

import (
	"errors"
	"net/netip"
	"testing"

	"github.com/getlantern/photon/internal/recovery"
)

type testStates struct {
	saved   map[string]recovery.ResumeState
	deleted []string
}

func (s *testStates) Take(key string) (recovery.ResumeState, bool) {
	v, ok := s.saved[key]
	delete(s.saved, key)
	return v, ok
}
func (s *testStates) Put(key string, v recovery.ResumeState) { s.saved[key] = v }
func (s *testStates) Delete(key string)                      { s.deleted = append(s.deleted, key) }

type testResumer struct {
	recovery.Controller
	resumed  *recovery.ResumeState
	snapshot *recovery.ResumeState
	failed   bool
}

func (r *testResumer) Resume(s recovery.ResumeState, _ uint64) { r.resumed = &s }
func (r *testResumer) Snapshot() (recovery.ResumeState, bool) {
	if r.snapshot == nil {
		return recovery.ResumeState{}, false
	}
	return *r.snapshot, true
}
func (r *testResumer) ResumeFailed() bool { return r.failed }

func resumeTestConn(states *testStates, r *testResumer) *Conn {
	c := &Conn{config: Config{Server: true, CongestionStates: states}, congestion: r, complete: true}
	c.publishRemoteAddress(netip.MustParseAddrPort("[::ffff:192.0.2.7]:4433"))
	return c
}

// A verified admission takes its client's state; an unverified one, whose
// source may be spoofed, doesn't.
func TestResumeCongestionStateNeedsVerifiedAddress(t *testing.T) {
	address := netip.MustParseAddrPort("192.0.2.7:4433")
	state := recovery.ResumeState{Bandwidth: 1e6, MinRTT: 1e8}
	for _, verified := range []bool{false, true} {
		states := &testStates{saved: map[string]recovery.ResumeState{"192.0.2.7": state}}
		r := &testResumer{}
		c := resumeTestConn(states, r)
		c.resumeCongestionState(address, verified)
		if took := r.resumed != nil; took != verified || (len(states.saved) == 0) != verified {
			t.Fatalf("verified %v: resumed %v, left %d states", verified, took, len(states.saved))
		}
	}
}

// A connection saves its path's state once, under its client's current
// address, unmapped, and only after a completed handshake and an end not
// caused by a protocol violation it detected. A failed jump drops the state
// it resumed from; a connection that saves nothing otherwise puts back the
// one it took.
func TestSaveCongestionState(t *testing.T) {
	snap := recovery.ResumeState{Bandwidth: 2e6, MinRTT: 1e8}
	taken := recovery.ResumeState{Bandwidth: 3e6, MinRTT: 1e8}
	for _, tc := range []struct {
		name     string
		complete bool
		err      error
		failed   bool
		snapshot bool
		want     *recovery.ResumeState
		deleted  bool
	}{
		{"clean", true, nil, false, true, &snap, false},
		{"peer closed", true, &ConnectionTransportError{Remote: true, ErrorCode: 0xa}, false, true, &snap, false},
		{"no handshake", false, nil, false, true, &taken, false},
		{"optimistic ACK", true, connectionFailure(&TransportError{Code: 0xa, Reason: "ack"}), false, true, &taken, false},
		{"short connection", true, nil, false, false, &taken, false},
		{"failed jump", true, nil, true, true, &snap, true},
		{"failed jump, nothing measured", true, nil, true, false, nil, true},
	} {
		states := &testStates{saved: map[string]recovery.ResumeState{}}
		r := &testResumer{failed: tc.failed}
		if tc.snapshot {
			r.snapshot = &snap
		}
		c := resumeTestConn(states, r)
		c.complete, c.err = tc.complete, tc.err
		c.carefulResume = carefulResumeState{key: "192.0.2.7", taken: &taken}
		c.saveCongestionState()
		c.saveCongestionState()
		got, saved := states.saved["192.0.2.7"]
		if saved != (tc.want != nil) || saved && got != *tc.want || (len(states.deleted) == 1) != tc.deleted || len(states.deleted) > 1 {
			t.Fatalf("%s: saved %v %+v, deleted %v", tc.name, saved, got, states.deleted)
		}
	}
	if !errors.As(connectionFailure(&TransportError{Code: 0xa}), new(*ConnectionTransportError)) {
		t.Fatal("a local transport error isn't a ConnectionTransportError")
	}
}
