package transport

import (
	"errors"
	"net/netip"
	"testing"

	"github.com/getlantern/photon/internal/recovery"
)

type testStates struct {
	saved map[string]recovery.ResumeState
}

func (s *testStates) Take(key string) (recovery.ResumeState, bool) {
	v, ok := s.saved[key]
	delete(s.saved, key)
	return v, ok
}
func (s *testStates) Put(key string, v recovery.ResumeState) { s.saved[key] = v }

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
// caused by a protocol violation it detected. A failed jump doesn't put
// back the state it resumed from, nor touch one another connection saved
// since; a connection that saves nothing otherwise puts back the one it
// took.
func TestSaveCongestionState(t *testing.T) {
	snap := recovery.ResumeState{Bandwidth: 2e6, MinRTT: 1e8}
	taken := recovery.ResumeState{Bandwidth: 3e6, MinRTT: 1e8}
	other := recovery.ResumeState{Bandwidth: 4e6, MinRTT: 1e8}
	for _, tc := range []struct {
		name     string
		complete bool
		err      error
		failed   bool
		snapshot bool
		since    *recovery.ResumeState // saved by another connection meanwhile
		want     *recovery.ResumeState
	}{
		{"clean", true, nil, false, true, nil, &snap},
		{"peer closed", true, &ConnectionTransportError{Remote: true, ErrorCode: 0xa}, false, true, nil, &snap},
		{"no handshake", false, nil, false, true, nil, &taken},
		{"optimistic ACK", true, connectionFailure(&TransportError{Code: 0xa, Reason: "ack"}), false, true, nil, &taken},
		{"short connection", true, nil, false, false, nil, &taken},
		{"failed jump", true, nil, true, true, nil, &snap},
		{"failed jump, nothing measured", true, nil, true, false, nil, nil},
		{"failed jump, another's state since", true, nil, true, false, &other, &other},
	} {
		states := &testStates{saved: map[string]recovery.ResumeState{}}
		if tc.since != nil {
			states.saved["192.0.2.7"] = *tc.since
		}
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
		if saved != (tc.want != nil) || saved && got != *tc.want {
			t.Fatalf("%s: saved %v %+v", tc.name, saved, got)
		}
	}
	// A single-path connection whose path an address-aware listener owns
	// saves; one that negotiated multipath doesn't. A wildcard listener
	// publishes no address, so the admitted one keys the state.
	for _, tc := range []struct {
		name      string
		multipath *pathCIDs
		published bool
		key       string
	}{
		{"base-only owner", &pathCIDs{baseOnly: true}, true, "192.0.2.7"},
		{"negotiated multipath", &pathCIDs{}, true, ""},
		{"wildcard listener", nil, false, "198.51.100.4"},
	} {
		states := &testStates{saved: map[string]recovery.ResumeState{}}
		c := &Conn{config: Config{Server: true, CongestionStates: states}, congestion: &testResumer{snapshot: &snap}, complete: true, multipath: tc.multipath}
		if tc.published {
			c.publishRemoteAddress(netip.MustParseAddrPort("192.0.2.7:4433"))
		}
		c.carefulResume = carefulResumeState{key: "198.51.100.4", admitted: netip.MustParseAddrPort("198.51.100.4:5000")}
		c.saveCongestionState()
		_, saved := states.saved[tc.key]
		if saved != (tc.key != "") || len(states.saved) > 1 {
			t.Fatalf("%s: saved %v", tc.name, states.saved)
		}
	}
	if !errors.As(connectionFailure(&TransportError{Code: 0xa}), new(*ConnectionTransportError)) {
		t.Fatal("a local transport error isn't a ConnectionTransportError")
	}
}
