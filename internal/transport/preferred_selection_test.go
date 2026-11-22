package transport

import (
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestPreferredSelectionRequiresProofAndNomination(t *testing.T) {
	for _, proofFirst := range []bool{false, true} {
		name := "nomination-first"
		if proofFirst {
			name = "proof-first"
		}
		t.Run(name, func(t *testing.T) {
			old := pathTuple{netip.MustParseAddrPort("192.0.2.1:443"), netip.MustParseAddrPort("192.0.2.2:4000")}
			next := pathTuple{netip.MustParseAddrPort("192.0.2.3:443"), old.remote}
			m, err := newMigrationSelection(old)
			if err != nil {
				t.Fatal(err)
			}
			now := time.Unix(101, 0)
			candidate, err := m.observePreferredAuthenticated(next, 10, false, 1200, now, time.Second)
			if err != nil || candidate == nil {
				t.Fatal("probing admission", err)
			}
			expires := candidate.validation.expires
			if m.canCommit(candidate) {
				t.Fatal("probe permitted commit")
			}
			// Original-address traffic does not destroy or restart validation.
			if _, err := m.observeAuthenticated(old, 11, true, 1200, now.Add(time.Millisecond), time.Second); err != nil {
				t.Fatal(err)
			}
			if m.candidate != candidate || candidate.validation.expires != expires {
				t.Fatal("original traffic discarded or renewed proof")
			}
			prove := func() {
				t.Helper()
				challenge, ready, err := candidate.challenge(now)
				if err != nil || !ready {
					t.Fatal(err)
				}
				if err := candidate.sent(challenge, 1200, now); err != nil {
					t.Fatal(err)
				}
				if !candidate.response(next, wire.PathResponseFrame{Data: challenge.Data}, now.Add(time.Millisecond)) {
					t.Fatal("proof rejected")
				}
			}
			nominate := func() {
				t.Helper()
				got, err := m.observePreferredAuthenticated(next, 12, true, 1200, now.Add(2*time.Millisecond), time.Second)
				if err != nil || got != candidate {
					t.Fatal("nomination replaced candidate", err)
				}
			}
			if proofFirst {
				prove()
			} else {
				nominate()
			}
			if _, err := m.commit(candidate); err == nil || m.validated != old {
				t.Fatal("committed with only one precondition")
			}
			if proofFirst {
				nominate()
			} else {
				prove()
			}
			if !m.canCommit(candidate) {
				t.Fatal("both conditions did not permit commit")
			}
			// A newer packet on the original tuple revokes nomination, not proof.
			if _, err := m.observeAuthenticated(old, 13, true, 1200, now.Add(3*time.Millisecond), time.Second); err != nil {
				t.Fatal(err)
			}
			if m.candidate != candidate || candidate.validation.state != PathValidated || m.canCommit(candidate) {
				t.Fatal("original traffic mishandled nomination")
			}
			for _, pn := range []uint64{12, 13} {
				if _, err := m.observePreferredAuthenticated(next, pn, true, 1200, now.Add(4*time.Millisecond), time.Second); err != nil {
					t.Fatal(err)
				}
				if m.canCommit(candidate) {
					t.Fatal("stale packet reauthorized commit")
				}
			}
			if _, err := m.observePreferredAuthenticated(next, 14, true, 1200, now.Add(5*time.Millisecond), time.Second); err != nil {
				t.Fatal(err)
			}
			previous, err := m.commit(candidate)
			if err != nil || previous != old || m.validated != next || m.candidate != nil {
				t.Fatal("validated commit", err)
			}
		})
	}
}

func TestPreferredSelectionExpiryAndBound(t *testing.T) {
	old := pathTuple{netip.MustParseAddrPort("192.0.2.1:443"), netip.MustParseAddrPort("192.0.2.2:4000")}
	next := pathTuple{netip.MustParseAddrPort("192.0.2.3:443"), old.remote}
	m, _ := newMigrationSelection(old)
	now := time.Unix(101, 0)
	candidate, err := m.observePreferredAuthenticated(next, 10, false, 1200, now, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	other := next
	other.remote = netip.MustParseAddrPort("192.0.2.2:4001")
	if got, err := m.observePreferredAuthenticated(other, 11, false, 1200, now, time.Second); err != nil || got != nil || m.candidate != candidate {
		t.Fatal("competing probe replaced bounded candidate", err)
	}
	deadline := candidate.validation.expires
	if got, err := m.observePreferredAuthenticated(next, 10, false, 1200, deadline, time.Second); err != nil || got != nil || m.candidate != nil {
		t.Fatal("replay renewed expired validation", err)
	}
	if m.validated != old {
		t.Fatal("expiry changed fallback")
	}
	got, err := m.observePreferredAuthenticated(next, 12, false, 1200, deadline, time.Second)
	if err != nil || got == nil || got == candidate || m.canCommit(got) {
		t.Fatal("fresh probe admission", err)
	}
}
