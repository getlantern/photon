package transport

import (
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestMigrationSelectionOrdering(t *testing.T) {
	local := netip.MustParseAddrPort("192.0.2.1:443")
	original := pathTuple{local, netip.MustParseAddrPort("192.0.2.2:1000")}
	first := pathTuple{local, netip.MustParseAddrPort("192.0.2.2:1001")}
	second := pathTuple{local, netip.MustParseAddrPort("192.0.2.3:1002")}
	now := time.Unix(101, 0)
	m, err := newMigrationSelection(original)
	if err != nil {
		t.Fatal(err)
	}
	observe := func(tuple pathTuple, pn uint64, nonProbing bool) *migrationCandidate {
		t.Helper()
		c, err := m.observeAuthenticated(tuple, pn, nonProbing, 1200, now, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	observe(original, 10, true)
	if observe(first, 9, true) != nil || observe(first, 50, false) != nil {
		t.Fatal("old/probing packet nominated address")
	}
	c := observe(first, 11, true)
	if c == nil || m.validated != original {
		t.Fatal("nomination changed established tuple")
	}
	deadline, credit := c.validation.expires, c.budget.credit
	if observe(first, 11, true) != c || c.validation.expires != deadline || c.budget.credit != credit+3600 {
		t.Fatal("duplicate replaced candidate or lost credit")
	}
	if observe(second, 10, true) != nil || m.candidate != c {
		t.Fatal("reordered packet replaced candidate")
	}
	next := observe(second, 12, true)
	if next == nil || next == c || m.candidate != next {
		t.Fatal("newer address did not replace candidate")
	}
	if _, err := m.commit(c); err == nil {
		t.Fatal("stale candidate committed")
	}
	if _, err := m.commit(next); err == nil {
		t.Fatal("unvalidated candidate committed")
	}
	observe(original, 11, true)
	if m.candidate != next {
		t.Fatal("old primary packet canceled candidate")
	}
	observe(original, 13, true)
	if m.candidate != nil || m.validated != original {
		t.Fatal("newer primary packet failed to cancel nomination")
	}
}

func TestMigrationSelectionExpiryAndCommit(t *testing.T) {
	local := netip.MustParseAddrPort("192.0.2.1:443")
	original := pathTuple{local, netip.MustParseAddrPort("192.0.2.2:1000")}
	changed := pathTuple{local, netip.MustParseAddrPort("192.0.2.2:1001")}
	now := time.Unix(101, 0)
	m, _ := newMigrationSelection(original)
	c, err := m.observeAuthenticated(changed, 0, true, 1200, now, time.Second)
	if err != nil || c == nil {
		t.Fatal("PN zero not admitted", err)
	}
	m.expire(now.Add(3 * time.Second))
	if m.candidate != nil || m.validated != original {
		t.Fatal("expired candidate lost fallback")
	}
	replay, err := m.observeAuthenticated(changed, 0, true, 1200, now.Add(4*time.Second), time.Second)
	if err != nil || replay != nil {
		t.Fatal("replay restarted expired candidate", err)
	}
	now = now.Add(4 * time.Second)
	c, err = m.observeAuthenticated(changed, 1, true, 1200, now, time.Second)
	if err != nil || c == nil {
		t.Fatal(err)
	}
	frame, ready, err := c.challenge(now)
	if err != nil || !ready {
		t.Fatal(err)
	}
	if err := c.sent(frame, 1200, now); err != nil {
		t.Fatal(err)
	}
	if !c.response(changed, wire.PathResponseFrame{Data: frame.Data}, now) {
		t.Fatal("candidate proof failed")
	}
	old, err := m.commit(c)
	if err != nil || old != original || m.validated != changed || m.candidate != nil {
		t.Fatal("commit failed", err)
	}
	if _, err := m.commit(c); err == nil {
		t.Fatal("candidate committed twice")
	}
}
