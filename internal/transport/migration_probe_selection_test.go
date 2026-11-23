package transport

import (
	"net/netip"
	"testing"
	"time"
)

func TestProbingCandidateNominationAndReplay(t *testing.T) {
	old := pathTuple{netip.MustParseAddrPort("192.0.2.1:443"), netip.MustParseAddrPort("192.0.2.2:444")}
	next := pathTuple{old.local, netip.MustParseAddrPort("192.0.2.2:445")}
	now := time.Unix(101, 0)
	m, err := newMigrationSelection(old)
	if err != nil {
		t.Fatal(err)
	}
	c, err := m.observeProbingAuthenticated(next, 10, false, 1200, now, time.Second, false)
	if err != nil || c == nil {
		t.Fatal(err)
	}
	expires := c.validation.expires
	if _, err = m.observeProbingAuthenticated(next, 11, false, 1200, now.Add(time.Second), time.Second, false); err != nil {
		t.Fatal(err)
	}
	if c.validation.expires != expires {
		t.Fatal("probe prolonged validation")
	}
	m.expire(expires)
	for _, pn := range []uint64{9, 10, 11} {
		c, err = m.observeProbingAuthenticated(next, pn, false, 1200, expires, time.Second, false)
		if err != nil || c != nil {
			t.Fatal("replay recreated expired candidate", pn, err)
		}
	}
	c, err = m.observeProbingAuthenticated(next, 12, false, 1200, expires, time.Second, false)
	if err != nil || c == nil {
		t.Fatal(err)
	}
	c.validation.state = PathValidated
	if m.canCommit(c) {
		t.Fatal("probe-only proof authorized switch")
	}
	if _, err = m.observeAuthenticated(old, 20, true, 1200, expires, time.Second); err != nil {
		t.Fatal(err)
	}
	if m.candidate != c {
		t.Fatal("old path discarded probe")
	}
	if _, err = m.observeAuthenticated(next, 19, true, 1200, expires, time.Second); err != nil {
		t.Fatal(err)
	}
	if m.canCommit(c) {
		t.Fatal("reordered application data nominated path")
	}
	if _, err = m.observeAuthenticated(next, 21, true, 1200, expires, time.Second); err != nil {
		t.Fatal(err)
	}
	if !m.canCommit(c) {
		t.Fatal("new application data did not nominate path")
	}
}

func TestProbingCandidateReplacement(t *testing.T) {
	old := pathTuple{netip.MustParseAddrPort("192.0.2.1:443"), netip.MustParseAddrPort("192.0.2.2:444")}
	first := pathTuple{old.local, netip.MustParseAddrPort("192.0.2.2:445")}
	second := pathTuple{old.local, netip.MustParseAddrPort("192.0.2.2:446")}
	now := time.Unix(101, 0)
	for _, mode := range []string{"pending", "validated", "nominated", "preferred"} {
		t.Run(mode, func(t *testing.T) {
			m, err := newMigrationSelection(old)
			if err != nil {
				t.Fatal(err)
			}
			a, err := m.observeProbingAuthenticated(first, 10, false, 1200, now, time.Second, mode == "preferred")
			if err != nil || a == nil {
				t.Fatal(err)
			}
			if mode != "pending" {
				a.validation.state = PathValidated
			}
			if mode == "nominated" {
				m.candidateNominated = true
			}
			for _, pn := range []uint64{9, 10} {
				got, err := m.observeProbingAuthenticated(second, pn, false, 1200, now, time.Second, false)
				if err != nil || got != nil || m.candidate != a {
					t.Fatal("stale probe replaced candidate", pn, err)
				}
			}
			b, err := m.observeProbingAuthenticated(second, 11, false, 1200, now, time.Second, false)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "nominated" || mode == "preferred" {
				if b != nil || m.candidate != a {
					t.Fatal("protected candidate displaced")
				}
				return
			}
			if b == nil || b == a || m.candidate != b || b.tuple != second || m.canCommit(a) || m.canCommit(b) {
				t.Fatal("incorrect replacement state")
			}
			if got, err := m.observeProbingAuthenticated(first, 10, false, 1200, now, time.Second, false); err != nil || got != nil || m.candidate != b {
				t.Fatal("old replay displaced replacement", err)
			}
		})
	}
}
