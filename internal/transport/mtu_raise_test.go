package transport

import (
	"github.com/getlantern/photon/internal/wire"
	"syscall"
	"testing"
	"time"
)

func TestMTUCompletedSearchReopens(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		e, c := mtuWriteEndpoint(t, version)
		defer e.Close()
		a := c.multipath.paths[1].application
		// A narrow interval reaches a completed lower size with two real probes.
		m, err := newMTUSearch(1202)
		if err != nil {
			t.Fatal(err)
		}
		a.mtu = &m
		now := time.Unix(101, 0)
		out, err := e.NextDatagram(now)
		if err != nil || out == nil || len(out.Data) != 1201 {
			t.Fatal("first probe", err)
		}
		ack := wire.PathAckFrame{PathID: 1, AckFrame: wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 0, Largest: 0}}}}
		if _, err := c.multipath.receiveACK(ack, now.Add(time.Millisecond), 0, 0, false); err != nil {
			t.Fatal(err)
		}
		if !a.mtuRaiseDeadline().IsZero() {
			t.Fatal("incomplete search scheduled recheck")
		}
		now = now.Add(time.Second)
		out, err = e.NextDatagram(now)
		if err != nil || out == nil || len(out.Data) != 1202 {
			t.Fatal("upper probe", err)
		}
		e.HandleWriteError(out, syscall.EMSGSIZE, now)
		deadline := now.Add(10 * time.Minute)
		if a.mtuRaiseDeadline() != deadline || a.mtu.confirmed != 1201 || a.mtu.upper != 1201 {
			t.Fatal("missing completed-search maintenance")
		}
		// Duplicate/old ACK feedback must not postpone the scheduled recheck.
		if _, err := c.multipath.receiveACK(ack, now.Add(time.Minute), 0, 0, false); err != nil {
			t.Fatal(err)
		}
		if a.mtuRaiseDeadline() != deadline || a.expireMTURaise(deadline.Add(-time.Nanosecond)) {
			t.Fatal("recheck moved or expired early")
		}
		if out, err = e.NextDatagram(deadline.Add(-time.Nanosecond)); err != nil || out != nil {
			t.Fatal("completed search sent prematurely", err)
		}
		if at := c.multipathRecoveryDeadline(); at.IsZero() || at.After(deadline) {
			t.Fatal("owner misses recheck")
		}
		if err := c.expireMultipathRecovery(deadline); err != nil {
			t.Fatal(err)
		}
		if a.mtu.confirmed != 1201 || a.mtu.upper != 1202 || !a.mtuRaiseDeadline().IsZero() || a.expireMTURaise(deadline) {
			t.Fatal("recheck discarded confirmed evidence or repeated")
		}
		out, err = e.NextDatagram(deadline)
		if err != nil || out == nil || len(out.Data) != 1202 || out.mtu.number != 2 {
			t.Fatal("fresh upward probe", err)
		}
		if _, err := c.multipath.receiveACK(ack, deadline, 0, 0, false); err != nil {
			t.Fatal(err)
		}
		if !a.mtu.pending || a.mtu.confirmed != 1201 {
			t.Fatal("old ACK confirmed recheck")
		}
		ack.Ranges = []wire.AckRange{{Smallest: 2, Largest: 2}}
		if _, err := c.multipath.receiveACK(ack, deadline.Add(time.Millisecond), 0, 0, false); err != nil {
			t.Fatal(err)
		}
		if a.mtu.confirmed != 1202 || !a.mtuRaiseDeadline().IsZero() {
			t.Fatal("ceiling confirmation should finish maintenance")
		}
	}
}

func TestMTURaiseResetCancelsOldTupleTimer(t *testing.T) {
	a := newPathApplication()
	m, _ := newMTUSearch(1500)
	m.confirmed, m.upper = 1350, 1350
	m.sentAny, m.lastNumber = true, 42
	a.mtu = &m
	now := time.Unix(101, 0)
	a.updateMTURaise(now)
	a.mtuNextProbe = now.Add(time.Second)
	a.resetMTU()
	if !a.mtuRaiseDeadline().IsZero() || !a.mtuRaiseAt.IsZero() || a.expireMTURaise(now.Add(time.Hour)) || a.mtu.confirmed != 1200 || a.mtu.lastNumber != 42 || a.mtuNextProbe != now.Add(time.Second) {
		t.Fatal("tuple reset retained maintenance or lost PN/rate state")
	}
}
