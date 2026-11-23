package transport

import (
	"crypto/tls"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestEarlySendAfterCompatibleInitial(t *testing.T) {
	for _, original := range []wire.Version{wire.Version1, wire.Version2} {
		ct, st := testTLS(t)
		ct.ClientSessionCache = tls.NewLRUClientSessionCache(4)
		c, s := earlyPair(t, original, ct, st, true)
		drivePair(t, c, s, false, false)
		c, s = earlyPairConfigured(t, original, ct, st, true, false, true)
		now := time.Unix(100, 0)
		for _, b := range drain(t, c, now) {
			if err := s.HandleDatagram(b, now); err != nil {
				t.Fatal(err)
			}
		}
		var pending [][]byte
		for _, b := range drain(t, s, now) {
			h, err := wire.ParseHeader(b, 8)
			if err != nil {
				t.Fatal(err)
			}
			if h.Type == wire.Initial {
				if err := c.HandleDatagram(b, now); err != nil {
					t.Fatal(err)
				}
			} else {
				pending = append(pending, b)
			}
		}
		if c.Version() == original || !c.sendingEarly() {
			t.Fatal("missing version selection before early send")
		}
		if s.acceptPacketVersion(wire.Header{Long: true, Type: wire.ZeroRTT, Version: c.Version()}) {
			t.Fatal("accepted negotiated-version early packet")
		}
		id, err := c.OpenStream(false)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.WriteStream(id, []byte("after compatible Initial"), true); err != nil {
			t.Fatal(err)
		}
		var delayed [][]byte
		for _, b := range drain(t, c, now) {
			h, err := wire.ParseHeader(b, 8)
			if err != nil {
				t.Fatal(err)
			}
			if h.Type == wire.ZeroRTT {
				if h.Version != original {
					t.Fatal("early packet changed version")
				}
				delayed = append(delayed, b)
			} else {
				if err := s.HandleDatagram(b, now); err != nil {
					t.Fatal(err)
				}
			}
		}
		if len(delayed) == 0 {
			t.Fatal("no early packets")
		}
		for _, b := range pending {
			if err := c.HandleDatagram(b, now); err != nil {
				t.Fatal(err)
			}
		}
		// Deliver client Finished before the delayed original-version early data.
		for _, b := range drain(t, c, now) {
			if err := s.HandleDatagram(b, now); err != nil {
				t.Fatal(err)
			}
		}
		if !s.confirmed || s.originalInitialRead != nil {
			t.Fatal("server did not discard Initial keys")
		}
		for _, b := range delayed {
			if err := s.HandleDatagram(b, now); err != nil {
				t.Fatal(err)
			}
		}
		got, err := s.AcceptStream(false)
		if err != nil || got != id {
			t.Fatal("delayed early stream", err)
		}
		buf := make([]byte, 64)
		n, err := s.ReadStream(got, buf)
		if string(buf[:n]) != "after compatible Initial" {
			t.Fatal("delayed early payload", err)
		}
		drivePair(t, c, s, false, false)
		selected := c.Version()
		resumed, peer := earlyPair(t, selected, ct, st, true)
		if !resumed.sendingEarly() {
			t.Fatal("new ticket not bound to negotiated version")
		}
		drivePair(t, resumed, peer, false, false)
		if !resumed.EarlyDataAccepted() {
			t.Fatal("negotiated-version ticket rejected")
		}
		originalClient, originalServer := earlyPair(t, original, ct, st, true)
		if originalClient.sendingEarly() {
			t.Fatal("new ticket reused across versions")
		}
		drivePair(t, originalClient, originalServer, false, false)
		if originalClient.ConnectionState().DidResume {
			t.Fatal("cross-version resumption")
		}
	}
}
