package transport

import (
	"errors"
	"github.com/getlantern/photon/internal/wire"
	"testing"
)

func TestHandshakeObserver(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		c, s := testPair(t, v)
		counts := map[*Conn]int{}
		confirmed := map[*Conn]int{}
		for _, conn := range []*Conn{c, s} {
			if err := conn.WatchHandshakeConfirmed(func() {
				if !conn.confirmed || counts[conn] != 1 {
					t.Error("confirmation before completion")
				}
				confirmed[conn]++
			}); err != nil {
				t.Fatal(err)
			}
			if err := conn.WatchHandshakeConfirmed(func() {}); err == nil {
				t.Fatal("confirmation observer replaced")
			}
			if err := conn.WatchHandshakeComplete(func() {
				if !conn.HandshakeComplete() || !conn.ConnectionState().HandshakeComplete {
					t.Error("callback before TLS completion")
				}
				counts[conn]++
			}); err != nil {
				t.Fatal(err)
			}
			if err := conn.WatchHandshakeComplete(func() {}); err == nil {
				t.Fatal("pending observer replaced")
			}
		}
		drivePair(t, c, s, false, false)
		for _, conn := range []*Conn{c, s} {
			if counts[conn] != 1 || confirmed[conn] != 1 || conn.onHandshakeComplete != nil || conn.onHandshakeConfirmed != nil {
				t.Fatal("notification count or retained callback")
			}
			if err := conn.WatchHandshakeComplete(func() { counts[conn]++ }); err != nil || counts[conn] != 2 {
				t.Fatal("late observer", err)
			}
		}
	}
	c, _ := testPair(t, wire.Version1)
	called := false
	if err := c.WatchHandshakeComplete(func() { called = true }); err != nil {
		t.Fatal(err)
	}
	if err := c.WatchHandshakeConfirmed(func() { called = true }); err != nil {
		t.Fatal(err)
	}
	c.fail(errors.New("handshake failure"))
	if called || c.onHandshakeComplete != nil || c.onHandshakeConfirmed != nil {
		t.Fatal("failed handshake notified or retained callback")
	}
	if err := c.WatchHandshakeComplete(func() {}); err == nil {
		t.Fatal("observer accepted after failure")
	}
}
