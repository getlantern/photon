package transport

import (
	"bytes"
	"context"
	"net/netip"
	"testing"
	"time"
)

func TestEndpointResetUnknown(t *testing.T) {
	e, err := NewClientOnlyEndpoint(8, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	key := ResetKey{9}
	peer := netip.MustParseAddrPort("192.0.2.1:443")
	cid := []byte("unknown0")
	now := time.Unix(1000, 0)
	packet := make([]byte, 1200)
	packet[0] = 0x40
	copy(packet[1:], cid)
	if _, out, err := e.HandleDatagram(context.Background(), packet, peer, now); err != nil || out != nil {
		t.Fatal("nil key emitted reset", err)
	}
	e.ResetKey = &key
	for size := 0; size <= 1200; size++ {
		raw := append([]byte(nil), packet[:size]...)
		at := now.Add(time.Duration(size) * time.Second)
		_, out, err := e.HandleDatagram(context.Background(), raw, peer, at)
		if err != nil {
			t.Fatal(err)
		}
		if size <= 21 {
			if out != nil {
				t.Fatal("undersized trigger")
			}
			continue
		}
		token := key.token(cid)
		if out == nil || len(out.Data) >= size || len(out.Data) < 21 || len(out.Data) > 42 || out.Data[0]&0xc0 != 0x40 || !bytes.Equal(out.Data[len(out.Data)-16:], token[:]) {
			t.Fatal("invalid reset", size)
		}
	}
	for _, first := range []byte{0, 0x80, 0xc0} {
		packet[0] = first
		if out, err := e.resetUnknown(packet, e.local, peer, now.Add(time.Hour)); err != nil || out != nil {
			t.Fatal("non-QUIC or long header reset", err)
		}
	}
	packet[0] = 0x40
	e.router.ids[string(cid)] = nil
	if out, err := e.resetUnknown(packet, e.local, peer, now.Add(time.Hour)); err != nil || out != nil {
		t.Fatal("live CID token disclosed", err)
	}
	delete(e.router.ids, string(cid))
	at := now.Add(2 * time.Hour)
	for i := 0; i < 101; i++ {
		out, err := e.resetUnknown(packet, e.local, peer, at)
		if err != nil || (out != nil) != (i < 100) {
			t.Fatal("rate limit", i, err)
		}
	}
	if out, err := e.resetUnknown(packet, e.local, peer, at.Add(time.Second)); err != nil || out == nil {
		t.Fatal("rate window did not recover", err)
	}
}
