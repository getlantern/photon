package transport

import (
	"bytes"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
)

func TestOutboundReusePreservesOwnedResultsAndClearsMetadata(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		c, _ := testPair(t, v)
		c.recovery.EnableECN()
		endpoint, err := NewClientEndpoint(c, netip.MustParseAddrPort("127.0.0.1:443"))
		if err != nil {
			t.Fatal(err)
		}
		at := time.Unix(100, 0)
		first, err := endpoint.NextDatagram(at)
		if err != nil || first == nil {
			t.Fatal(first, err)
		}
		original := bytes.Clone(first.Data)
		if err := c.handleAck(initialSpace, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 0, Largest: 0}}}, at.Add(time.Millisecond)); err != nil {
			t.Fatal(err)
		}
		if err := c.enqueue(initialSpace, wire.PingFrame{}, 1); err != nil {
			t.Fatal(err)
		}
		storage := Outbound{ECN: recovery.ECT1, mtu: new(outboundMTUProbe), large: new(outboundLargeStream), ecn: ecnCommit{space: recovery.Handshake, number: 999}}
		packetStorage := make([]byte, 2048)
		next, err := endpoint.nextDatagramInto(at.Add(time.Second), &storage, packetStorage, nil)
		if err != nil || next != &storage {
			t.Fatal("descriptor not reused", err)
		}
		if &next.Data[0] != &packetStorage[0] || c.sealStorage != nil {
			t.Fatal("packet buffer not scoped to construction")
		}
		clear(packetStorage)
		if next.ECN != recovery.NotECT || next.mtu != nil || next.large != nil || next.ecn.space != recovery.Initial || next.ecn.number != 1 {
			t.Fatal("stale descriptor metadata")
		}
		if first == next || first.ECN != recovery.ECT0 || !bytes.Equal(first.Data, original) {
			t.Fatal("owned prior result changed")
		}
	}
}

func TestCachedCloseDoesNotBorrowSocketStorage(t *testing.T) {
	c, _ := testPair(t, wire.Version1)
	endpoint, err := NewClientEndpoint(c, netip.MustParseAddrPort("127.0.0.1:443"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(100, 0)
	if err := c.CloseWithError(0, "cached close", now); err != nil {
		t.Fatal(err)
	}
	var descriptor Outbound
	storage := make([]byte, 2048)
	out, err := endpoint.nextDatagramInto(now, &descriptor, storage, nil)
	if err != nil || out == nil {
		t.Fatal("missing close", err)
	}
	saved := bytes.Clone(out.Data)
	for i := range storage {
		storage[i] = 0xff
	}
	if !bytes.Equal(out.Data, saved) || c.sealStorage != nil {
		t.Fatal("cached close aliases socket buffer")
	}
}
