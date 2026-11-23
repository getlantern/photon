package transport

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/stream"
	"github.com/getlantern/photon/internal/wire"
)

func TestMultipathStreamAndDatagramDispatch(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		params := streamParameters()
		params.MaxDatagramFrameSize = 1200
		client, server := testPair(t, v, params)
		drivePair(t, client, server, false, false)
		attachSlowPath(t, client)
		var secondary []byte
		for seq, id := range client.multipath.paths[1].local.active {
			if seq == 0 {
				secondary = bytes.Clone(id.cid)
				client.multipath.markSent(1, seq)
			}
		}
		now := time.Unix(101, 0)
		keys := server.writeKeys(applicationSpace)
		// A server-initiated bidi stream uses ID 1. Deliver its tail before its
		// prefix, on a different path with the same packet number.
		tail, _ := (wire.StreamFrame{StreamID: 1, Offset: 5, Data: []byte("world"), Fin: true}).Append(nil)
		tail, _ = (wire.DatagramFrame{Data: []byte("once")}).Append(tail)
		raw := sealPathPayload(t, keys, secondary, 1, 100, tail)
		if err := client.handleMultipathPacket(raw, now); err != nil {
			t.Fatal(err)
		}
		if err := client.handleMultipathPacket(raw, now); err != nil {
			t.Fatal("replay", err)
		}
		prefix, _ := (wire.StreamFrame{StreamID: 1, Data: []byte("hello")}).Append(nil)
		raw = sealPathPayload(t, keys, client.config.LocalCID, 0, 100, prefix)
		if err := client.handleMultipathPacket(raw, now); err != nil {
			t.Fatal(err)
		}
		id, err := client.AcceptStream(false)
		if err != nil || id != 1 {
			t.Fatal(id, err)
		}
		out := make([]byte, 20)
		n, err := client.ReadStream(id, out)
		if err != io.EOF || string(out[:n]) != "helloworld" {
			t.Fatal("cross-path reassembly", n, err, string(out[:n]))
		}
		d, err := client.ReceiveDatagram()
		if err != nil || string(d) != "once" {
			t.Fatal(d, err)
		}
		if _, err := client.ReceiveDatagram(); !errors.Is(err, stream.ErrWouldBlock) {
			t.Fatal("replayed datagram delivered twice", err)
		}
	}
}

func TestMultipathEncryptedACKDispatch(t *testing.T) {
	c, keys, cid := pathDecodeFixture(t, wire.Version1)
	now := time.Unix(101, 0)
	a, _ := c.multipath.application(1)
	item := &sendItem{frame: wire.PingFrame{}, bytes: 1}
	c.queuedBytes++
	sendPathPacket(t, a, now)
	a.space.sent[0] = item
	payload, _ := (wire.PathAckFrame{PathID: 1, AckFrame: wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 0, Largest: 0}}}}).Append(nil)
	// ACK arrives on path zero but names a transmission on path one.
	raw := sealPathPayload(t, keys, c.config.LocalCID, 0, 100, payload)
	before := c.queuedBytes
	if err := c.handleMultipathPacket(raw, now.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if !item.acked || c.queuedBytes != before-1 || a.sent.BytesInFlight != 0 {
		t.Fatal("ACK used receiving path")
	}
	// A distinct packet with the same ACK must not release bytes a second time.
	raw = sealPathPayload(t, keys, cid, 1, 100, payload)
	if err := c.handleMultipathPacket(raw, now.Add(2*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if c.queuedBytes != before-1 {
		t.Fatal("double completion")
	}
}

func TestMultipathFlowControlRemainsConnectionWide(t *testing.T) {
	params := streamParameters()
	params.InitialMaxData = 6
	client, server := testPair(t, wire.Version1, params)
	drivePair(t, client, server, false, false)
	attachSlowPath(t, client)
	var cid []byte
	for seq, id := range client.multipath.paths[1].local.active {
		if seq == 0 {
			cid = id.cid
			client.multipath.markSent(1, seq)
		}
	}
	now := time.Unix(101, 0)
	keys := server.writeKeys(applicationSpace)
	first, _ := (wire.StreamFrame{StreamID: 1, Data: []byte("1234")}).Append(nil)
	if err := client.handleMultipathPacket(sealPathPayload(t, keys, client.config.LocalCID, 0, 100, first), now); err != nil {
		t.Fatal(err)
	}
	second, _ := (wire.StreamFrame{StreamID: 5, Data: []byte("5678")}).Append(nil)
	err := client.handleMultipathPacket(sealPathPayload(t, keys, cid, 1, 100, second), now)
	requirePathError(t, err, 3, 0x0a)
}
