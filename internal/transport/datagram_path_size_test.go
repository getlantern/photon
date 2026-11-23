package transport

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func requireDatagramLimit(t *testing.T, err error, want int64) {
	t.Helper()
	var size *DatagramTooLargeError
	if !errors.As(err, &size) || size.MaxDatagramPayloadSize != want || !errors.Is(err, ErrDatagramTooLarge) || !errors.Is(err, &DatagramTooLargeError{MaxDatagramPayloadSize: want}) || errors.Is(err, &DatagramTooLargeError{MaxDatagramPayloadSize: want + 1}) {
		t.Fatalf("DATAGRAM limit: %T %v, want %d", err, err, want)
	}
}

func sizedDatagramPair(t *testing.T, v wire.Version) (*Conn, *Conn) {
	t.Helper()
	params := streamParameters()
	params.MaxDatagramFrameSize = 65535
	c, peer := testPair(t, v, params)
	drivePair(t, c, peer, false, false)
	for _, conn := range []*Conn{c, peer} {
		conn.config.InitialPacketSize = 1452
		conn.config.DisablePathMTUDiscovery = true
		p, err := adoptPathZero(conn, 3, 3, 4)
		if err != nil {
			t.Fatal(err)
		}
		conn.multipath = p
	}
	return c, peer
}

func TestDatagramConfiguredPathSize(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		c, peer := sizedDatagramPair(t, v)
		payload := bytes.Repeat([]byte{93}, 1410)
		if err := c.SendDatagram(payload); err != nil {
			t.Fatal(err)
		}
		before := c.queuedBytes
		requireDatagramLimit(t, c.SendDatagram(make([]byte, 1411)), 1410)
		if c.queuedBytes != before || c.pendingDatagrams != 1 {
			t.Fatal("rejected datagram changed accounting")
		}
		packet, err := c.nextPacketOnPath(0, time.Unix(101, 0))
		if err != nil || len(packet) <= 1200 || len(packet) > 1452 {
			t.Fatal("large DATAGRAM packet", len(packet), err)
		}
		if err := peer.handleMultipathPacket(packet, time.Unix(101, 0)); err != nil {
			t.Fatal(err)
		}
		got, err := peer.ReceiveDatagram()
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatal("datagram delivery", len(got), err)
		}
		if c.queuedBytes != 0 || c.pendingDatagrams != 0 {
			t.Fatal("sent datagram retained")
		}
		c.peerParams.MaxDatagramFrameSize = 100
		requireDatagramLimit(t, c.SendDatagram(make([]byte, 100)), 99)
	}
}

func TestDatagramPathShrinkDropsWithoutSend(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		c, peer := sizedDatagramPair(t, v)
		for i := 0; i < maxBufferedDatagrams-1; i++ {
			if err := c.SendDatagram(make([]byte, 1410)); err != nil {
				t.Fatal(err)
			}
		}
		if err := c.SendDatagram([]byte("still fits")); err != nil {
			t.Fatal(err)
		}
		app := c.multipath.paths[0].application
		app.resetMTU()
		requireDatagramLimit(t, c.SendDatagram(make([]byte, 1159)), 1158)
		pn, sealed, flight := app.space.nextPN, c.sealed[applicationSpace], app.sent.BytesInFlight
		packet, err := c.nextPacketOnPath(0, time.Unix(101, 0))
		if err != nil || packet == nil || len(packet) > 1200 {
			t.Fatal("shrink blocked smaller message", err)
		}
		if app.space.nextPN != pn+1 || c.sealed[applicationSpace] != sealed+1 || app.sent.BytesInFlight != flight+uint64(len(packet)) {
			t.Fatal("discarded messages committed sends")
		}
		if c.queuedBytes != 0 || c.pendingDatagrams != 0 || len(app.space.queue) != 0 {
			t.Fatal("discard accounting")
		}
		if err := peer.handleMultipathPacket(packet, time.Unix(101, 0)); err != nil {
			t.Fatal(err)
		}
		got, err := peer.ReceiveDatagram()
		if err != nil || string(got) != "still fits" {
			t.Fatal("wrong delivered message", err)
		}
		if c.Err() != nil {
			t.Fatal("path shrink closed connection", c.Err())
		}
	}
}

func TestDatagramBorrowSkipsSmallerPath(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		c, _ := sendingPathPair(t, v)
		c.peerParams.MaxDatagramFrameSize = 65535
		c.config.InitialPacketSize = 1452
		c.config.DisablePathMTUDiscovery = true
		source := c.multipath.paths[0].application
		c.seedInitialPathMTU(source)
		// Path 1 is unvalidated during admission, so the message belongs to path 0.
		if err := c.SendDatagram(make([]byte, 1410)); err != nil {
			t.Fatal(err)
		}
		dest := c.multipath.paths[1].application
		dest.validation.state = PathValidated
		dest.budget.Validate()
		item := source.space.queue[0]
		charge := c.queuedBytes
		packet, err := c.nextSharedPathData(1, []uint64{0, 1}, time.Unix(101, 0))
		if err != nil || packet != nil {
			t.Fatal("oversized borrowing", err)
		}
		if len(source.space.queue) != 1 || source.space.queue[0] != item || !item.queued || item.acked || len(dest.space.queue) != 0 || c.queuedBytes != charge || c.pendingDatagrams != 1 {
			t.Fatal("borrow changed source ownership")
		}
		packet, err = c.nextPacketOnPath(0, time.Unix(101, 0))
		if err != nil || len(packet) <= 1200 || c.pendingDatagrams != 0 {
			t.Fatal("source no longer sendable", err)
		}
	}
}
