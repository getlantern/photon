package transport

import (
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

// peerUpdatedPair returns a confirmed pair where the server has updated keys
// and the client has followed, sending only an ACK with the new keys, so no
// packet the client sent with them has been acknowledged.
func peerUpdatedPair(t *testing.T) (client, server *Conn, now time.Time) {
	t.Helper()
	client, server = testPair(t, wire.Version1, streamParameters())
	drivePair(t, client, server, false, false)
	now = time.Unix(200, 0)
	if err := server.InitiateKeyUpdateAt(now); err != nil {
		t.Fatal(err)
	}
	if err := server.enqueue(applicationSpace, wire.PingFrame{}, 1); err != nil {
		t.Fatal(err)
	}
	for _, b := range drain(t, server, now) {
		if err := client.HandleDatagram(b, now); err != nil {
			t.Fatal(err)
		}
	}
	ackNow(client)
	for _, b := range drain(t, client, now) {
		if err := server.HandleDatagram(b, now); err != nil {
			t.Fatal(err)
		}
	}
	k := client.applicationKeys
	if !k.updated || k.writeAcknowledged || k.awaitingRead {
		t.Fatal("setup: want an unacknowledged follow of the peer's update", k.updated, k.writeAcknowledged, k.awaitingRead)
	}
	return client, server, now
}

// oneRTTFrames decodes the 1-RTT frames of datagram, sent to to.
func oneRTTFrames(t *testing.T, to *Conn, datagram []byte) []wire.Frame {
	t.Helper()
	h, err := wire.ParseHeader(datagram, len(to.config.LocalCID))
	if err != nil {
		t.Fatal(err)
	}
	_, plain, _, err := to.readKeys(applicationSpace).Open(datagram, h.PNOffset, to.spaces[applicationSpace].received.Expected())
	if err != nil {
		t.Fatal(err)
	}
	frames, _, err := parsePacketFrames(plain, wire.FrameContext{PacketType: wire.OneRTT})
	if err != nil {
		t.Fatal(err)
	}
	return frames
}

func clientLimit(t *testing.T, c *Conn) uint64 {
	t.Helper()
	limit, _ := c.writeKeys(applicationSpace).UsageLimits()
	return limit
}

// Near the AEAD limit, an endpoint that has sent only ACKs since the peer's
// key update sends a PING, so its own forced update at the limit is allowed.
func TestKeyUpdatePINGBeforeAEADLimit(t *testing.T) {
	client, server, now := peerUpdatedPair(t)
	limit := clientLimit(t, client)
	client.sealed[applicationSpace] = limit - limit/16
	out := drain(t, client, now)
	if len(out) != 1 {
		t.Fatal("datagrams", len(out))
	}
	frames := oneRTTFrames(t, server, out[0])
	if len(frames) != 1 {
		t.Fatalf("frames %d", len(frames))
	}
	if _, ok := frames[0].(wire.PingFrame); !ok {
		t.Fatalf("frame %T, want a PING", frames[0])
	}
	if drained := drain(t, client, now); len(drained) != 0 {
		t.Fatal("PING repeated while one is in flight")
	}
	if err := server.HandleDatagram(out[0], now); err != nil {
		t.Fatal(err)
	}
	ackNow(server)
	for _, b := range drain(t, server, now) {
		if err := client.HandleDatagram(b, now); err != nil {
			t.Fatal(err)
		}
	}
	if !client.applicationKeys.writeAcknowledged {
		t.Fatal("PING's acknowledgment did not unblock the update")
	}
	client.sealed[applicationSpace] = limit
	if err := client.enqueue(applicationSpace, wire.PingFrame{}, 1); err != nil {
		t.Fatal(err)
	}
	if b, err := client.NextDatagram(now); err != nil || b == nil || client.err != nil {
		t.Fatal("send at the AEAD limit failed", err, client.err)
	}
}

// Without that PING, the forced update at the limit is refused and the
// connection fails; this is the failure the PING prevents.
func TestAEADLimitWithoutAcknowledgedKeysFails(t *testing.T) {
	client, _, now := peerUpdatedPair(t)
	client.sealed[applicationSpace] = clientLimit(t, client)
	if err := client.enqueue(applicationSpace, wire.PingFrame{}, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := client.NextDatagram(now); err == nil && client.err == nil {
		t.Fatal("send at the limit with an unacknowledged update did not fail")
	}
}

// The same PING is sent on a multipath connection.
func TestMultipathKeyUpdatePINGBeforeAEADLimit(t *testing.T) {
	client, server := sendingPathPair(t, wire.Version1)
	now := time.Unix(101, 0)
	server.multipath.paths[1].application.budget.Validate()
	if err := server.InitiateKeyUpdateAt(now); err != nil {
		t.Fatal(err)
	}
	if err := server.queueFrameOnPath(0, wire.PingFrame{}); err != nil {
		t.Fatal(err)
	}
	raw, err := server.nextPacketOnPath(0, now)
	if err != nil || raw == nil {
		t.Fatal(err)
	}
	if err := client.handleMultipathPacket(raw, now); err != nil {
		t.Fatal(err)
	}
	k := client.applicationKeys
	if !k.updated || k.writeAcknowledged || k.awaitingRead {
		t.Fatal("setup: want an unacknowledged follow of the peer's update", k.updated, k.writeAcknowledged, k.awaitingRead)
	}
	limit := clientLimit(t, client)
	client.sealed[applicationSpace] = limit - limit/16
	sawPING := false
	for i := 0; i < 8 && !sawPING; i++ {
		_, b, err := client.nextMultipathPacket(now)
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			break
		}
		decoded, err := server.decodeMultipathPacket(b, now)
		if err != nil || decoded == nil {
			t.Fatal(err)
		}
		for _, f := range decoded.frames {
			if _, ok := f.(wire.PingFrame); ok {
				sawPING = true
			}
		}
	}
	if !sawPING {
		t.Fatal("no PING near the AEAD limit")
	}
}
