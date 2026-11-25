package transport

import (
	"errors"
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

func threshold(t *testing.T, c *Conn) uint64 {
	t.Helper()
	limit := clientLimit(t, c)
	return limit - limit/4
}

// sendOne returns the client's next datagram, failing on a connection error.
func sendOne(t *testing.T, c *Conn, now time.Time) []byte {
	t.Helper()
	b, err := c.NextDatagram(now)
	if err != nil || c.err != nil {
		t.Fatal(err, c.err)
	}
	return b
}

// An update that is allowed happens at 3/4 of the AEAD limit, without a PING.
func TestKeyUpdateBeforeAEADLimit(t *testing.T) {
	client, server := testPair(t, wire.Version1, streamParameters())
	drivePair(t, client, server, false, false)
	now := time.Unix(200, 0)
	phase := client.applicationKeys.phase
	client.sealed[applicationSpace] = threshold(t, client) - 1
	sendOne(t, client, now)
	if client.applicationKeys.phase != phase {
		t.Fatal("updated keys before the threshold")
	}
	client.sealed[applicationSpace] = threshold(t, client)
	sendOne(t, client, now)
	if client.applicationKeys.phase == phase || client.sealed[applicationSpace] >= threshold(t, client) {
		t.Fatal("keys not updated at the threshold", client.sealed[applicationSpace])
	}
	if client.pingQueued() {
		t.Fatal("PING queued for an allowed update")
	}
}

// After the peer updated keys and this endpoint sent only ACKs, the update at
// the threshold is refused, so a PING goes out; once it is acknowledged, the
// update goes ahead, long before the limit.
func TestKeyUpdatePINGBeforeAEADLimit(t *testing.T) {
	client, server, now := peerUpdatedPair(t)
	phase := client.applicationKeys.phase
	client.sealed[applicationSpace] = threshold(t, client)
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
		t.Fatal("PING repeated within a PTO")
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
	sendOne(t, client, now)
	if client.applicationKeys.phase == phase || client.sealed[applicationSpace] >= threshold(t, client) {
		t.Fatal("keys not updated once the PING was acknowledged", client.sealed[applicationSpace])
	}
}

// A PING that gets no acknowledgment is replaced after a PTO, not before.
func TestKeyUpdatePINGRepeatsAfterPTO(t *testing.T) {
	client, _, now := peerUpdatedPair(t)
	client.sealed[applicationSpace] = threshold(t, client)
	if out := drain(t, client, now); len(out) != 1 {
		t.Fatal("first PING", len(out))
	}
	pto := client.recovery.RTT.PTO(true, true, client.keepAliveACKDelay(), 0)
	if out := drain(t, client, now.Add(pto-time.Millisecond)); len(out) != 0 {
		t.Fatal("PING repeated before a PTO", len(out))
	}
	if !client.pingQueued() {
		if err := client.updateKeysBeforeLimit(now.Add(pto)); err != nil {
			t.Fatal(err)
		}
	}
	if !client.pingQueued() {
		t.Fatal("no replacement PING after a PTO")
	}
}

// A full send buffer skips the PING instead of failing the connection.
func TestKeyUpdatePINGSkippedWhenSendBufferFull(t *testing.T) {
	client, _, now := peerUpdatedPair(t)
	client.sealed[applicationSpace] = threshold(t, client)
	client.queuedBytes = maxSendBytes
	if err := client.updateKeysBeforeLimit(now); err != nil || client.err != nil {
		t.Fatal("full send buffer failed the connection", err, client.err)
	}
	if client.pingQueued() {
		t.Fatal("PING queued into a full send buffer")
	}
}

// Without an acknowledged packet under the current keys, the update the limit
// forces is refused and the connection fails: the failure the early update
// and its PING prevent.
func TestAEADLimitWithoutAcknowledgedKeysFails(t *testing.T) {
	client, _, now := peerUpdatedPair(t)
	client.sealed[applicationSpace] = clientLimit(t, client)
	client.applicationKeys.elicitedAt = now // no PING this time
	if err := client.enqueue(applicationSpace, wire.PingFrame{}, 1); err != nil {
		t.Fatal(err)
	}
	_, err := client.NextDatagram(now)
	var transport *TransportError
	if !errors.As(err, &transport) || transport.Code != 0xf {
		t.Fatal("want the AEAD limit error", err)
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
	client.sealed[applicationSpace] = threshold(t, client)
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
