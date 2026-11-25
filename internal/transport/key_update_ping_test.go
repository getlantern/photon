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

// pingQueued reports whether a PING is queued in the application space, on
// any path.
func (c *Conn) pingQueued() bool {
	if pingIn(c.spaces[applicationSpace].queue) {
		return true
	}
	if c.multipath != nil {
		for _, state := range c.multipath.paths {
			if app := state.application; app != nil && pingIn(app.space.queue) {
				return true
			}
		}
	}
	return false
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
	frames := framesIn(t, server, out[0])
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
	client, server, now := peerUpdatedPair(t)
	client.sealed[applicationSpace] = threshold(t, client)
	if out := drain(t, client, now); len(out) != 1 {
		t.Fatal("first PING", len(out))
	}
	pto := client.recovery.RTT.PTO(true, true, client.keepAliveACKDelay(), 0)
	if out := drain(t, client, now.Add(pto-time.Millisecond)); len(out) != 0 {
		t.Fatal("PING repeated before a PTO", len(out))
	}
	if client.pingQueued() {
		t.Fatal("setup: a PING still queued")
	}
	out := drain(t, client, now.Add(pto))
	sawPING := false
	for _, b := range out {
		for _, f := range framesIn(t, server, b) {
			if _, ok := f.(wire.PingFrame); ok {
				sawPING = true
			}
		}
	}
	if !sawPING {
		t.Fatal("no replacement PING sent after a PTO", len(out))
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

// A key update needs no room in a full send buffer when ack-eliciting data
// is already queued: that data, sent with the new keys, does the PING's job.
func TestKeyUpdateWithFullSendBufferAndQueuedData(t *testing.T) {
	client, server := testPair(t, wire.Version1, streamParameters())
	drivePair(t, client, server, false, false)
	now := time.Unix(200, 0)
	if err := client.enqueue(applicationSpace, wire.PingFrame{}, 1); err != nil {
		t.Fatal(err)
	}
	queued := len(client.spaces[applicationSpace].queue)
	client.queuedBytes = maxSendBytes
	if err := client.InitiateKeyUpdateAt(now); err != nil {
		t.Fatal("update refused with queued data and a full buffer", err)
	}
	if len(client.spaces[applicationSpace].queue) != queued {
		t.Fatal("PING queued although data was already queued")
	}
}

// On multipath a PING costs at least 32 bytes of send buffer; with less free,
// none is queued and the once-per-PTO pacing does not start.
func TestMultipathKeyUpdatePINGNeedsRoom(t *testing.T) {
	client, server := sendingPathPair(t, wire.Version1)
	now := time.Unix(101, 0)
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
	client.sealed[applicationSpace] = threshold(t, client)
	client.queuedBytes = maxSendBytes - client.reservedResetBytes - 31
	if err := client.updateKeysBeforeLimit(now); err != nil {
		t.Fatal(err)
	}
	if client.pingQueued() || !client.applicationKeys.elicitedAt.IsZero() {
		t.Fatal("PING queued, or pacing started, without room for it")
	}
}

// With the update refused, the last packet the keys allow still goes out
// with them, counted exactly at the limit, and the next send fails with the
// AEAD limit error rather than reuse the keys.
func TestAEADLimitBoundaryWhenUpdateRefused(t *testing.T) {
	client, _, now := peerUpdatedPair(t)
	keys := client.writeKeys(applicationSpace)
	limit := clientLimit(t, client)
	client.sealed[applicationSpace] = limit - 1
	client.applicationKeys.elicitedAt = now // no PING: keep the update refused
	for range 2 {
		if err := client.enqueue(applicationSpace, wire.PingFrame{}, 1); err != nil {
			t.Fatal(err)
		}
	}
	if b, err := client.NextDatagram(now); err != nil || len(b) == 0 {
		t.Fatal("last allowed packet", err)
	}
	if client.sealed[applicationSpace] != limit || client.writeKeys(applicationSpace) != keys {
		t.Fatal("last allowed packet not counted", client.sealed[applicationSpace])
	}
	_, err := client.NextDatagram(now)
	var transport *TransportError
	if !errors.As(err, &transport) || transport.Code != 0xf {
		t.Fatal("want the AEAD limit error at the limit", err)
	}
}

// A PING queued on another path, which may be held there, does not stand in
// for the one on the path keepalive selection picks.
func TestMultipathKeyUpdatePINGIgnoresOtherPathQueue(t *testing.T) {
	client, server := sendingPathPair(t, wire.Version1)
	now := time.Unix(101, 0)
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
	client.multipath.paths[1].application.budget.Validate()
	selected, _, found := client.keepAlivePath(client.keepAliveACKDelay())
	if !found {
		t.Fatal("setup: no keepalive path")
	}
	other := 1 - selected
	if err := client.queueFrameOnPath(other, wire.PingFrame{}); err != nil {
		t.Fatal(err)
	}
	client.sealed[applicationSpace] = threshold(t, client)
	if err := client.updateKeysBeforeLimit(now); err != nil {
		t.Fatal(err)
	}
	if !pingIn(client.multipath.paths[selected].application.space.queue) {
		t.Fatal("PING on another path suppressed the one on the selected path")
	}
}

// A CID advertisement held while forwarding is blocked is not data sure to be
// sent, so a key update still queues its PING.
func TestKeyUpdatePINGNotReplacedByHeldCIDAdvertisement(t *testing.T) {
	client, server := testPair(t, wire.Version1, streamParameters())
	drivePair(t, client, server, false, false)
	s := &client.spaces[applicationSpace]
	s.queue = append(s.queue, &sendItem{frame: wire.NewConnectionIDFrame{Sequence: 9, ConnectionID: []byte("heldcid0")}, bytes: 32, queued: true})
	client.forwardingBlocked = true
	if client.applicationDataQueued() {
		t.Fatal("held CID advertisement counted as data sure to be sent")
	}
	if err := client.InitiateKeyUpdateAt(time.Unix(200, 0)); err != nil {
		t.Fatal(err)
	}
	if !pingIn(s.queue) {
		t.Fatal("key update queued no PING beside a held CID advertisement")
	}
}
