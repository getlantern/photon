package transport

import (
	"bytes"
	"fmt"
	"net/netip"
	"syscall"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

// ackScheduled reports whether s will acknowledge what it has received, now
// or once its ACK delay passes.
func ackScheduled(s *numberSpace) bool { return s.ackPending || !s.ackDueAt.IsZero() }

// ackNow makes c's delayed ACKs due without moving the test's clock, for tests
// about something other than ACK timing.
func ackNow(c *Conn) { c.expireACKDelays(time.Unix(1<<40, 0)) }

// settledPair returns a confirmed pair whose client has no ACK owed, and the
// next in-order application packet number the server would send it.
func settledPair(t *testing.T, parameters ...wire.TransportParameters) (client, server *Conn, now time.Time, next uint64) {
	t.Helper()
	client, server = testPair(t, wire.Version1, parameters...)
	drivePair(t, client, server, false, false)
	now = time.Unix(200, 0)
	ackNow(client)
	drain(t, client, now)
	largest, ok := client.spaces[applicationSpace].received.Largest()
	if !ok {
		t.Fatal("no application packets after the handshake")
	}
	return client, server, now, largest + 1
}

// framesIn decodes the 1-RTT frames of datagram, sent to to.
func framesIn(t *testing.T, to *Conn, datagram []byte) []wire.Frame {
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

// ackFrameIn reports whether datagram, sent to to, carries an ACK.
func ackFrameIn(t *testing.T, to *Conn, datagram []byte, now time.Time) bool {
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
	for _, f := range frames {
		if _, ok := f.(wire.AckFrame); ok {
			return true
		}
	}
	return false
}

// A lone in-order ack-eliciting packet is acknowledged by its delay deadline,
// which RecoveryDeadline reports and HandleTimeout fires; a second one is
// acknowledged at once.
func TestDelayedACK(t *testing.T) {
	client, server, now, next := settledPair(t)
	if err := client.HandleDatagram(makePeerPacket(t, server, applicationSpace, next, wire.PingFrame{}), now); err != nil {
		t.Fatal(err)
	}
	if out := drain(t, client, now); len(out) != 0 {
		t.Fatal("first in-order packet acknowledged at once")
	}
	due := now.Add(client.ackDelay())
	if got := client.RecoveryDeadline(); !got.Equal(due) {
		t.Fatal("deadline", got, "want", due)
	}
	if err := client.HandleTimeout(due.Add(-time.Nanosecond)); err != nil {
		t.Fatal(err)
	}
	if out := drain(t, client, due.Add(-time.Nanosecond)); len(out) != 0 {
		t.Fatal("ACK sent before its deadline")
	}
	if err := client.HandleTimeout(due); err != nil {
		t.Fatal(err)
	}
	out := drain(t, client, due)
	if len(out) != 1 || !ackFrameIn(t, server, out[0], due) {
		t.Fatal("delayed ACK not sent at its deadline", len(out))
	}
	if !client.ackDeadline().IsZero() {
		t.Fatal("deadline kept after the ACK")
	}
	// Two in-order packets: the second is acknowledged at once.
	for i := uint64(1); i <= 2; i++ {
		if err := client.HandleDatagram(makePeerPacket(t, server, applicationSpace, next+i, wire.PingFrame{}), due); err != nil {
			t.Fatal(err)
		}
	}
	if out := drain(t, client, due); len(out) != 1 || !ackFrameIn(t, server, out[0], due) {
		t.Fatal("second ack-eliciting packet not acknowledged at once", len(out))
	}
}

// Packets RFC 9000 section 13.2.1 says to acknowledge at once are: one after a
// gap or out of order, one marked ECN-CE, and a duplicate, which may mean the
// earlier ACK was lost.
func TestImmediateACK(t *testing.T) {
	for _, tc := range []struct {
		name string
		send func(t *testing.T, client, server *Conn, now time.Time, next uint64) error
	}{
		{"gap", func(t *testing.T, client, server *Conn, now time.Time, next uint64) error {
			return client.HandleDatagram(makePeerPacket(t, server, applicationSpace, next+1, wire.PingFrame{}), now)
		}},
		{"ecn-ce", func(t *testing.T, client, server *Conn, now time.Time, next uint64) error {
			return client.HandleDatagramECN(makePeerPacket(t, server, applicationSpace, next, wire.PingFrame{}), now, ECNCE)
		}},
		{"duplicate", func(t *testing.T, client, server *Conn, now time.Time, next uint64) error {
			return client.HandleDatagram(makePeerPacket(t, server, applicationSpace, next-1, wire.PingFrame{}), now)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, server, now, next := settledPair(t)
			if err := tc.send(t, client, server, now, next); err != nil {
				t.Fatal(err)
			}
			if out := drain(t, client, now); len(out) != 1 || !ackFrameIn(t, server, out[0], now) {
				t.Fatal("not acknowledged at once", len(out))
			}
		})
	}
}

// A packet sent at or after a delayed ACK's deadline carries it, even if the
// owner did not run HandleTimeout first.
func TestDelayedACKSentWithLateSend(t *testing.T) {
	client, server, now, next := settledPair(t)
	if err := client.HandleDatagram(makePeerPacket(t, server, applicationSpace, next, wire.PingFrame{}), now); err != nil {
		t.Fatal(err)
	}
	late := now.Add(client.ackDelay())
	if out := drain(t, client, late); len(out) != 1 || !ackFrameIn(t, server, out[0], late) {
		t.Fatal("send after the deadline omitted the ACK", len(out))
	}
}

// Advertising a max_ack_delay of zero acknowledges every packet at once.
func TestZeroMaxACKDelayAcknowledgesAtOnce(t *testing.T) {
	parameters := wire.DefaultTransportParameters()
	parameters.MaxAckDelay = 0
	client, server, now, next := settledPair(t, parameters)
	if err := client.HandleDatagram(makePeerPacket(t, server, applicationSpace, next, wire.PingFrame{}), now); err != nil {
		t.Fatal(err)
	}
	if out := drain(t, client, now); len(out) != 1 || !ackFrameIn(t, server, out[0], now) {
		t.Fatal("zero max_ack_delay delayed the ACK", len(out))
	}
}

// HandleTimeout makes a due delayed ACK pending even if no packet is sent, so
// a past deadline leaves RecoveryDeadline and the owner does not spin on it.
func TestDelayedACKDeadlineClearsWithoutSend(t *testing.T) {
	client, server, now, next := settledPair(t)
	if err := client.HandleDatagram(makePeerPacket(t, server, applicationSpace, next, wire.PingFrame{}), now); err != nil {
		t.Fatal(err)
	}
	due := client.RecoveryDeadline()
	if err := client.HandleTimeout(due); err != nil {
		t.Fatal(err)
	}
	if !client.ackDeadline().IsZero() || !client.spaces[applicationSpace].ackPending {
		t.Fatal("due ACK deadline left armed", client.ackDeadline())
	}
	if got := client.RecoveryDeadline(); !got.IsZero() && !got.After(due) {
		t.Fatal("RecoveryDeadline still reports the past ACK deadline", got)
	}
}

// An ACK owed but not yet due rides on the next data packet, ahead of the
// data, and is then no longer owed.
func TestACKBundledWithData(t *testing.T) {
	client, server, now, next := settledPair(t, streamParameters())
	if err := client.HandleDatagram(makePeerPacket(t, server, applicationSpace, next, wire.PingFrame{}), now); err != nil {
		t.Fatal(err)
	}
	id, err := client.OpenStream(false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.WriteStream(id, []byte("data"), true); err != nil {
		t.Fatal(err)
	}
	out := drain(t, client, now)
	if len(out) != 1 {
		t.Fatal("datagrams", len(out))
	}
	frames := framesIn(t, server, out[0])
	if len(frames) != 2 {
		t.Fatalf("frames %T", frames)
	}
	if _, ok := frames[0].(wire.AckFrame); !ok {
		t.Fatalf("first frame %T, want the owed ACK", frames[0])
	}
	if f, ok := frames[1].(wire.StreamFrame); !ok || string(f.Data) != "data" {
		t.Fatalf("second frame %T, want the data", frames[1])
	}
	if !client.ackDeadline().IsZero() || ackScheduled(&client.spaces[applicationSpace]) {
		t.Fatal("ACK still owed after it was bundled")
	}
}

// An owed ACK too large to fit beside a full-size stream frame is not
// bundled: the frame goes out whole and the ACK stays owed.
func TestACKTooLargeToBundleStaysOwed(t *testing.T) {
	client, server, now, next := settledPair(t, streamParameters())
	// A history of gaps makes the owed ACK too large to fit beside a
	// full-size stream frame.
	const gaps = 40
	for i := uint64(0); i < gaps; i++ {
		if err := client.HandleDatagram(makePeerPacket(t, server, applicationSpace, next+2*i, wire.PingFrame{}), now); err != nil {
			t.Fatal(err)
		}
	}
	ackNow(client)
	drain(t, client, now)
	// One in-order packet leaves an ACK owed but not yet due.
	if err := client.HandleDatagram(makePeerPacket(t, server, applicationSpace, next+2*gaps-1, wire.PingFrame{}), now); err != nil {
		t.Fatal(err)
	}
	if client.spaces[applicationSpace].ackPending || client.ackDeadline().IsZero() {
		t.Fatal("setup: want an ACK owed but not due")
	}
	id, err := client.OpenStream(false)
	if err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 4000)
	for i := range payload {
		payload[i] = byte(i * 7)
	}
	writeAll(t, client, id, payload)
	queued := len(client.spaces[applicationSpace].queue)
	b, err := client.NextDatagram(now)
	if err != nil || b == nil {
		t.Fatal(err)
	}
	frames := framesIn(t, server, b)
	if len(frames) != 1 {
		t.Fatalf("frames %d, want the stream frame alone", len(frames))
	}
	if _, ok := frames[0].(wire.StreamFrame); !ok {
		t.Fatalf("frame %T, want the stream frame", frames[0])
	}
	if got := len(client.spaces[applicationSpace].queue); got != queued-1 {
		t.Fatal("queue split for the ACK", queued, got)
	}
	if !ackScheduled(&client.spaces[applicationSpace]) {
		t.Fatal("ACK no longer owed")
	}
}

// A bundled ACK must not hold back a packet that could leave without it: with
// exactly enough congestion window for the data alone, the data goes out
// alone and the ACK stays owed.
func TestACKNotBundledPastCongestionWindow(t *testing.T) {
	client, server, now, next := settledPair(t, streamParameters())
	if err := client.HandleDatagram(makePeerPacket(t, server, applicationSpace, next, wire.PingFrame{}), now); err != nil {
		t.Fatal(err)
	}
	id, err := client.OpenStream(false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.WriteStream(id, []byte("data"), true); err != nil {
		t.Fatal(err)
	}
	frame, err := client.spaces[applicationSpace].queue[0].frame.Append(nil)
	if err != nil {
		t.Fatal(err)
	}
	alone := 1 + len(client.peerCID) + 4 + len(frame) + 16
	client.recovery.BytesInFlight = client.congestion.Window() - uint64(alone)
	b, err := client.NextDatagram(now)
	if err != nil || b == nil {
		t.Fatal("data held back by the bundled ACK", err)
	}
	frames := framesIn(t, server, b)
	if len(frames) != 1 {
		t.Fatalf("frames %d, want the stream frame alone", len(frames))
	}
	if _, ok := frames[0].(wire.StreamFrame); !ok {
		t.Fatalf("frame %T, want the stream frame", frames[0])
	}
	if !ackScheduled(&client.spaces[applicationSpace]) {
		t.Fatal("ACK no longer owed")
	}
}

// A bundled ACK on a large base-path packet the OS rejects with EMSGSIZE never
// left: the rollback must owe it again, now, and restore the key-phase ackSent
// it may have set, for both STREAM and DATAGRAM packets.
func TestBundledACKRestoredOnLargeWriteRejection(t *testing.T) {
	for _, datagram := range []bool{false, true} {
		t.Run(fmt.Sprintf("datagram=%t", datagram), func(t *testing.T) { testBundledACKRestored(t, datagram, false) })
	}
	// If the key phase moved on while the write was pending, its ackSent
	// belongs to the new phase and is left alone.
	t.Run("phase-moved", func(t *testing.T) { testBundledACKRestored(t, false, true) })
}

func testBundledACKRestored(t *testing.T, datagram, phaseMoved bool) {
	p := streamParameters()
	p.MaxDatagramFrameSize = 65535
	c, peer := testPair(t, wire.Version1, p)
	c.config.InitialPacketSize = 1452
	drivePair(t, c, peer, false, false)
	now := time.Unix(101, 0)
	ackNow(c)
	drain(t, c, now)
	largest, _ := c.spaces[applicationSpace].received.Largest()
	if err := c.HandleDatagram(makePeerPacket(t, peer, applicationSpace, largest+1, wire.PingFrame{}), now); err != nil {
		t.Fatal(err)
	}
	e, err := NewClientEndpoint(c, netip.MustParseAddrPort("127.0.0.1:443"))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	payload := bytes.Repeat([]byte{43}, 1252)
	if datagram {
		err = c.SendDatagram(payload)
	} else {
		var id uint64
		if id, err = c.OpenStream(false); err == nil {
			_, err = c.WriteStream(id, payload, true)
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	ackSent := c.applicationKeys.ackSent
	out, err := e.NextDatagram(now)
	if err != nil || out == nil || out.baseLarge == nil || !out.baseLarge.ack {
		t.Fatal("want a large base packet carrying the ACK", err)
	}
	if ackScheduled(&c.spaces[applicationSpace]) {
		t.Fatal("setup: ACK still owed after bundling")
	}
	if ackSent {
		t.Fatal("setup: want ackSent false before the bundled ACK")
	}
	c.applicationKeys.ackSent = true // as if the bundled ACK had set it
	if phaseMoved {
		c.advanceKeyNumbers() // what a key update does
	}
	e.HandleWriteError(out, syscall.EMSGSIZE, now)
	if c.closed {
		t.Fatal("rejection closed the connection")
	}
	if !c.spaces[applicationSpace].ackPending {
		t.Fatal("rejected packet's ACK not owed again")
	}
	if got, want := c.applicationKeys.ackSent, phaseMoved; got != want {
		t.Fatal("key-phase ackSent", got, "want", want)
	}
}

// owePathACK has from send to on path 1 an ack-eliciting packet, which leaves
// on owing a delayed PATH_ACK on that path.
func owePathACK(t *testing.T, from, on *Conn, now time.Time) {
	t.Helper()
	from.multipath.paths[1].application.budget.Validate()
	if err := from.queueFrameOnPath(1, wire.PingFrame{}); err != nil {
		t.Fatal(err)
	}
	raw, err := from.nextPacketOnPath(1, now)
	if err != nil || raw == nil {
		t.Fatal("peer packet on path 1", err)
	}
	if err := on.handleMultipathPacket(raw, now); err != nil {
		t.Fatal(err)
	}
	if s := on.multipath.paths[1].application.space; s.ackPending || s.ackDueAt.IsZero() {
		t.Fatal("setup: want a delayed PATH_ACK owed on path 1")
	}
}

// On a multipath path, an owed PATH_ACK rides ahead of the path's data, and
// is then no longer owed.
func TestMultipathACKBundledWithData(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(vName(v), func(t *testing.T) {
			client, server := sendingPathPair(t, v)
			now := time.Unix(101, 0)
			owePathACK(t, server, client, now)
			client.multipath.paths[1].application.budget.Validate()
			if err := client.queueFrameOnPath(1, wire.PingFrame{}); err != nil {
				t.Fatal(err)
			}
			raw, err := client.nextPacketOnPath(1, now)
			if err != nil || raw == nil {
				t.Fatal("data packet", err)
			}
			if ackScheduled(client.multipath.paths[1].application.space) {
				t.Fatal("PATH_ACK still owed after it was bundled")
			}
			if err := server.handleMultipathPacket(raw, now); err != nil {
				t.Fatal(err)
			}
			if server.multipath.paths[1].application.sent.BytesInFlight != 0 {
				t.Fatal("the data packet carried no PATH_ACK")
			}
			if !ackScheduled(server.multipath.paths[1].application.space) {
				t.Fatal("the bundled packet was not ack-eliciting")
			}
		})
	}
}

// A multipath packet whose write is rejected as too large owes its bundled
// PATH_ACK again, for STREAM data and for a DATAGRAM.
func TestMultipathBundledACKRestoredOnLargeWriteRejection(t *testing.T) {
	for _, datagram := range []bool{false, true} {
		t.Run(fmt.Sprintf("datagram=%t", datagram), func(t *testing.T) { testMultipathBundledACKRestored(t, datagram, false) })
	}
	// A key update while the write was pending leaves the new phase's ackSent
	// alone, even though path 0, abandoned here, sent nothing in between.
	t.Run("phase-moved", func(t *testing.T) { testMultipathBundledACKRestored(t, false, true) })
}

func testMultipathBundledACKRestored(t *testing.T, datagram, phaseMoved bool) {
	e, c, remote := mtuWriteEndpointPair(t, wire.Version1, 1500)
	defer e.Close()
	app := c.multipath.paths[1].application
	now := time.Unix(101, 0)
	owePathACK(t, remote, c, now)
	if datagram {
		c.peerParams.MaxDatagramFrameSize = 65535
		c.config.InitialPacketSize = 1452
		c.config.enablePathMTUDiscovery = true
		c.seedInitialPathMTU(app)
		if err := c.SendDatagram(make([]byte, 1300)); err != nil {
			t.Fatal(err)
		}
	} else {
		app.mtu.confirmed, app.mtu.upper = 3000, 3000
		id, err := c.OpenStream(false)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.WriteStream(id, make([]byte, 2500), true); err != nil {
			t.Fatal(err)
		}
	}
	// An earlier key update, after which path 0 sends nothing.
	c.advanceKeyNumbers()
	ackSent := c.applicationKeys.ackSent
	out, err := e.NextDatagram(now)
	if err != nil || out == nil || len(out.Data) <= 1200 {
		t.Fatal("large output", err)
	}
	probe := out.datagram
	if !datagram && out.large != nil {
		probe = &out.large.outboundMTUProbe
	}
	if probe == nil || !probe.ack {
		t.Fatal("want a large packet carrying the PATH_ACK")
	}
	if ackScheduled(app.space) {
		t.Fatal("setup: PATH_ACK still owed after bundling")
	}
	if ackSent {
		t.Fatal("setup: want ackSent false before the bundled ACK")
	}
	c.applicationKeys.ackSent = true // as if the bundled ACK had set it
	if phaseMoved {
		c.advanceKeyNumbers() // what a key update does
	}
	e.HandleWriteError(out, fmt.Errorf("socket: %w", syscall.EMSGSIZE), now)
	if c.closed {
		t.Fatal("rejection closed the connection")
	}
	if !app.space.ackPending {
		t.Fatal("rejected packet's PATH_ACK not owed again")
	}
	if got, want := c.applicationKeys.ackSent, phaseMoved; got != want {
		t.Fatal("key-phase ackSent", got, "want", want)
	}
}

// With exactly enough congestion window for a path's data alone, the data
// goes out without the PATH_ACK, which stays owed.
func TestMultipathACKNotBundledPastCongestionWindow(t *testing.T) {
	client, server := sendingPathPair(t, wire.Version1)
	now := time.Unix(101, 0)
	owePathACK(t, server, client, now)
	app := client.multipath.paths[1].application
	app.budget.Validate()
	if err := client.queueFrameOnPath(1, wire.PingFrame{}); err != nil {
		t.Fatal(err)
	}
	peer := client.multipath.paths[1].peer
	alone := 1 + len(peer.active[peer.current].cid) + 4 + 1 + 16
	app.sent.BytesInFlight = app.congestion.Window() - uint64(alone)
	raw, err := client.nextPacketOnPath(1, now)
	if err != nil || raw == nil {
		t.Fatal("data held back by the bundled PATH_ACK", err)
	}
	if !ackScheduled(app.space) {
		t.Fatal("PATH_ACK no longer owed")
	}
}

// newTokenOfPacketSize is a NEW_TOKEN frame that fills a 1-RTT packet to
// size bytes on its own, for a peer CID of cidLen bytes.
func newTokenOfPacketSize(size, cidLen int) wire.NewTokenFrame {
	frame := size - (1 + cidLen + 4 + 16)
	return wire.NewTokenFrame{Token: bytes.Repeat([]byte{7}, frame-3)} // type, 2-byte length
}

// An owed ACK is not bundled onto a frame other than STREAM or DATAGRAM if the
// packet would then exceed the minimum QUIC MTU: only those two can be rolled
// back if the write is rejected as too large.
func TestControlFrameBundleStaysWithinMinimumMTU(t *testing.T) {
	client, server := testPair(t, wire.Version1, streamParameters())
	server.config.InitialPacketSize = 1452
	drivePair(t, client, server, false, false)
	now := time.Unix(200, 0)
	ackNow(server)
	drain(t, server, now)
	next := client.spaces[applicationSpace].nextPN + 10
	// A history of gaps gives the owed ACK some size; the last packet arrives
	// in order, so the ACK is owed but not due.
	for i := uint64(0); i < 20; i++ {
		if err := server.HandleDatagram(makePeerPacket(t, client, applicationSpace, next+2*i, wire.PingFrame{}), now); err != nil {
			t.Fatal(err)
		}
	}
	ackNow(server)
	drain(t, server, now)
	if err := server.HandleDatagram(makePeerPacket(t, client, applicationSpace, next+39, wire.PingFrame{}), now); err != nil {
		t.Fatal(err)
	}
	s := &server.spaces[applicationSpace]
	if s.ackPending || s.ackDueAt.IsZero() {
		t.Fatal("setup: want an ACK owed but not due")
	}
	f := newTokenOfPacketSize(minimumQUICMTU-8, len(server.peerCID))
	if err := server.enqueue(applicationSpace, f, len(f.Token)); err != nil {
		t.Fatal(err)
	}
	out := drain(t, server, now)
	if len(out) == 0 || len(out[0]) > minimumQUICMTU {
		t.Fatal("NEW_TOKEN packet grew past the minimum QUIC MTU", len(out))
	}
	if !ackScheduled(s) {
		t.Fatal("ACK no longer owed")
	}
}

// The same holds on a multipath path whose confirmed MTU allows more.
func TestMultipathControlFrameBundleStaysWithinMinimumMTU(t *testing.T) {
	client, server := sendingPathPair(t, wire.Version1)
	now := time.Unix(101, 0)
	owePathACK(t, client, server, now)
	app := server.multipath.paths[1].application
	app.budget.Validate()
	app.validation.state = PathValidated
	if err := server.startPathMTUSearch(1, 1500); err != nil {
		t.Fatal(err)
	}
	app.mtu.confirmed = 1452
	peer := server.multipath.paths[1].peer
	f := newTokenOfPacketSize(minimumQUICMTU-2, len(peer.active[peer.current].cid))
	if err := server.queueFrameOnPath(1, f); err != nil {
		t.Fatal(err)
	}
	raw, err := server.nextPacketOnPath(1, now)
	if err != nil || raw == nil {
		t.Fatal("NEW_TOKEN packet", err)
	}
	if len(raw) > minimumQUICMTU {
		t.Fatal("NEW_TOKEN packet grew past the minimum QUIC MTU", len(raw))
	}
	if !ackScheduled(app.space) {
		t.Fatal("PATH_ACK no longer owed")
	}
}

func TestBundledSizeLimit(t *testing.T) {
	stream, datagram, token := wire.StreamFrame{}, wire.DatagramFrame{}, wire.NewTokenFrame{}
	for _, c := range []struct {
		name     string
		f        wire.Frame
		alone    int
		rollback bool
		want     int
	}{
		{"stream", stream, 1000, true, 1452},
		{"stream without rollback", stream, 1000, false, minimumQUICMTU},
		{"datagram", datagram, 1000, true, minimumQUICMTU},
		{"control", token, 1000, true, minimumQUICMTU},
		{"datagram already large", datagram, 1300, true, 1452},
		{"control already large", token, 1300, false, 1452},
	} {
		if got := bundledSizeLimit(c.f, 1452, c.alone, c.rollback); got != c.want {
			t.Errorf("%s: limit %d, want %d", c.name, got, c.want)
		}
	}
}

// A DATAGRAM that fits the minimum QUIC MTU alone does not carry an ACK that
// would take it past: a rejected DATAGRAM is dropped, not retransmitted.
func TestMultipathDatagramBundleStaysWithinMinimumMTU(t *testing.T) {
	client, server := sendingPathPair(t, wire.Version1)
	now := time.Unix(101, 0)
	owePathACK(t, client, server, now)
	app := server.multipath.paths[1].application
	app.budget.Validate()
	app.validation.state = PathValidated
	if err := server.startPathMTUSearch(1, 1500); err != nil {
		t.Fatal(err)
	}
	app.mtu.confirmed = 1452
	peer := server.multipath.paths[1].peer
	// Alone, the packet is 2 bytes short of the minimum MTU: header, the type
	// byte of a DATAGRAM without a length, payload, AEAD tag.
	payload := minimumQUICMTU - 2 - (1 + len(peer.active[peer.current].cid) + 4 + 16) - 1
	if err := server.queueFrameOnPath(1, wire.DatagramFrame{Data: make([]byte, payload), OmitLength: true}); err != nil {
		t.Fatal(err)
	}
	server.pendingDatagrams++ // as SendDatagram counts it
	raw, err := server.nextPacketOnPath(1, now)
	if err != nil || raw == nil {
		t.Fatal("DATAGRAM packet", err)
	}
	if len(raw) > minimumQUICMTU {
		t.Fatal("DATAGRAM packet grew past the minimum QUIC MTU", len(raw))
	}
	if !ackScheduled(app.space) {
		t.Fatal("PATH_ACK no longer owed")
	}
}

// A bundled PATH_ACK counts as an ACK sent in the current key phase, which
// the peer's next key update requires.
func TestMultipathBundledACKMarksKeyPhaseACKSent(t *testing.T) {
	client, server := sendingPathPair(t, wire.Version1)
	now := time.Unix(101, 0)
	owePathACK(t, server, client, now)
	// As after a peer key update whose first packet on path 1 came before the
	// one now owed an ACK.
	k := &client.applicationKeys
	k.updated, k.ackSent, k.awaitingRead = true, false, false
	numbers := client.keyNumbers(1)
	numbers.haveFirst, numbers.first = true, 0
	client.multipath.paths[1].application.budget.Validate()
	if err := client.queueFrameOnPath(1, wire.PingFrame{}); err != nil {
		t.Fatal(err)
	}
	if raw, err := client.nextPacketOnPath(1, now); err != nil || raw == nil {
		t.Fatal("data packet", err)
	}
	if ackScheduled(client.multipath.paths[1].application.space) {
		t.Fatal("setup: PATH_ACK not bundled")
	}
	if !k.ackSent {
		t.Fatal("bundled PATH_ACK did not mark an ACK sent in this key phase")
	}
}
