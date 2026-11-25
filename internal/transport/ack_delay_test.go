package transport

import (
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
