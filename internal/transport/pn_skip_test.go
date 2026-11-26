package transport

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
)

func sendPing(t *testing.T, c *Conn, now time.Time) {
	t.Helper()
	if err := c.enqueue(applicationSpace, wire.PingFrame{}, 1); err != nil {
		t.Fatal(err)
	}
	if b, err := c.NextDatagram(now); err != nil || b == nil {
		t.Fatal("no PING", err)
	}
}

func isProtocolViolation(err error) bool {
	var te *TransportError
	return errors.As(err, &te) && te.Code == 0xa
}

// A peer that acknowledges a skipped packet number, one no packet carried,
// is acknowledging packets it never received: the connection fails with
// PROTOCOL_VIOLATION. An honest ACK around the gap is accepted.
func TestOptimisticACKFailsConnection(t *testing.T) {
	client, server := testPair(t, wire.Version1, streamParameters())
	drivePair(t, client, server, false, false)
	now := time.Unix(200, 0)
	s := &client.spaces[applicationSpace]
	s.nextPacketNumber()
	s.skip.at = s.nextPN
	first := s.nextPN
	sendPing(t, client, now)
	sendPing(t, client, now)
	if !slices.Contains(client.recovery.Skipped(recovery.Application), first) || s.largestSent != first+2 {
		t.Fatal("skip not taken", first, s.largestSent, client.recovery.Skipped(recovery.Application))
	}
	honest := makePeerPacket(t, server, applicationSpace, 1000, ackFor(first+1, first+2))
	if err := client.HandleDatagram(honest, now); err != nil {
		t.Fatal("honest ACK", err)
	}
	optimistic := makePeerPacket(t, server, applicationSpace, 1001, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: first, Largest: first + 2}}})
	if err := client.HandleDatagram(optimistic, now); !isProtocolViolation(err) {
		t.Fatal("ACK of a skipped number accepted", err)
	}
}

// The same on a multipath path, whose numbers and skips are its own.
func TestOptimisticPathACKFailsConnection(t *testing.T) {
	client, server := sendingPathPair(t, wire.Version1)
	app := client.multipath.paths[1].application
	app.budget.Validate()
	now := time.Unix(101, 0)
	s := app.space
	s.nextPacketNumber()
	s.skip.at = s.nextPN
	first := s.nextPN
	for range 2 {
		if err := client.queueFrameOnPath(1, wire.PingFrame{}); err != nil {
			t.Fatal(err)
		}
		if b, err := client.nextPacketOnPath(1, now); err != nil || b == nil {
			t.Fatal("no PING", err)
		}
	}
	if !slices.Contains(app.sent.Skipped(recovery.Application), first) || s.largestSent != first+2 {
		t.Fatal("skip not taken", first, s.largestSent)
	}
	keys := server.writeKeys(applicationSpace)
	payload, _ := (wire.PathAckFrame{PathID: 1, AckFrame: wire.AckFrame{Ranges: []wire.AckRange{{Smallest: first, Largest: first + 2}}}}).Append(nil)
	if err := client.handleMultipathPacket(sealPathPayload(t, keys, client.config.LocalCID, 0, 100, payload), now); !isProtocolViolation(err) {
		t.Fatal("path ACK of a skipped number accepted", err)
	}
}

// A packet built while a skip is due but then held back, here by the
// congestion window, consumes no number: the skip is recorded only with the
// packet that follows it.
func TestHeldPacketSkipsNothing(t *testing.T) {
	client, server := testPair(t, wire.Version1, streamParameters())
	drivePair(t, client, server, false, false)
	now := time.Unix(200, 0)
	s := &client.spaces[applicationSpace]
	s.nextPacketNumber()
	s.skip.at = s.nextPN
	before := s.nextPN
	flight := client.recovery.BytesInFlight
	client.recovery.BytesInFlight = client.congestion.Window()
	if err := client.enqueue(applicationSpace, wire.PingFrame{}, 1); err != nil {
		t.Fatal(err)
	}
	if b, err := client.NextDatagram(now); err != nil || b != nil {
		t.Fatal("congestion-blocked PING sent", err)
	}
	if s.nextPN != before || len(client.recovery.Skipped(recovery.Application)) != 0 {
		t.Fatal("held packet consumed a number")
	}
	client.recovery.BytesInFlight = flight
	if b, err := client.NextDatagram(now); err != nil || b == nil {
		t.Fatal("no PING", err)
	}
	if s.nextPN != before+2 || s.largestSent != before+1 || !slices.Equal(client.recovery.Skipped(recovery.Application), []uint64{before}) {
		t.Fatal("skip not recorded with the packet", s.nextPN, s.largestSent)
	}
}

// Skips are never adjacent, average about the period apart, and the period
// doubles to its maximum.
func TestSkipSchedule(t *testing.T) {
	var k pnSkipper
	next := uint64(10)
	period := pnSkipInitialPeriod
	for range 20 {
		k.schedule(next)
		if k.at < next+3 || k.at >= next+3+2*period {
			t.Fatal("skip at", k.at, "after", next, "period", period)
		}
		period = min(2*period, pnSkipMaxPeriod)
		if k.period != period {
			t.Fatal("period", k.period, "want", period)
		}
		next = k.at + 1
	}
	if k.period != pnSkipMaxPeriod {
		t.Fatal("period never reached its maximum", k.period)
	}
}
