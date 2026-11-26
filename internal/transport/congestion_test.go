package transport

import (
	"testing"
	"time"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
)

func TestEngineCongestionWindowAndPTO(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(vName(v), func(t *testing.T) {
			client, _ := testPair(t, v)
			now := time.Unix(100, 0)
			for i := 0; i < 30; i++ {
				if err := client.enqueue(initialSpace, wire.PingFrame{}, 1); err != nil {
					t.Fatal(err)
				}
			}
			flight := drain(t, client, now)
			if len(flight) != 10 {
				t.Fatal("initial window packet count", len(flight))
			}
			window, bytes := client.CongestionStats()
			if window != 12000 || bytes != 12000 {
				t.Fatal(window, bytes)
			}
			// A pending padded Initial ACK must not hide the PTO probe behind a
			// congestion-blocked packet. Unlike pure ACK packets, padding is in flight.
			client.spaces[initialSpace].received.Record(0, now)
			client.spaces[initialSpace].ackPending = true
			deadline := client.RecoveryDeadline()
			if err := client.HandleTimeout(deadline); err != nil {
				t.Fatal(err)
			}
			probes := drain(t, client, deadline)
			if len(probes) != 1 {
				t.Fatal("PTO allowance must permit exactly one packet", len(probes))
			}
			window, bytes = client.CongestionStats()
			if window != 12000 || bytes != 13200 {
				t.Fatal("PTO changed window or missed accounting", window, bytes)
			}
			if err := client.handleAck(initialSpace, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 0, Largest: 10}}}, deadline.Add(10*time.Millisecond)); err != nil {
				t.Fatal(err)
			}
			if _, bytes = client.CongestionStats(); bytes != 0 {
				t.Fatal("ACK did not release flight", bytes)
			}
			if b, err := client.NextDatagram(deadline.Add(10 * time.Millisecond)); err != nil || len(b) == 0 {
				t.Fatal("ACK did not unblock sender", err)
			}
		})
	}
}

func TestEnginePureACKBypassesCongestion(t *testing.T) {
	client, server := testPair(t, wire.Version1)
	drivePair(t, client, server, false, false)
	now := time.Unix(200, 0)
	for i := 0; i < 1000; i++ {
		if err := client.enqueue(applicationSpace, wire.PingFrame{}, 1); err != nil {
			t.Fatal(err)
		}
	}
	// There are many small short-header packets; stop at the controller gate.
	for i := 0; i < 2000; i++ {
		b, err := client.NextDatagram(now)
		if err != nil {
			t.Fatal(err)
		}
		if len(b) == 0 {
			break
		}
		if i == 1999 {
			t.Fatal("window never blocked")
		}
	}
	_, before := client.CongestionStats()
	s := &client.spaces[applicationSpace]
	s.received.Record(100, now)
	s.ackPending = true
	b, err := client.NextDatagram(now)
	if err != nil || len(b) == 0 {
		t.Fatal("congestion blocked pure ACK", err)
	}
	if _, after := client.CongestionStats(); after != before {
		t.Fatal("pure ACK increased flight", before, after)
	}
}

func TestPersistentCongestionRefreshesPathRTT(t *testing.T) {
	client, server := testPair(t, wire.Version1)
	drivePair(t, client, server, false, false)
	now := time.Unix(200, 0)
	// Use a fresh path ledger with an established RTT sample.
	ledger, _ := recovery.NewSent(20)
	ledger.RTT.Update(10*time.Millisecond, 0, 0, true)
	client.recovery = ledger
	for i := 0; i < 4; i++ {
		if err := ledger.OnSent(recovery.Application, recovery.Packet{Number: uint64(i), SentAt: now.Add(time.Duration(i) * time.Second), Bytes: 1200, InFlight: true, AckEliciting: true}); err != nil {
			t.Fatal(err)
		}
	}
	ev, err := ledger.OnACK(recovery.Application, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 3, Largest: 3}}}, now.Add(3*time.Second+10*time.Millisecond), 0, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	client.onCongestionLoss(ev.Lost, now.Add(4*time.Second))
	if client.congestion.Window() != 2400 {
		t.Fatal("window not collapsed")
	}
	ledger.RTT.Update(100*time.Millisecond, 0, 0, true)
	if ledger.RTT.Minimum != 100*time.Millisecond {
		t.Fatal("path minimum not refreshed")
	}
}

// recordingController records each ACK event its wrapped controller receives.
type recordingController struct {
	recovery.Controller
	events []recovery.ACKEvent
}

func (r *recordingController) OnACK(e recovery.ACKEvent) {
	r.events = append(r.events, e)
	r.Controller.OnACK(e)
}

// lastValidRate is the last valid rate sample the controller received.
func (r *recordingController) lastValidRate() (recovery.ACKEvent, bool) {
	for i := len(r.events) - 1; i >= 0; i-- {
		if r.events[i].Rate.Valid {
			return r.events[i], true
		}
	}
	return recovery.ACKEvent{}, false
}

// The congestion controller receives each ACK's delivery-rate sample. Data
// that crosses a 30 ms round trip and is then acknowledged measures its own
// delivery, application limited once nothing is left to send.
func TestControllerReceivesRateSamples(t *testing.T) {
	client, server, now, _ := settledPair(t, streamParameters())
	recorder := &recordingController{Controller: client.congestion}
	client.congestion = recorder
	id, err := client.OpenStream(false)
	if err != nil {
		t.Fatal(err)
	}
	// WriteStream admits a frame at a time.
	for data := make([]byte, 6000); len(data) > 0; {
		n, err := client.WriteStream(id, data, true)
		if err != nil {
			t.Fatal(err)
		}
		data = data[n:]
	}
	for _, d := range drain(t, client, now) {
		if err := server.HandleDatagram(d, now); err != nil {
			t.Fatal(err)
		}
	}
	ackNow(server)
	later := now.Add(30 * time.Millisecond)
	for _, d := range drain(t, server, now) {
		if err := client.HandleDatagram(d, later); err != nil {
			t.Fatal(err)
		}
	}
	e, ok := recorder.lastValidRate()
	if !ok {
		t.Fatal("no valid rate sample reached the controller", len(recorder.events))
	}
	if e.Rate.Delivered < 6000 || e.Rate.Interval != 30*time.Millisecond || e.Rate.Rate == 0 || !e.Now.Equal(later) {
		t.Fatalf("sample %+v, want the 6000 bytes over 30ms", e.Rate)
	}
	if !e.Limited {
		t.Fatal("ACK with nothing left to send not reported as limited")
	}
}

// On a multipath connection each path's controller receives its own samples.
func TestMultipathControllerReceivesRateSamples(t *testing.T) {
	client, server := sendingPathPair(t, wire.Version1)
	now := time.Unix(101, 0)
	app := client.multipath.paths[1].application
	app.budget.Validate()
	app.validation.state = PathValidated
	recorder := &recordingController{Controller: app.congestion}
	app.congestion = recorder
	id, err := client.OpenStream(false)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.queueFrameOnPath(1, wire.StreamFrame{StreamID: id, Data: make([]byte, 1000)}); err != nil {
		t.Fatal(err)
	}
	raw, err := client.nextPacketOnPath(1, now)
	if err != nil || raw == nil {
		t.Fatal("stream packet", err)
	}
	if err := server.handleMultipathPacket(raw, now); err != nil {
		t.Fatal(err)
	}
	ackNow(server)
	server.multipath.paths[1].application.budget.Validate()
	ack, err := server.nextPacketOnPath(1, now)
	if err != nil || ack == nil {
		t.Fatal("PATH_ACK", err)
	}
	later := now.Add(30 * time.Millisecond)
	if err := client.handleMultipathPacket(ack, later); err != nil {
		t.Fatal(err)
	}
	e, ok := recorder.lastValidRate()
	if !ok || e.Rate.Interval != 30*time.Millisecond || e.Rate.Delivered == 0 || !e.Limited {
		t.Fatalf("path 1 sample %+v (limited %t), valid %t", e.Rate, e.Limited, ok)
	}
}

// A send pass that finds nothing to send while the window has room marks the
// path application limited, without waiting for an ACK to notice.
func TestIdleSendMarksAppLimited(t *testing.T) {
	client, _, now, _ := settledPair(t, streamParameters())
	drain(t, client, now)
	fresh, _ := recovery.NewSent(maxSentPackets)
	client.recovery = fresh
	if client.recovery.AppLimited() {
		t.Fatal("setup: fresh path already app limited")
	}
	if b, err := client.NextDatagram(now); err != nil || b != nil {
		t.Fatal("setup: idle connection sent", len(b), err)
	}
	if !client.recovery.AppLimited() {
		t.Fatal("idle send pass did not mark the path app limited")
	}
}

func TestMultipathIdleSendMarksAppLimited(t *testing.T) {
	e, client, _ := mtuWriteEndpointPair(t, wire.Version1, 1500)
	defer e.Close()
	now := time.Unix(101, 0)
	for client.nextBoundOrNil(t, now) {
	}
	app := client.multipath.paths[1].application
	app.sent, _ = recovery.NewSent(maxSentPackets)
	if _, _, b, err := client.nextBoundMultipathPacket(now); err != nil || b != nil {
		t.Fatal("setup: idle connection sent", len(b), err)
	}
	if !app.sent.AppLimited() {
		t.Fatal("idle send pass did not mark path 1 app limited")
	}
}

// nextBoundOrNil sends one multipath packet, reporting whether there was one.
func (c *Conn) nextBoundOrNil(t *testing.T, now time.Time) bool {
	t.Helper()
	_, _, b, err := c.nextBoundMultipathPacket(now)
	if err != nil {
		t.Fatal(err)
	}
	return b != nil
}

// Stream data queued on one path can be sent on another, so a path with an
// empty queue is not application limited, nor reported limited to its
// controller, while another path holds data it could borrow.
func TestMultipathBorrowableDataIsNotAppLimited(t *testing.T) {
	client, server := sendingPathPair(t, wire.Version1)
	now := time.Unix(101, 0)
	app := client.multipath.paths[1].application
	app.budget.Validate()
	app.validation.state = PathValidated
	recorder := &recordingController{Controller: app.congestion}
	app.congestion = recorder
	id, err := client.OpenStream(false)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.queueFrameOnPath(1, wire.StreamFrame{StreamID: id, Data: make([]byte, 1000)}); err != nil {
		t.Fatal(err)
	}
	raw, err := client.nextPacketOnPath(1, now)
	if err != nil || raw == nil {
		t.Fatal("stream packet", err)
	}
	// More stream data waits on path 0, which path 1 could borrow.
	if err := client.queueFrameOnPath(0, wire.StreamFrame{StreamID: id, Offset: 1000, Data: make([]byte, 1000)}); err != nil {
		t.Fatal(err)
	}
	if !client.pathHasData(1) {
		t.Fatal("path 1 sees no data though path 0 holds stream data it can borrow")
	}
	if err := server.handleMultipathPacket(raw, now); err != nil {
		t.Fatal(err)
	}
	ackNow(server)
	server.multipath.paths[1].application.budget.Validate()
	ack, err := server.nextPacketOnPath(1, now)
	if err != nil || ack == nil {
		t.Fatal("PATH_ACK", err)
	}
	if err := client.handleMultipathPacket(ack, now.Add(30*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if len(recorder.events) == 0 || recorder.events[len(recorder.events)-1].Limited {
		t.Fatal("PATH_ACK reported limited while path 0 held borrowable data")
	}
	app.sent, _ = recovery.NewSent(maxSentPackets)
	client.noteMultipathSendIdle()
	if app.sent.AppLimited() {
		t.Fatal("idle send pass marked path 1 app limited while path 0 held borrowable data")
	}
}

// A path still validating cannot borrow another path's data, so that data
// doesn't count as its own.
func TestValidatingPathCannotCountBorrowableData(t *testing.T) {
	client, _ := sendingPathPair(t, wire.Version1)
	app := client.multipath.paths[1].application
	app.validation.state = PathValidating
	id, err := client.OpenStream(false)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.queueFrameOnPath(0, wire.StreamFrame{StreamID: id, Data: make([]byte, 1000)}); err != nil {
		t.Fatal(err)
	}
	if client.pathHasData(1) {
		t.Fatal("validating path counted data it cannot borrow")
	}
	app.validation.state = PathValidated
	if !client.pathHasData(1) {
		t.Fatal("validated path did not count data it can borrow")
	}
}

// A send pass held back by a pending foreign write is limited by the socket,
// not the application.
func TestPendingWriteIsNotAppLimited(t *testing.T) {
	client, _, now, _ := settledPair(t, streamParameters())
	drain(t, client, now)
	client.recovery, _ = recovery.NewSent(maxSentPackets)
	client.pendingWrite = &pendingForeignWrite{}
	if b, err := client.NextDatagram(now); err != nil || b != nil {
		t.Fatal("setup", err)
	}
	client.pendingWrite = nil
	if client.recovery.AppLimited() {
		t.Fatal("pending foreign write marked the path app limited")
	}
}

// A DATAGRAM too large for a path stays on its source, so it doesn't count as
// data that path could send; one that fits does.
func TestOversizedDatagramIsNotBorrowable(t *testing.T) {
	client, _ := sendingPathPair(t, wire.Version1)
	client.multipath.paths[1].application.validation.state = PathValidated
	if err := client.queueFrameOnPath(0, wire.DatagramFrame{Data: make([]byte, 1300), OmitLength: true}); err != nil {
		t.Fatal(err)
	}
	if client.pathHasData(1) {
		t.Fatal("path 1 counted a DATAGRAM too large for it")
	}
	if err := client.queueFrameOnPath(0, wire.DatagramFrame{Data: make([]byte, 100), OmitLength: true}); err != nil {
		t.Fatal(err)
	}
	if !client.pathHasData(1) {
		t.Fatal("path 1 did not count a DATAGRAM that fits it")
	}
}
