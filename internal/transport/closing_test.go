package transport

import (
	"bytes"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestApplicationClosePacketAndDraining(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		client, server := testPair(t, v)
		drivePair(t, client, server, false, false)
		now := time.Unix(200, 0)
		if err := client.CloseWithError(42, "finished", now); err != nil {
			t.Fatal(err)
		}
		if !client.Closing() {
			t.Fatal("not closing")
		}
		b, err := client.NextDatagram(now)
		if err != nil || len(b) == 0 {
			t.Fatal(err)
		}
		h, err := wire.ParseHeader(b, len(server.config.LocalCID))
		if err != nil {
			t.Fatal(err)
		}
		_, plain, _, err := server.readKeys(applicationSpace).Open(b, h.PNOffset, server.spaces[applicationSpace].received.Expected())
		if err != nil {
			t.Fatal(err)
		}
		frame, _, err := wire.ConsumeFrame(plain)
		if err != nil {
			t.Fatal(err)
		}
		close, ok := frame.(wire.ConnectionCloseFrame)
		if !ok || !close.Application || close.ErrorCode != 42 || string(close.Reason) != "finished" {
			t.Fatal(frame)
		}
		if err := server.HandleDatagram(b, now); !errors.Is(err, ErrPeerClosed) || !server.Draining() {
			t.Fatal(err)
		}
		if _, err := client.OpenStream(false); !errors.Is(err, ErrLocalClosed) {
			t.Fatal("application operation after close", err)
		}
		for _, item := range []struct {
			conn   *Conn
			remote bool
		}{{client, false}, {server, true}} {
			var app *ApplicationError
			if !errors.As(item.conn.err, &app) || app.Remote != item.remote || app.ErrorCode != 42 || app.ErrorMessage != "finished" || !errors.Is(item.conn.err, net.ErrClosed) || !errors.Is(item.conn.err, &ApplicationError{Remote: item.remote, ErrorCode: 42}) || errors.Is(item.conn.err, &ApplicationError{Remote: !item.remote, ErrorCode: 42}) {
				t.Fatal("application error identity", item.conn.err)
			}
		}
		deadline := client.RecoveryDeadline()
		client.HandleTimeout(deadline)
		if client.Closing() || !client.closed || !client.RecoveryDeadline().IsZero() {
			t.Fatal("closing did not expire")
		}
	}
}
func TestCloseResponseRateAndCachedPacket(t *testing.T) {
	client, server := testPair(t, wire.Version1)
	drivePair(t, client, server, false, false)
	now := time.Unix(200, 0)
	before := client.sentTotals
	client.CloseWithError(42, "bye", now)
	first, err := client.NextDatagram(now)
	if err != nil {
		t.Fatal(err)
	}
	interval := client.closing.interval
	ping := makePeerPacket(t, server, applicationSpace, 1000, wire.PingFrame{})
	for i := 0; i < 10; i++ {
		client.HandleDatagram(ping, now)
		if b, _ := client.NextDatagram(now); len(b) != 0 {
			t.Fatal("close flood")
		}
	}
	at := now.Add(interval)
	client.HandleDatagram(ping, at)
	second, err := client.NextDatagram(at)
	if err != nil || !bytes.Equal(first, second) {
		t.Fatal("close retransmission changed ciphertext", err)
	}
	if client.sentTotals.packets != before.packets+2 || client.sentTotals.bytes != before.bytes+uint64(len(first)+len(second)) {
		t.Fatal("close retransmission accounting", client.sentTotals, before)
	}
	second[0] ^= 1
	if !bytes.Equal(first, client.closing.packets[0]) {
		t.Fatal("adapter mutated cached close")
	}
	if client.RecoveryDeadline() != now.Add(3*interval) {
		t.Fatal("close responses extended deadline")
	}
}
func TestClosingToDrainingPreservesDeadline(t *testing.T) {
	client, server := testPair(t, wire.Version1)
	drivePair(t, client, server, false, false)
	now := time.Unix(200, 0)
	client.CloseWithError(1, "", now)
	deadline := client.RecoveryDeadline()
	raw := makePeerPacket(t, server, applicationSpace, 1000, wire.ConnectionCloseFrame{Application: true, ErrorCode: 2})
	client.HandleDatagram(raw, now.Add(time.Millisecond))
	if !client.Draining() || client.Closing() || client.RecoveryDeadline() != deadline {
		t.Fatal("transition changed termination time")
	}
	if b, _ := client.NextDatagram(now); len(b) != 0 {
		t.Fatal("output after peer close")
	}
}
func TestApplicationCloseBeforeConfirmation(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, handshake := range []bool{false, true} {
			client, server := testPair(t, v)
			now := time.Unix(200, 0)
			space := initialSpace
			if handshake {
				for _, b := range drain(t, client, now) {
					if err := server.HandleDatagram(b, now); err != nil {
						t.Fatal(err)
					}
				}
				for _, b := range drain(t, server, now) {
					if err := client.HandleDatagram(b, now); err != nil {
						t.Fatal(err)
					}
				}
				drain(t, client, now) // Send Finished; do not let the server confirm yet.
				if client.confirmed || client.writeKeys(initialSpace) != nil || client.writeKeys(handshakeSpace) == nil {
					t.Fatal("wrong handshake setup")
				}
				space = handshakeSpace
			}
			if err := client.CloseWithError(wire.MaxVarint+1, "private", now); !errors.Is(err, wire.ErrVarintRange) {
				t.Fatal(err)
			}
			if client.Closing() || client.err != nil {
				t.Fatal("invalid close mutated state")
			}
			if err := client.CloseWithError(42, "private application details", now); err != nil {
				t.Fatal(err)
			}
			raw, err := client.NextDatagram(now)
			if err != nil || len(raw) == 0 {
				t.Fatal(err)
			}
			h, err := wire.ParseHeader(raw, len(server.config.LocalCID))
			if err != nil || h.Type != packetTypes[space] {
				t.Fatal(h, err)
			}
			_, plain, _, err := server.readKeys(space).Open(raw, h.PNOffset, 0)
			if err != nil {
				t.Fatal(err)
			}
			f, _, err := wire.ConsumeFrame(plain)
			if err != nil {
				t.Fatal(err)
			}
			close, ok := f.(wire.ConnectionCloseFrame)
			if !ok || close.Application || close.ErrorCode != 0xc || close.TriggerFrame != 0 || len(close.Reason) != 0 {
				t.Fatal(f)
			}
			if err := server.HandleDatagram(raw, now); !errors.Is(err, ErrPeerClosed) || !server.Draining() {
				t.Fatal(err)
			}
		}
	}
}

func TestInitialTransportClose(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		client, server := testPair(t, version)
		now := time.Unix(200, 0)
		if err := client.CloseTransportError(7, 0x18, "bad frame", now); err != nil {
			t.Fatal(err)
		}
		raw, err := client.NextDatagram(now)
		if err != nil || len(raw) != 1200 {
			t.Fatal("Initial close padding", len(raw), err)
		}
		h, err := wire.ParseHeader(raw, len(server.config.LocalCID))
		if err != nil || h.Type != wire.Initial {
			t.Fatal(h, err)
		}
		_, plain, _, err := server.readKeys(initialSpace).Open(raw, h.PNOffset, 0)
		if err != nil {
			t.Fatal(err)
		}
		frame, _, err := wire.ConsumeFrame(plain)
		if err != nil {
			t.Fatal(err)
		}
		close := frame.(wire.ConnectionCloseFrame)
		if close.Application || close.ErrorCode != 7 || close.TriggerFrame != 0x18 || string(close.Reason) != "bad frame" {
			t.Fatal(close)
		}
		if err := server.HandleDatagram(raw, now); !errors.Is(err, ErrPeerClosed) || !server.Draining() {
			t.Fatal(err)
		}
	}
}
func TestTransportCloseAfterReportedFailure(t *testing.T) {
	client, server := testPair(t, wire.Version1)
	drivePair(t, client, server, false, false)
	now := time.Unix(200, 0)
	original := client.fail(&TransportError{Code: 3, FrameType: 8, Reason: "flow limit"})
	if err := client.CloseTransportError(3, 8, "flow limit", now); err != nil {
		t.Fatal(err)
	}
	raw, err := client.NextDatagram(now)
	if err != nil || len(raw) == 0 {
		t.Fatal(err)
	}
	if client.err != original {
		t.Fatal("close replaced original failure")
	}
	if err := server.HandleDatagram(raw, now); !errors.Is(err, ErrPeerClosed) {
		t.Fatal(err)
	}
}
func TestTransportCloseHonorsAmplification(t *testing.T) {
	client, server := testPair(t, wire.Version1)
	now := time.Unix(200, 0)
	raw, err := client.NextDatagram(now)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.CloseTransportError(7, 0, "", now); err != nil {
		t.Fatal(err)
	}
	if b, err := server.NextDatagram(now); err != nil || len(b) != 0 {
		t.Fatal("unvalidated server exceeded zero credit", err)
	}
	server.HandleDatagram(raw, now)
	if b, err := server.NextDatagram(now); err != nil || len(b) != 1200 {
		t.Fatal("received credit did not permit close", len(b), err)
	}
}
func TestTransportCloseCannotExceedIntegrityLimit(t *testing.T) {
	client, server := testPair(t, wire.Version1)
	drivePair(t, client, server, false, false)
	client.integrityLimit = 10
	client.failedAuth = 10
	if err := client.CloseTransportError(0xf, 0, "", time.Unix(200, 0)); err == nil {
		t.Fatal("close allowed after AEAD exhaustion")
	}
	if client.Closing() {
		t.Fatal("protected close queued after AEAD exhaustion")
	}
}

func TestHandshakeCloseAtAllProtectionLevels(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, application := range []bool{false, true} {
			client, server := testPair(t, v)
			now := time.Unix(200, 0)
			initial := drain(t, client, now)
			for _, b := range initial {
				if err := server.HandleDatagram(b, now); err != nil {
					t.Fatal(err)
				}
			}
			for _, b := range drain(t, server, now) {
				if err := client.HandleDatagram(b, now); err != nil {
					t.Fatal(err)
				}
			}
			if server.confirmed {
				t.Fatal("server already confirmed")
			}
			before := server.sealed
			var err error
			if application {
				err = server.CloseWithError(42, "private", now)
			} else {
				err = server.CloseTransportError(7, 6, "bad crypto", now)
			}
			if err != nil {
				t.Fatal(err)
			}
			// Exactly enough credit for the Initial: the remaining levels must wait.
			server.budget = Amplification{credit: 1200}
			first, err := server.NextDatagram(now)
			if err != nil || len(first) != 1200 {
				t.Fatal("Initial", len(first), err)
			}
			if b, err := server.NextDatagram(now); err != nil || len(b) != 0 {
				t.Fatal("amplification overspend", err)
			}
			server.budget.Received(1200)
			packets := append([][]byte{first}, drain(t, server, now)...)
			if len(packets) != 3 {
				t.Fatal("missing protection level", len(packets))
			}
			for space, b := range packets {
				if server.sealed[space] != before[space]+1 {
					t.Fatal("wrong AEAD use count")
				}
				h, err := wire.ParseHeader(b, len(client.config.LocalCID))
				if err != nil || h.Type != packetTypes[space] {
					t.Fatal(h, err)
				}
				_, plain, _, err := client.readKeys(space).Open(b, h.PNOffset, client.spaces[space].received.Expected())
				if err != nil {
					t.Fatal("decrypt", space, err)
				}
				f, _, err := wire.ConsumeFrame(plain)
				if err != nil {
					t.Fatal(err)
				}
				close := f.(wire.ConnectionCloseFrame)
				if application && space != applicationSpace {
					if close.Application || close.ErrorCode != 0xc || len(close.Reason) != 0 {
						t.Fatal("application details exposed", close)
					}
				} else if application {
					if !close.Application || close.ErrorCode != 42 || string(close.Reason) != "private" {
						t.Fatal(close)
					}
				} else if close.Application || close.ErrorCode != 7 || close.TriggerFrame != 6 || string(close.Reason) != "bad crypto" {
					t.Fatal(close)
				}
			}
			// Traffic after the response interval permits a fresh burst of cached bytes.
			sealed := server.sealed
			deadline := server.RecoveryDeadline()
			at := now.Add(server.closing.interval)
			server.HandleDatagram(initial[0], at)
			repeated := drain(t, server, at)
			if len(repeated) != 3 {
				t.Fatal("repeat burst", len(repeated))
			}
			for i := range repeated {
				if !bytes.Equal(repeated[i], packets[i]) {
					t.Fatal("close resealed")
				}
			}
			if server.sealed != sealed || server.RecoveryDeadline() != deadline {
				t.Fatal("repeat changed key use or deadline")
			}
		}
	}
}

func TestClosingProcessesCoalescedPackets(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, prefix := range []string{"valid", "damaged", "other-version"} {
			client, server := testPair(t, v)
			now := time.Unix(200, 0)
			for _, b := range drain(t, client, now) {
				if err := server.HandleDatagram(b, now); err != nil {
					t.Fatal(err)
				}
			}
			for _, b := range drain(t, server, now) {
				if err := client.HandleDatagram(b, now); err != nil {
					t.Fatal(err)
				}
			}
			first := makePeerPacket(t, server, initialSpace, 1000, wire.PingFrame{})
			if prefix == "damaged" {
				first[len(first)-1] ^= 1
			}
			if prefix == "other-version" {
				other := wire.Version1
				if other == v {
					other = wire.Version2
				}
				header, err := wire.AppendLongHeader(nil, other, wire.Initial, client.config.LocalCID, server.config.LocalCID, nil, 0, 4, 48)
				if err != nil {
					t.Fatal(err)
				}
				first = append(header, make([]byte, 48)...)
			}
			last := makePeerPacket(t, server, handshakeSpace, 1000, wire.ConnectionCloseFrame{ErrorCode: 7})
			data := append(first, last...)
			if err := client.CloseWithError(42, "private", now); err != nil {
				t.Fatal(err)
			}
			deadline := client.RecoveryDeadline()
			credit := client.budget.credit
			if err := client.HandleDatagram(data, now); !errors.Is(err, ErrLocalClosed) || !client.Draining() {
				t.Fatal("later close ignored", err)
			}
			if client.RecoveryDeadline() != deadline {
				t.Fatal("deadline extended")
			}
			if client.budget.credit-credit != 3*uint64(len(data)) {
				t.Fatal("coalesced input counted more than once")
			}
		}
	}
}

func TestClosingAuthenticatedPacketNotReset(t *testing.T) {
	client, server := testPair(t, wire.Version1)
	drivePair(t, client, server, false, false)
	now := time.Unix(200, 0)
	raw := makePeerPacket(t, server, applicationSpace, 1000, wire.PingFrame{})
	id := client.peerIDs.active[client.peerIDs.current]
	copy(id.token[:], raw[len(raw)-16:])
	id.hasToken = true
	client.peerIDs.active[client.peerIDs.current] = id
	if err := client.CloseWithError(42, "done", now); err != nil {
		t.Fatal(err)
	}
	client.HandleDatagram(raw, now)
	if !client.Closing() || client.Draining() {
		t.Fatal("authenticated suffix treated as reset")
	}
	damaged := bytes.Clone(raw)
	damaged[1] ^= 0xff
	client.HandleDatagram(damaged, now)
	if !client.Draining() {
		t.Fatal("unknown CID reset missed")
	}
}

func TestClosingRejectsApplicationCloseInInitial(t *testing.T) {
	client, server := testPair(t, wire.Version1)
	now := time.Unix(200, 0)
	raw := makePeerPacket(t, server, initialSpace, 0, wire.ConnectionCloseFrame{Application: true, ErrorCode: 42})
	client.CloseWithError(1, "", now)
	client.HandleDatagram(raw, now)
	if !client.Closing() || client.Draining() {
		t.Fatal("illegal close frame accepted")
	}
}

func TestApplicationCloseRetainsLocalReason(t *testing.T) {
	client, server := testPair(t, wire.Version1)
	drivePair(t, client, server, false, false)
	reason := string(bytes.Repeat([]byte("x"), 1200))
	if err := client.CloseWithError(123, reason, time.Unix(200, 0)); err != nil {
		t.Fatal(err)
	}
	var app *ApplicationError
	if !errors.As(client.err, &app) || app.ErrorMessage != reason {
		t.Fatal(client.err)
	}
	first := client.err
	if err := client.CloseWithError(456, "replacement", time.Unix(201, 0)); err != nil || client.err != first {
		t.Fatal("close replaced cause", err, client.err)
	}
	packet, err := client.NextDatagram(time.Unix(200, 0))
	if err != nil {
		t.Fatal(err)
	}
	err = server.HandleDatagram(packet, time.Unix(200, 0))
	var peer *ApplicationError
	if !errors.As(err, &peer) || !peer.Remote || peer.ErrorCode != 123 || len(peer.ErrorMessage) != 900 {
		t.Fatal("wire reason bound", err)
	}
}
