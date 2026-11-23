package transport

import (
	"errors"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/stream"
	"github.com/getlantern/photon/internal/wire"
)

// These tests exercise the frame handler directly; protected negotiation is
// covered by the sender and independent interoperability tests.
func partialResetReceiver(t *testing.T, version wire.Version) *Conn {
	t.Helper()
	p := streamParameters()
	p.InitialMaxData = 16
	p.InitialMaxStreamsUni = 1
	client, server := testPair(t, version, p)
	drivePair(t, client, server, false, false)
	if err := server.handleStreamFrame(wire.ResetStreamAtFrame{StreamID: 2, FinalSize: 16, ReliableSize: 8, ErrorCode: 7}); err != nil {
		t.Fatal(err)
	}
	if id, err := server.AcceptStream(true); err != nil || id != 2 {
		t.Fatal(id, err)
	}
	return server
}

func requirePartialResetError(t *testing.T, err error, code StreamErrorCode, remote bool) {
	t.Helper()
	var reset *StreamError
	if !errors.As(err, &reset) || reset.StreamID != 2 || reset.ErrorCode != code || reset.Remote != remote {
		t.Fatalf("unexpected stream error: %v", err)
	}
}

func TestPartialResetReceiveLifecycle(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(vName(version), func(t *testing.T) {
			server := partialResetReceiver(t, version)
			closed, released := 0, 0
			if err := server.WatchReceiveClose(2, func(err error) { closed++; requirePartialResetError(t, err, 7, true) }); err != nil {
				t.Fatal(err)
			}
			if err := server.AutoReleaseStream(2, func(err error) { released++; requirePartialResetError(t, err, 7, true) }); err != nil {
				t.Fatal(err)
			}
			if n, err := server.ReadStream(2, make([]byte, 8)); n != 0 || !errors.Is(err, stream.ErrWouldBlock) {
				t.Fatal(n, err)
			}
			if err := server.handleStreamFrame(wire.StreamFrame{StreamID: 2, Offset: 4, Data: []byte("efgh")}); err != nil {
				t.Fatal(err)
			}
			if closed != 0 || released != 0 || server.streams.receive.Consumed() != 0 {
				t.Fatal("gap prematurely terminated receive")
			}
			if err := server.handleStreamFrame(wire.StreamFrame{StreamID: 2, Data: []byte("abcd")}); err != nil {
				t.Fatal(err)
			}
			if closed != 1 || released != 0 || len(server.streams.objects) != 1 {
				t.Fatal("unread prefix lifecycle", closed, released)
			}
			b := make([]byte, 16)
			n, err := server.ReadStream(2, b)
			requirePartialResetError(t, err, 7, true)
			if n != 8 || string(b[:n]) != "abcdefgh" {
				t.Fatal(n, string(b[:n]))
			}
			if closed != 1 || released != 1 || len(server.streams.objects) != 0 {
				t.Fatal("release lifecycle", closed, released)
			}
			if server.streams.receive.Consumed() != 16 || server.streams.receive.Limit() != 32 || server.streams.ids.ReceiveLimit(true) != 2 {
				t.Fatal("missing final-size or stream credit")
			}
			for _, frame := range []wire.Frame{
				wire.ResetStreamAtFrame{StreamID: 2, FinalSize: 16, ReliableSize: 12, ErrorCode: 7},
				wire.ResetStreamFrame{StreamID: 2, FinalSize: 16, ErrorCode: 7},
				wire.StreamFrame{StreamID: 2, Offset: 8, Data: []byte("ijklmnop"), Fin: true},
			} {
				if err := server.handleStreamFrame(frame); err != nil {
					t.Fatal(err)
				}
			}
			if closed != 1 || released != 1 || len(server.streams.objects) != 0 || server.streams.receive.Consumed() != 16 || server.streams.receive.Limit() != 32 || server.streams.ids.ReceiveLimit(true) != 2 {
				t.Fatal("late input recreated state or credit")
			}
		})
	}
}

func TestPartialResetCanceledReceive(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(vName(version), func(t *testing.T) {
			server := partialResetReceiver(t, version)
			closed, released := 0, 0
			if err := server.WatchReceiveClose(2, func(err error) { closed++; requirePartialResetError(t, err, 9, false) }); err != nil {
				t.Fatal(err)
			}
			if err := server.AutoReleaseStream(2, func(err error) { released++; requirePartialResetError(t, err, 9, false) }); err != nil {
				t.Fatal(err)
			}
			if err := server.CancelRead(2, 9); err != nil {
				t.Fatal(err)
			}
			if closed != 1 || released != 0 {
				t.Fatal(closed, released)
			}
			stops := func() int {
				n := 0
				for _, item := range server.spaces[applicationSpace].queue {
					if f, ok := item.frame.(wire.StopSendingFrame); ok && f.StreamID == 2 && !item.acked {
						n++
					}
				}
				return n
			}
			if stops() != 1 {
				t.Fatal("missing STOP_SENDING")
			}
			if err := server.handleStreamFrame(wire.StreamFrame{StreamID: 2, Data: []byte("abcdefgh")}); err != nil {
				t.Fatal(err)
			}
			if closed != 1 || released != 1 || len(server.streams.objects) != 0 || stops() != 0 {
				t.Fatal("canceled receive not retired", closed, released, stops())
			}
			if server.streams.receive.Consumed() != 16 || server.streams.receive.Limit() != 32 || server.streams.ids.ReceiveLimit(true) != 2 {
				t.Fatal("canceled prefix credit")
			}
		})
	}
}

func TestPartialResetReleasedValidation(t *testing.T) {
	cases := []struct {
		name  string
		frame wire.Frame
		code  TransportErrorCode
	}{
		{"partial-code", wire.ResetStreamAtFrame{StreamID: 2, FinalSize: 16, ReliableSize: 8, ErrorCode: 8}, 5},
		{"ordinary-code", wire.ResetStreamFrame{StreamID: 2, FinalSize: 16, ErrorCode: 8}, 5},
		{"partial-final", wire.ResetStreamAtFrame{StreamID: 2, FinalSize: 17, ReliableSize: 8, ErrorCode: 7}, 6},
		{"ordinary-final", wire.ResetStreamFrame{StreamID: 2, FinalSize: 17, ErrorCode: 7}, 6},
		{"stream-beyond-final", wire.StreamFrame{StreamID: 2, Offset: 16, Data: []byte("x")}, 6},
		{"stream-short-fin", wire.StreamFrame{StreamID: 2, Offset: 8, Fin: true}, 6},
	}
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(vName(version), func(t *testing.T) {
			for _, serverReceives := range []bool{false, true} {
				for _, tc := range cases {
					role := "client-receives"
					if serverReceives {
						role = "server-receives"
					}
					t.Run(role+"/"+tc.name, func(t *testing.T) {
						p := streamParameters()
						p.ResetStreamAt = true
						client, server := testPair(t, version, p)
						drivePair(t, client, server, false, false)
						sender, receiver := client, server
						id := uint64(2)
						if !serverReceives {
							sender, receiver = server, client
							id = 3
						}
						frame := tc.frame
						switch f := frame.(type) {
						case wire.ResetStreamAtFrame:
							f.StreamID = id
							frame = f
						case wire.ResetStreamFrame:
							f.StreamID = id
							frame = f
						case wire.StreamFrame:
							f.StreamID = id
							frame = f
						}
						packet := makePeerPacket(t, sender, applicationSpace, 100,
							wire.ResetStreamAtFrame{StreamID: id, FinalSize: 16, ErrorCode: 7})
						if err := receiver.HandleDatagram(packet, time.Unix(101, 0)); err != nil {
							t.Fatal(err)
						}
						if got, err := receiver.AcceptStream(true); err != nil || got != id {
							t.Fatal(got, err)
						}
						if err := receiver.ReleaseStream(id); err != nil {
							t.Fatal(err)
						}
						packet = makePeerPacket(t, sender, applicationSpace, 101, wire.PingFrame{}, frame)
						err := receiver.HandleDatagram(packet, time.Unix(101, 0))
						var failure *ConnectionTransportError
						if !errors.As(err, &failure) || failure.ErrorCode != tc.code || failure.FrameType != frame.Type() || failure.Remote || !receiver.Closing() {
							t.Fatalf("late violation: %v", err)
						}
						output, err := receiver.NextDatagram(time.Unix(101, 0))
						if err != nil || len(output) == 0 {
							t.Fatal("missing protected close", err)
						}
						err = sender.HandleDatagram(output, time.Unix(101, 0))
						var remote *ConnectionTransportError
						if !errors.As(err, &remote) || !remote.Remote || remote.ErrorCode != tc.code || remote.FrameType != frame.Type() || !sender.Draining() {
							t.Fatalf("peer did not receive expected close: %v", err)
						}
					})
				}
			}
		})
	}
}

func TestPartialResetHistoryBound(t *testing.T) {
	server := partialResetReceiver(t, wire.Version1)
	h := new(partialResetHistory)
	server.streams.partialClosed = h
	for i := 0; i < partialResetHistoryCapacity+3; i++ {
		h.add(uint64(i)*4+2, 16, 7)
	}
	if len(h.records) != partialResetHistoryCapacity || len(h.order) != partialResetHistoryCapacity {
		t.Fatal("unbounded history")
	}
	for _, id := range []uint64{2, 6, 10} {
		if _, ok := h.records[id]; ok {
			t.Fatal("old record retained", id)
		}
	}
	if err := h.validate(14, wire.ResetStreamFrame{FinalSize: 17, ErrorCode: 7}); !errors.Is(err, stream.ErrFinalSize) {
		t.Fatal(err)
	}
	// Repeated insertion must neither change known metadata nor evict another ID.
	h.add(14, 100, 100)
	if h.records[14] != (partialResetRecord{16, 7}) || h.next != 3 {
		t.Fatal("duplicate history insertion")
	}
	if err := server.handleStreamFrame(wire.ResetStreamAtFrame{StreamID: 2, FinalSize: 16, ErrorCode: 7}); err != nil {
		t.Fatal(err)
	}
	if err := server.ReleaseStream(2); err != nil {
		t.Fatal(err)
	}
	// Evict the real released stream again and ensure closed-ID tracking survives.
	for i := 0; i < partialResetHistoryCapacity; i++ {
		h.add(uint64(i)*4+10002, 16, 7)
	}
	if _, ok := h.records[2]; ok {
		t.Fatal("record not evicted")
	}
	if err := server.handleStreamFrame(wire.StreamFrame{StreamID: 2, Data: []byte("abcdefgh")}); err != nil {
		t.Fatal(err)
	}
	if len(server.streams.objects) != 0 || server.streams.receive.Consumed() != 16 || server.streams.ids.ReceiveLimit(true) != 2 {
		t.Fatal("evicted history recreated stream")
	}
}

func TestPartialResetCompletedReadSurvivesClose(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(vName(version), func(t *testing.T) {
			server := partialResetReceiver(t, version)
			if err := server.handleStreamFrame(wire.StreamFrame{StreamID: 2, Data: []byte("abcdefgh")}); err != nil {
				t.Fatal(err)
			}
			released := 0
			if err := server.AutoReleaseStream(2, func(err error) { released++; requirePartialResetError(t, err, 7, true) }); err != nil {
				t.Fatal(err)
			}
			original := server.streams.objects[2].receive
			server.beginDraining(ErrPeerClosed, time.Unix(200, 0))
			retained := server.completedReads[2]
			if retained == nil || retained.receive != original || len(server.streams.objects) != 0 || released != 0 {
				t.Fatal("completed prefix lost during close")
			}
			b := make([]byte, 4)
			if n, err := server.ReadStream(2, b); n != 4 || err != nil || string(b) != "abcd" {
				t.Fatal(n, err, string(b))
			}
			n, err := server.ReadStream(2, b)
			requirePartialResetError(t, err, 7, true)
			if n != 4 || string(b) != "efgh" || released != 1 || len(server.completedReads) != 0 {
				t.Fatal("retained prefix not drained", n, string(b), released)
			}
		})
	}
}

// Exercise FIFO eviction through negotiated, authenticated input and actual
// releases. Synthetic history insertion alone cannot verify closed-ID tracking.
func TestPartialResetHistoryProtectedEviction(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, serverReceives := range []bool{false, true} {
			role := "client-receives"
			if serverReceives {
				role = "server-receives"
			}
			t.Run(vName(version)+"/"+role, func(t *testing.T) {
				p := streamParameters()
				p.ResetStreamAt = true
				p.InitialMaxStreamsUni = partialResetHistoryCapacity + 1
				client, server := testPair(t, version, p)
				drivePair(t, client, server, false, false)
				sender, receiver, first := server, client, uint64(3)
				if serverReceives {
					sender, receiver, first = client, server, 2
				}
				pn := uint64(100)
				deliver := func(frame wire.Frame) error {
					packet := makePeerPacket(t, sender, applicationSpace, pn, frame)
					pn++
					return receiver.HandleDatagram(packet, time.Unix(101, 0))
				}
				for i := 0; i <= partialResetHistoryCapacity; i++ {
					id := first + 4*uint64(i)
					if err := deliver(wire.ResetStreamAtFrame{StreamID: id, FinalSize: 16, ErrorCode: 7}); err != nil {
						t.Fatalf("reset %d: %v", i, err)
					}
					if got, err := receiver.AcceptStream(true); err != nil || got != id {
						t.Fatal(got, id, err)
					}
					if err := receiver.ReleaseStream(id); err != nil {
						t.Fatal(err)
					}
				}
				h := receiver.streams.partialClosed
				if len(h.records) != partialResetHistoryCapacity || len(h.order) != partialResetHistoryCapacity {
					t.Fatal("unbounded released history")
				}
				if _, ok := h.records[first]; ok {
					t.Fatal("oldest actual release was not evicted")
				}
				consumed, limit := receiver.streams.receive.Consumed(), receiver.streams.receive.Limit()
				streams := receiver.streams.ids.ReceiveLimit(true)
				if consumed != 16*(partialResetHistoryCapacity+1) || streams != 2*(partialResetHistoryCapacity+1) {
					t.Fatal("incorrect release credit", consumed, streams)
				}
				for _, frame := range []wire.Frame{
					wire.ResetStreamAtFrame{StreamID: first, FinalSize: 16, ReliableSize: 8, ErrorCode: 7},
					wire.StreamFrame{StreamID: first, Data: []byte("late"), Fin: true},
					wire.StreamFrame{StreamID: first, Offset: 16, Data: []byte("x")},
					wire.ResetStreamFrame{StreamID: first, FinalSize: 17, ErrorCode: 8},
					wire.ResetStreamAtFrame{StreamID: first, FinalSize: 17, ReliableSize: 8, ErrorCode: 8},
				} {
					if err := deliver(frame); err != nil {
						t.Fatalf("evicted metadata should not be reconstructed: %v", err)
					}
					if len(receiver.streams.objects) != 0 || receiver.streams.receive.Consumed() != consumed || receiver.streams.receive.Limit() != limit || receiver.streams.ids.ReceiveLimit(true) != streams {
						t.Fatal("late packet recreated stream state or credit")
					}
				}
				// The next oldest record remains available and must enforce the
				// draft's error-code consistency rule even after stream release.
				frame := wire.ResetStreamAtFrame{StreamID: first + 4, FinalSize: 16, ErrorCode: 8}
				var failure *ConnectionTransportError
				if err := deliver(frame); !errors.As(err, &failure) || failure.ErrorCode != 5 || failure.FrameType != frame.Type() {
					t.Fatalf("retained contradiction: %v", err)
				}
			})
		}
	}
}
