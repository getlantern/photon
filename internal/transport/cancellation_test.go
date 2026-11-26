package transport

import (
	"errors"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestEngineStopSending(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(vName(version), func(t *testing.T) {
			client, server := testPair(t, version, streamParameters())
			drivePair(t, client, server, false, false)
			id, err := client.OpenStream(false)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := client.WriteStream(id, []byte("first chunk"), false); err != nil {
				t.Fatal(err)
			}
			drivePair(t, client, server, false, false)
			if _, err := server.AcceptStream(false); err != nil {
				t.Fatal(err)
			}
			if err := server.CancelRead(id, 42); err != nil {
				t.Fatal(err)
			}
			if err := server.CancelRead(id, 99); err != nil {
				t.Fatal(err)
			}
			_, err = server.ReadStream(id, make([]byte, 100))
			var canceled *StreamError
			if !errors.As(err, &canceled) || canceled.ErrorCode != 42 || canceled.Remote || canceled.StreamID != StreamID(id) {
				t.Fatal(err)
			}
			// Lose and reorder cancellation traffic; STOP and RESET must remain reliable.
			drivePair(t, client, server, true, true)
			if !client.streams.objects[id].send.Complete() {
				t.Fatal("reset not acknowledged")
			}
			var stopped *StreamError
			if _, err := client.WriteStream(id, []byte("late"), false); !errors.As(err, &stopped) || stopped.ErrorCode != 42 || !stopped.Remote || stopped.StreamID != StreamID(id) {
				t.Fatal(err)
			}
			final, known := server.streams.objects[id].receive.FinalSize()
			if !known || final != uint64(len("first chunk")) {
				t.Fatal(final, known)
			}
			if server.streams.receive.Consumed() != final {
				t.Fatal("cancellation did not release unread bytes")
			}
			if err := client.handleStreamFrame(wire.StopSendingFrame{StreamID: id, ErrorCode: 99}); err != nil {
				t.Fatal(err)
			}
			if client.HasOutstanding() {
				t.Fatal("duplicate STOP created another reset")
			}
		})
	}
}
func TestCancelWriteRetiresDataWithoutDroppingFlight(t *testing.T) {
	client, server := testPair(t, wire.Version1, streamParameters())
	drivePair(t, client, server, false, false)
	id, _ := client.OpenStream(false)
	client.WriteStream(id, make([]byte, 900), false)
	packet, err := client.NextDatagram(time.Unix(200, 0))
	if err != nil || len(packet) == 0 {
		t.Fatal(err)
	}
	_, before := client.CongestionStats()
	client.WriteStream(id, make([]byte, 500), false)
	if err := client.CancelWrite(id, 17); err != nil {
		t.Fatal(err)
	}
	if _, after := client.CongestionStats(); after != before {
		t.Fatal("cancellation removed congestion flight")
	}
	if client.queuedBytes != 32 {
		t.Fatal("stream data retained", client.queuedBytes)
	}
	for _, item := range client.spaces[applicationSpace].queue {
		if _, ok := item.frame.(wire.StreamFrame); ok {
			t.Fatal("canceled STREAM queued")
		}
	}
	drivePair(t, client, server, true, true)
	if _, err := server.AcceptStream(false); err != nil {
		t.Fatal(err)
	}
	_, err = server.ReadStream(id, make([]byte, 1))
	var reset *StreamError
	if !errors.As(err, &reset) || reset.ErrorCode != 17 {
		t.Fatal(err)
	}
	if final, _ := server.streams.objects[id].receive.FinalSize(); final != 1400 {
		t.Fatal("reset changed committed final size", final)
	}
}
func TestCompletedSendIgnoresStop(t *testing.T) {
	client, server := testPair(t, wire.Version1, streamParameters())
	drivePair(t, client, server, false, false)
	id, _ := client.OpenStream(false)
	writeAll(t, client, id, []byte("complete"))
	drivePair(t, client, server, false, false)
	if !client.streams.objects[id].send.Complete() {
		t.Fatal("all-data ACK did not complete send side")
	}
	if err := client.handleStreamFrame(wire.StopSendingFrame{StreamID: id, ErrorCode: 42}); err != nil {
		t.Fatal(err)
	}
	if client.HasOutstanding() {
		t.Fatal("completed send reset")
	}
}

func TestFINACKWaitsForEarlierData(t *testing.T) {
	client, server := testPair(t, wire.Version1, streamParameters())
	drivePair(t, client, server, false, false)
	id, _ := client.OpenStream(false)
	client.WriteStream(id, []byte("first"), false)
	client.WriteStream(id, []byte("last"), true)
	first := client.spaces[applicationSpace].nextPN
	now := time.Unix(200, 0)
	if packets := drain(t, client, now); len(packets) != 2 {
		t.Fatal("expected two data packets", len(packets))
	}
	pns := sentNumbers(&client.spaces[applicationSpace], first)
	if len(pns) != 2 {
		t.Fatal("expected two data packets in the ledger", pns)
	}
	if err := client.handleAck(applicationSpace, ackFor(pns[1]), now); err != nil {
		t.Fatal(err)
	}
	if client.streams.objects[id].send.Complete() {
		t.Fatal("FIN-only ACK completed send")
	}
	if err := client.handleAck(applicationSpace, ackFor(pns...), now); err != nil {
		t.Fatal(err)
	}
	if !client.streams.objects[id].send.Complete() {
		t.Fatal("all-data ACK did not complete send")
	}
}
