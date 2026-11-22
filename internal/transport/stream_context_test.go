package transport

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/getlantern/photon/internal/wire"
)

func TestSendCloseNotification(t *testing.T) {
	client, server := testPair(t, wire.Version1, streamParameters())
	drivePair(t, client, server, false, false)
	id, err := client.OpenStream(false)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	if err := client.WatchSendClose(id, func(err error) {
		if err != context.Canceled {
			t.Fatal(err)
		}
		calls++
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.WriteStream(id, []byte("done"), true); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || client.streams.objects[id].onSendClosed != nil || client.streams.objects[id].send.Complete() {
		t.Fatal("context close confused with delivery")
	}
	drivePair(t, client, server, false, false)
	if calls != 1 {
		t.Fatal("duplicate notification")
	}
	// Late registration must observe a reset that happened before acceptance.
	remote, err := server.AcceptStream(false)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.CancelWrite(remote, 5); err != nil {
		t.Fatal(err)
	}
	if err := server.WatchSendClose(remote, func(err error) {
		if e, ok := err.(*StreamError); !ok || e.ErrorCode != 5 || e.Remote {
			t.Fatal(err)
		}
	}); err != nil {
		t.Fatal(err)
	}
}

func TestReceiveCloseNotification(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, mode := range []string{"fin-gap", "reset", "cancel", "before-accept"} {
			t.Run(fmt.Sprintf("%x/%s", version, mode), func(t *testing.T) {
				client, server := testPair(t, version, streamParameters())
				drivePair(t, client, server, false, false)
				id, err := client.OpenStream(false)
				if err != nil {
					t.Fatal(err)
				}
				frame := wire.StreamFrame{StreamID: id, Offset: 2, Data: []byte("cd"), Fin: true}
				if mode == "before-accept" {
					frame.Offset = 0
					frame.Data = []byte("abcd")
				}
				if err := server.handleStreamFrame(frame); err != nil {
					t.Fatal(err)
				}
				if _, err := server.AcceptStream(false); err != nil {
					t.Fatal(err)
				}
				calls := 0
				var cause error
				if err := server.WatchReceiveClose(id, func(err error) { calls++; cause = err }); err != nil {
					t.Fatal(err)
				}
				if mode != "before-accept" && calls != 0 {
					t.Fatal("FIN with gap closed receive")
				}
				switch mode {
				case "fin-gap":
					if err := server.handleStreamFrame(wire.StreamFrame{StreamID: id, Data: []byte("ab")}); err != nil {
						t.Fatal(err)
					}
				case "reset":
					if err := server.handleStreamFrame(wire.ResetStreamFrame{StreamID: id, FinalSize: 4, ErrorCode: 42}); err != nil {
						t.Fatal(err)
					}
				case "cancel":
					if err := server.CancelRead(id, 42); err != nil {
						t.Fatal(err)
					}
				}
				if calls != 1 {
					t.Fatalf("notifications: %d", calls)
				}
				if mode == "fin-gap" || mode == "before-accept" {
					if cause != io.EOF {
						t.Fatal(cause)
					}
					b := make([]byte, 4)
					if n, err := server.ReadStream(id, b); n != 4 || err != io.EOF || string(b) != "abcd" {
						t.Fatal(n, err, string(b))
					}
				} else {
					e, ok := cause.(*StreamError)
					if !ok || e.StreamID != StreamID(id) || e.ErrorCode != 42 || e.Remote != (mode == "reset") {
						t.Fatal(cause)
					}
				}
				if err := server.handleStreamFrame(frame); err != nil {
					t.Fatal(err)
				}
				if calls != 1 || server.streams.objects[id].onReceiveClosed != nil {
					t.Fatal("duplicate notification or retained observer")
				}
			})
		}
	}
}
