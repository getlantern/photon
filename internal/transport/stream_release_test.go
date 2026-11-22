package transport

import (
	"errors"
	"io"
	"testing"

	"github.com/getlantern/photon/internal/stream"
	"github.com/getlantern/photon/internal/wire"
)

func TestStreamReleaseReplenishesLimits(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(vName(version), func(t *testing.T) {
			p := streamParameters()
			p.InitialMaxStreamsUni = 1
			client, server := testPair(t, version, p)
			drivePair(t, client, server, false, false)
			for i := 0; i < 300; i++ {
				id, err := client.OpenStream(true)
				if err != nil {
					t.Fatal("open", i, err)
				}
				if _, err := client.OpenStream(true); !errors.Is(err, stream.ErrWouldBlock) {
					t.Fatal("stream limit not enforced", err)
				}
				writeAll(t, client, id, []byte("done"))
				drivePair(t, client, server, false, false)
				if err := server.ReleaseStream(id); !errors.Is(err, stream.ErrWouldBlock) {
					t.Fatal("released unaccepted stream", err)
				}
				accepted, err := server.AcceptStream(true)
				if err != nil || accepted != id {
					t.Fatal(accepted, id, err)
				}
				if err := server.ReleaseStream(id); !errors.Is(err, stream.ErrWouldBlock) {
					t.Fatal("released unread data", err)
				}
				n, err := server.ReadStream(id, make([]byte, 4))
				if n != 4 || err != io.EOF {
					t.Fatal(n, err)
				}
				if err := server.ReleaseStream(id); err != nil {
					t.Fatal(err)
				}
				if err := client.ReleaseStream(id); err != nil {
					t.Fatal(err)
				}
				if err := server.ReleaseStream(id); err != nil {
					t.Fatal("duplicate release", err)
				}
				drivePair(t, client, server, false, false)
				if len(client.streams.objects) != 0 || len(server.streams.objects) != 0 {
					t.Fatal("stream objects retained")
				}
				charged := server.streams.receive.Charged()
				if err := server.handleStreamFrame(wire.StreamFrame{StreamID: id, Data: []byte("done"), Fin: true}); err != nil {
					t.Fatal(err)
				}
				if len(server.streams.objects) != 0 || server.streams.receive.Charged() != charged {
					t.Fatal("late frame resurrected released stream")
				}
			}
		})
	}
}
func TestCanceledCompleteReadCanRelease(t *testing.T) {
	client, server := testPair(t, wire.Version1, streamParameters())
	drivePair(t, client, server, false, false)
	id, _ := client.OpenStream(true)
	writeAll(t, client, id, []byte("unread"))
	drivePair(t, client, server, false, false)
	server.AcceptStream(true)
	if err := server.CancelRead(id, 42); err != nil {
		t.Fatal(err)
	}
	if server.streams.receive.Consumed() != 6 {
		t.Fatal("unread completed bytes not reclaimed")
	}
	if err := server.ReleaseStream(id); err != nil {
		t.Fatal(err)
	}
}
