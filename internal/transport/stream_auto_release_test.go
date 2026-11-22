package transport

import (
	"errors"
	"io"
	"testing"

	"github.com/getlantern/photon/internal/stream"
	"github.com/getlantern/photon/internal/wire"
)

func TestStreamAutomaticRelease(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(vName(v), func(t *testing.T) {
			p := streamParameters()
			p.InitialMaxStreamsUni = 1
			client, server := testPair(t, v, p)
			drivePair(t, client, server, false, false)
			sent, received := 0, 0
			for i := 0; i < 300; i++ {
				id, err := client.OpenStream(true)
				if err != nil {
					t.Fatal(i, err)
				}
				if err := client.AutoReleaseStream(id, func(err error) {
					if err != nil {
						t.Fatal(err)
					}
					sent++
				}); err != nil {
					t.Fatal(err)
				}
				writeAll(t, client, id, []byte("done"))
				if sent != i || len(client.streams.objects) != 1 {
					t.Fatal("released before send acknowledgement")
				}
				drivePair(t, client, server, false, false)
				if err := server.AutoReleaseStream(id, func(error) {}); !errors.Is(err, stream.ErrStreamState) {
					t.Fatal("unaccepted watch", err)
				}
				accepted, err := server.AcceptStream(true)
				if err != nil || accepted != id {
					t.Fatal(accepted, err)
				}
				if err := server.AutoReleaseStream(id, func(err error) {
					if err != io.EOF {
						t.Fatal("terminal read", err)
					}
					received++
				}); err != nil {
					t.Fatal(err)
				}
				if len(server.streams.objects) != 1 {
					t.Fatal("unread data released")
				}
				n, err := server.ReadStream(id, make([]byte, 4))
				if n != 4 || err != io.EOF {
					t.Fatal(n, err)
				}
				drivePair(t, client, server, false, false)
				if len(client.streams.objects) != 0 || len(server.streams.objects) != 0 {
					t.Fatal("retained storage")
				}
				if sent != i+1 || received != i+1 {
					t.Fatal("callback count", sent, received)
				}
			}
		})
	}
}

func TestAutomaticReleasePreservesReset(t *testing.T) {
	client, server := testPair(t, wire.Version1, streamParameters())
	drivePair(t, client, server, false, false)
	id, err := client.OpenStream(true)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.CancelWrite(id, 77); err != nil {
		t.Fatal(err)
	}
	drivePair(t, client, server, false, false)
	if _, err := server.AcceptStream(true); err != nil {
		t.Fatal(err)
	}
	var terminal error
	if err := server.AutoReleaseStream(id, func(err error) { terminal = err }); err != nil {
		t.Fatal(err)
	}
	var reset *StreamError
	if !errors.As(terminal, &reset) || reset.ErrorCode != 77 || !reset.Remote || reset.StreamID != StreamID(id) {
		t.Fatalf("lost reset: %v", terminal)
	}
	if len(server.streams.objects) != 0 {
		t.Fatal("reset stream retained")
	}
}
