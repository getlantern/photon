package transport

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestCompletedReadSurvivesPeerClose(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, cancelRead := range []bool{false, true} {
			client, server := testPair(t, v, streamParameters())
			drivePair(t, client, server, false, false)
			id, err := client.OpenStream(true)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = client.WriteStream(id, []byte("delivered before close"), true); err != nil {
				t.Fatal(err)
			}
			drivePair(t, client, server, false, false)
			accepted, err := server.AcceptStream(true)
			if err != nil || accepted != id {
				t.Fatal(accepted, err)
			}
			original := server.streams.objects[id].receive
			var terminal error
			if err := server.AutoReleaseStream(id, func(err error) { terminal = err }); err != nil {
				t.Fatal(err)
			}
			now := time.Unix(200, 0)
			closePacket := makePeerPacket(t, client, applicationSpace, 1000, wire.ConnectionCloseFrame{Application: true, ErrorCode: 0x100})
			if err := server.HandleDatagram(closePacket, now); !errors.Is(err, ErrPeerClosed) {
				t.Fatal(err)
			}
			if len(server.streams.objects) != 0 || server.completedReads[id].receive != original {
				t.Fatal("did not transfer existing buffer")
			}
			// Closing the routing lifetime must not destroy an application's completed read.
			server.Close()
			if cancelRead {
				if err := server.CancelRead(id, 9); err != nil {
					t.Fatal(err)
				}
				var reset *StreamError
				if !errors.As(terminal, &reset) || reset.ErrorCode != 9 {
					t.Fatal("cancel callback", terminal)
				}
			} else {
				var got []byte
				buf := make([]byte, 3)
				for {
					n, err := server.ReadStream(id, buf)
					got = append(got, buf[:n]...)
					if err == io.EOF {
						break
					}
					if err != nil {
						t.Fatal(err)
					}
				}
				if string(got) != "delivered before close" || terminal != io.EOF {
					t.Fatal(string(got), terminal)
				}
			}
			if len(server.completedReads) != 0 {
				t.Fatal("retained consumed data")
			}
		}
	}
}

func TestPeerCloseDoesNotRetainIncompleteOrUnacceptedReads(t *testing.T) {
	client, server := testPair(t, wire.Version1, streamParameters())
	drivePair(t, client, server, false, false)
	id, _ := client.OpenStream(true)
	client.WriteStream(id, []byte("incomplete"), false)
	drivePair(t, client, server, false, false)
	if _, err := server.AcceptStream(true); err != nil {
		t.Fatal(err)
	}
	unaccepted, _ := client.OpenStream(true)
	client.WriteStream(unaccepted, []byte("complete but unaccepted"), true)
	drivePair(t, client, server, false, false)
	packet := makePeerPacket(t, client, applicationSpace, 1000, wire.ConnectionCloseFrame{Application: true})
	if err := server.HandleDatagram(packet, time.Unix(200, 0)); !errors.Is(err, ErrPeerClosed) {
		t.Fatal(err)
	}
	if len(server.completedReads) != 0 {
		t.Fatal("retained incomplete or unaccepted stream")
	}
	if _, err := server.ReadStream(id, make([]byte, 20)); !errors.Is(err, ErrPeerClosed) {
		t.Fatal("incomplete read did not fail", err)
	}
}
