package transport

import (
	"errors"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestPeerCloseDrainsAndReleasesState(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, application := range []bool{false, true} {
			client, server := testPair(t, version, streamParameters())
			drivePair(t, client, server, false, false)
			id, _ := client.OpenStream(false)
			client.WriteStream(id, []byte("pending"), false)
			now := time.Unix(200, 0)
			packet := makePeerPacket(t, server, applicationSpace, 1000, wire.ConnectionCloseFrame{Application: application, ErrorCode: 42, Reason: []byte("finished")})
			err := client.HandleDatagram(packet, now)
			if !errors.Is(err, ErrPeerClosed) || !client.Draining() {
				t.Fatal("peer close did not drain", err)
			}
			var closeError *PeerCloseError
			if !errors.As(err, &closeError) || closeError.Application != application || closeError.Code != 42 || closeError.Reason != "finished" {
				t.Fatal("peer close details lost", err)
			}
			var failure *ConnectionTransportError
			if errors.As(err, &failure) == application {
				t.Fatal("transport identity", err)
			}
			if !application && (!failure.Remote || failure.ErrorCode != 42 || failure.ErrorMessage != "finished") {
				t.Fatal(failure)
			}
			var app *ApplicationError
			if errors.As(err, &app) != application {
				t.Fatal("transport/application error distinction", err)
			}
			deadline := client.RecoveryDeadline()
			if !deadline.After(now) {
				t.Fatal("missing drain deadline")
			}
			if client.peerIDs == nil || client.localIDs == nil {
				t.Fatal("routing identity removed too early")
			}
			if len(client.streams.objects) != 0 || client.queuedBytes != 0 || client.recovery.BytesInFlight != 0 || client.applicationKeys.read != nil {
				t.Fatal("draining retained stream/crypto/flight resources")
			}
			for i := 0; i < 5; i++ {
				if got := client.HandleDatagram(packet, now.Add(time.Millisecond)); !errors.Is(got, ErrPeerClosed) {
					t.Fatal(got)
				}
				if client.RecoveryDeadline() != deadline {
					t.Fatal("traffic extended draining")
				}
				if b, _ := client.NextDatagram(now); len(b) != 0 {
					t.Fatal("output while draining")
				}
			}
			if err := client.HandleTimeout(deadline.Add(-time.Nanosecond)); err != nil {
				t.Fatal(err)
			}
			if !client.Draining() {
				t.Fatal("draining ended early")
			}
			if err := client.HandleTimeout(deadline); err != nil {
				t.Fatal(err)
			}
			if client.Draining() || !client.closed || client.peerIDs != nil || client.localIDs != nil || !client.RecoveryDeadline().IsZero() {
				t.Fatal("expired draining retained routing state")
			}
		}
	}
}

func TestPeerCloseDuringHandshake(t *testing.T) {
	client, server := testPair(t, wire.Version1)
	now := time.Unix(200, 0)
	raw := makePeerPacket(t, client, initialSpace, 1000, wire.ConnectionCloseFrame{ErrorCode: 42})
	if err := server.HandleDatagram(raw, now); !errors.Is(err, ErrPeerClosed) {
		t.Fatal(err)
	}
	if server.RecoveryDeadline() != now.Add(2997*time.Millisecond) {
		t.Fatal("handshake draining duration", server.RecoveryDeadline())
	}
	if server.initialRead != nil || server.initialWrite != nil {
		t.Fatal("Initial keys retained during draining")
	}
}
