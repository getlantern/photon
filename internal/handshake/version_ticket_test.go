package handshake

import (
	"context"
	"crypto/tls"
	"fmt"
	"testing"

	"github.com/getlantern/photon/internal/packet"
	"github.com/getlantern/photon/internal/wire"
)

// Drive TLS using original-version client Initials and negotiated-version server
// flights. This exercises ticket events as well as protected Handshake packets.
func versionTicketHandshake(t *testing.T, cc, sc *tls.Config, original, selected wire.Version, limit uint64, resume bool, previous uint64, clientOffered ...bool) {
	t.Helper()
	cp, sp := wire.DefaultTransportParameters(), wire.DefaultTransportParameters()
	cp.InitialSourceConnectionID = []byte("client00")
	cp.VersionInformation = &wire.VersionInformation{Chosen: original, Available: []wire.Version{wire.Version1, wire.Version2}}
	sp.InitialSourceConnectionID = []byte("server00")
	sp.OriginalDestinationConnectionID = []byte("original")
	sp.InitialMaxData = limit
	sp.VersionInformation = &wire.VersionInformation{Chosen: original, Available: []wire.Version{wire.Version1, wire.Version2}}
	cb, err := cp.Marshal(false)
	if err != nil {
		t.Fatal(err)
	}
	sb, err := sp.Marshal(true)
	if err != nil {
		t.Fatal(err)
	}
	client, err := New(false, original, cc, cb)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	server, err := NewNegotiatingServer(original, sc, sb, func(wire.TransportParameters) (wire.Version, []byte, error) {
		sp.VersionInformation.Chosen = selected
		data, err := sp.Marshal(true)
		return selected, data, err
	})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	if err := client.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := server.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	ck, _, err := packet.InitialKeys(original, []byte("original"))
	if err != nil {
		t.Fatal(err)
	}
	cn, sn := make(map[tls.QUICEncryptionLevel]uint64), make(map[tls.QUICEncryptionLevel]uint64)
	transfer(t, original, client, server, ck, ck, cn)
	if err := client.SetNegotiatedVersion(selected); err != nil {
		t.Fatal(err)
	}
	ck, sk, err := packet.InitialKeys(selected, []byte("original"))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		n := transfer(t, selected, server, client, sk, sk, sn) + transfer(t, selected, client, server, ck, ck, cn)
		if n == 0 {
			break
		}
	}
	for _, h := range []*TLS{client, server} {
		if !h.Complete() || h.ConnectionState().DidResume != resume {
			t.Fatalf("server=%t complete=%t resumed=%t want=%t", h.server, h.Complete(), h.ConnectionState().DidResume, resume)
		}
		limits, ok := h.ResumedTransport()
		wantRestored := resume
		if !h.server && len(clientOffered) != 0 {
			wantRestored = clientOffered[0]
		}
		if ok != wantRestored || (ok && limits.InitialMaxData != previous) {
			t.Fatalf("server=%t restored=%t max_data=%d want=%d", h.server, ok, limits.InitialMaxData, previous)
		}
		if h.ReadKeys(tls.QUICEncryptionLevelEarly) != nil || h.WriteKeys(tls.QUICEncryptionLevelEarly) != nil {
			t.Fatal("enabled early data")
		}
	}
	checkCachedLimitOwnership(t, cc.ClientSessionCache, selected, sp.EarlyTransport())
}

func TestCompatibleVersionTicketBinding(t *testing.T) {
	for _, original := range []wire.Version{wire.Version1, wire.Version2} {
		selected := wire.Version1
		if original == selected {
			selected = wire.Version2
		}
		t.Run(fmt.Sprintf("%x-to-%x", original, selected), func(t *testing.T) {
			cc, sc := configs(t)
			cc.ClientSessionCache = tls.NewLRUClientSessionCache(4)
			versionTicketHandshake(t, cc, sc, original, original, 8192, false, 0)
			versionTicketHandshake(t, cc, sc, original, selected, 4096, true, 8192)
			versionTicketHandshake(t, cc, sc, selected, selected, 2048, true, 4096)
			// A ticket issued after switching cannot be offered on the original version.
			versionTicketHandshake(t, cc, sc, original, original, 1024, false, 0)
		})
	}
}

// Change only client-local provenance, leaving the encrypted server ticket
// untouched, so the server's independent version check must reject the PSK.
func TestSwitchedTicketServerRejectsWrongOriginalVersion(t *testing.T) {
	for _, original := range []wire.Version{wire.Version1, wire.Version2} {
		selected := wire.Version1
		if original == selected {
			selected = wire.Version2
		}
		t.Run(fmt.Sprintf("%x-to-%x", original, selected), func(t *testing.T) {
			cc, sc := configs(t)
			cc.ClientSessionCache = tls.NewLRUClientSessionCache(4)
			versionTicketHandshake(t, cc, sc, original, selected, 4096, false, 0)
			saved, ok := cc.ClientSessionCache.Get("localhost")
			if !ok {
				t.Fatal("missing ticket")
			}
			ticket, state, err := saved.ResumptionState()
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := state.Bytes()
			if err != nil {
				t.Fatal(err)
			}
			private, err := tls.ParseSessionState(encoded)
			if err != nil {
				t.Fatal(err)
			}
			limits, ok := restoredTicketTransport(private.Extra, selected)
			if !ok {
				t.Fatal("missing negotiated provenance")
			}
			record, err := ticketState(original, limits)
			if err != nil {
				t.Fatal(err)
			}
			private.Extra = [][]byte{record}
			altered, err := tls.NewResumptionState(append([]byte(nil), ticket...), private)
			if err != nil {
				t.Fatal(err)
			}
			cc.ClientSessionCache.Put("localhost", altered)
			versionTicketHandshake(t, cc, sc, original, selected, 2048, false, 4096, true)
		})
	}
}
