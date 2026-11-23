package transport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/handshake"
	"github.com/getlantern/photon/internal/wire"
)

// Inject authenticated preferred-address parameters through TLS. Advertising
// and serving this address is still intentionally unavailable in NewConn.
func preferredAddressPair(t *testing.T, version wire.Version, override ...wire.PreferredAddress) (*Conn, *Conn, wire.PreferredAddress) {
	t.Helper()
	params := wire.DefaultTransportParameters()
	params.ActiveConnectionIDLimit = 2
	client, server := testPair(t, version, params)
	address := wire.PreferredAddress{IPv4: [4]byte{127, 0, 0, 1}, IPv4Port: 8443, ConnectionID: []byte("prefer01"), StatelessResetToken: [16]byte{9, 8, 7}}
	if len(override) != 0 {
		address = override[0]
	}
	advertised := server.config.Parameters
	advertised.PreferredAddress = &address
	server.config.Parameters = advertised
	encoded, err := advertised.Marshal(true)
	if err != nil {
		t.Fatal(err)
	}
	server.tls.Close()
	server.tls, err = handshake.New(true, version, server.config.TLS, encoded)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.tls.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Mirror issuance at the fixture server so it doesn't reuse sequence 1.
	if err := server.localIDs.reservePreferred(&address); err != nil {
		t.Fatal(err)
	}
	drivePair(t, client, server, false, false)
	return client, server, address
}

func TestPreferredAddressCIDAccounting(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, scenario := range []string{"limit", "conflicting duplicate", "retirement", "reset lifecycle", "path adoption"} {
			t.Run(fmt.Sprintf("%x/%s", uint32(v), scenario), func(t *testing.T) {
				client, server, address := preferredAddressPair(t, v)
				id, ok := client.peerIDs.active[1]
				if !ok || !bytes.Equal(id.cid, address.ConnectionID) || !id.hasToken || id.token != address.StatelessResetToken || id.used || client.peerIDs.current != 0 {
					t.Fatal("preferred CID was not registered as unused sequence 1")
				}
				if client.isStatelessReset(resetDatagram(address.StatelessResetToken)) {
					t.Fatal("unused preferred token accepted")
				}
				frame := wire.NewConnectionIDFrame{Sequence: 1, ConnectionID: address.ConnectionID, StatelessResetToken: address.StatelessResetToken}
				now := time.Unix(100, 0).Add(time.Second)
				receive := func(f wire.NewConnectionIDFrame, pn uint64) error {
					return client.HandleDatagram(makePeerPacket(t, server, applicationSpace, pn, f), now)
				}
				if err := receive(frame, 1000); err != nil {
					t.Fatal("matching duplicate", err)
				}
				switch scenario {
				case "limit":
					frame.Sequence = 2
					frame.ConnectionID = []byte("extra002")
					var te *TransportError
					if err := receive(frame, 1001); !errors.As(err, &te) || te.Code != 9 {
						t.Fatal("preferred CID excluded from active limit", err)
					}
				case "conflicting duplicate":
					frame.StatelessResetToken[0] ^= 1
					var te *TransportError
					if err := receive(frame, 1001); !errors.As(err, &te) || te.Code != 0xa {
						t.Fatal("conflicting token accepted", err)
					}
				case "retirement":
					replacement := wire.NewConnectionIDFrame{Sequence: 2, RetirePriorTo: 2, ConnectionID: []byte("extra002")}
					if err := receive(replacement, 1001); err != nil {
						t.Fatal(err)
					}
					if err := receive(frame, 1002); err != nil {
						t.Fatal("retired duplicate", err)
					}
					if _, ok := client.peerIDs.active[1]; ok {
						t.Fatal("retired preferred CID revived")
					}
				case "reset lifecycle":
					if err := client.RotatePeerID(); err != nil {
						t.Fatal(err)
					}
					if _, err := client.NextDatagram(now); err != nil {
						t.Fatal(err)
					}
					if !client.isStatelessReset(resetDatagram(address.StatelessResetToken)) {
						t.Fatal("used preferred token unavailable")
					}
					replacement := wire.NewConnectionIDFrame{Sequence: 2, RetirePriorTo: 2, ConnectionID: []byte("extra002")}
					if err := receive(replacement, 1001); err != nil {
						t.Fatal(err)
					}
					if client.isStatelessReset(resetDatagram(address.StatelessResetToken)) {
						t.Fatal("retired token remained active")
					}
				case "path adoption":
					paths, err := adoptPathZero(client, 1, 1, 2)
					if err != nil {
						t.Fatal(err)
					}
					binding := pathCIDBinding{sequence: 1}
					if paths.peerByCID[string(address.ConnectionID)] != binding || paths.peerHistory[binding].token != address.StatelessResetToken {
						t.Fatal("preferred CID missing from path history")
					}
				}
			})
		}
	}
}
