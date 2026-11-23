package handshake

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"testing"

	"github.com/getlantern/photon/internal/packet"
	"github.com/getlantern/photon/internal/wire"
)

func TestTLSEarlyDataPolicy(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, mode := range []string{"accept", "server-disabled", "client-disabled", "reduced-credit", "invalid-acceptance", "malformed-ticket", "ticket-disabled", "reset-preserved", "reset-enabled", "reset-disabled", "reset-invalid-acceptance", "legacy-transport"} {
			t.Run(fmt.Sprintf("%x/%s", v, mode), func(t *testing.T) {
				cc, sc := configs(t)
				cc.ClientSessionCache = tls.NewLRUClientSessionCache(4)
				if mode == "malformed-ticket" || mode == "legacy-transport" {
					sc.WrapSession = func(cs tls.ConnectionState, state *tls.SessionState) ([]byte, error) {
						if mode == "legacy-transport" {
							// Original transport schema had no reset-support flag.
							record := state.Extra[0]
							record[len(ticketStatePrefix)+4] = 2
							state.Extra[0] = record[:len(record)-1]
						} else {
							state.Extra[0] = append(state.Extra[0], 0)
						}
						return sc.EncryptTicket(cs, state)
					}
				}
				cp, sp := wire.DefaultTransportParameters(), wire.DefaultTransportParameters()
				cp.InitialSourceConnectionID = []byte("client00")
				sp.InitialSourceConnectionID = []byte("server00")
				sp.OriginalDestinationConnectionID = []byte("original")
				sp.ResetStreamAt = mode == "reset-preserved" || mode == "reset-disabled" || mode == "reset-invalid-acceptance"
				previousReset := sp.ResetStreamAt
				sp.InitialMaxData = 8192
				sp.InitialMaxStreamDataBidiRemote = 4096
				sp.InitialMaxStreamsBidi = 2
				for round := 0; round < 2; round++ {
					if round == 1 && (mode == "reduced-credit" || mode == "invalid-acceptance") {
						sp.InitialMaxData = 4096
					}
					if round == 1 {
						switch mode {
						case "reset-disabled", "reset-invalid-acceptance":
							sp.ResetStreamAt = false
						case "reset-enabled":
							sp.ResetStreamAt = true
						}
					}
					cb, err := cp.Marshal(false)
					if err != nil {
						t.Fatal(err)
					}
					sb, err := sp.Marshal(true)
					if err != nil {
						t.Fatal(err)
					}
					clientNew, serverNew := NewWithEarlyData, NewWithEarlyData
					if round == 1 && mode == "client-disabled" {
						clientNew = New
					}
					if (round == 1 && mode == "server-disabled") || (round == 0 && mode == "ticket-disabled") {
						serverNew = New
					}
					client, err := clientNew(false, v, cc, cb)
					if err != nil {
						t.Fatal(err)
					}
					server, err := serverNew(true, v, sc, sb)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { client.Close(); server.Close() })
					if round == 1 && mode == "invalid-acceptance" {
						// Simulate a peer accepting against old credit while
						// advertising smaller limits in this handshake.
						server.localTransport.InitialMaxData = 8192
					}
					if round == 1 && mode == "reset-invalid-acceptance" {
						// Misbehaving peer accepts against a retained capability
						// while omitting it from the actual TLS parameters.
						server.localTransport.ResetStreamAt = true
					}
					var savedState *tls.SessionState
					var savedBytes []byte
					if round == 1 {
						cached, ok := cc.ClientSessionCache.Get("localhost")
						if !ok {
							t.Fatal("missing ticket")
						}
						_, savedState, err = cached.ResumptionState()
						if err != nil {
							t.Fatal(err)
						}
						savedBytes, err = savedState.Bytes()
						if err != nil {
							t.Fatal(err)
						}
					}
					if err := client.Start(context.Background()); err != nil {
						t.Fatal(err)
					}
					if err := server.Start(context.Background()); err != nil {
						t.Fatal(err)
					}
					if savedState != nil {
						after, err := savedState.Bytes()
						if err != nil || !bytes.Equal(after, savedBytes) {
							t.Fatal("cache snapshot mutated on resume", err)
						}
					}
					offered := round == 1 && mode != "client-disabled" && mode != "ticket-disabled"
					earlyWrite := client.WriteKeys(tls.QUICEncryptionLevelEarly)
					if (earlyWrite != nil) != offered {
						t.Fatal("early write key offer", round)
					}
					if round == 1 {
						remembered, ok := client.ResumedTransport()
						if !ok || remembered.ResetStreamAt != previousReset {
							t.Fatal("client lost remembered reset capability")
						}
					}
					ck, sk, err := packet.InitialKeys(v, []byte("original"))
					if err != nil {
						t.Fatal(err)
					}
					cn, sn := make(map[tls.QUICEncryptionLevel]uint64), make(map[tls.QUICEncryptionLevel]uint64)
					transfer(t, v, client, server, ck, ck, cn)
					if round == 1 && mode != "malformed-ticket" {
						remembered, ok := server.ResumedTransport()
						if !ok || remembered.ResetStreamAt != previousReset {
							t.Fatal("server lost remembered reset capability")
						}
					}
					accepted := round == 1 && (mode == "accept" || mode == "invalid-acceptance" || mode == "reset-preserved" || mode == "reset-enabled" || mode == "reset-invalid-acceptance" || mode == "legacy-transport")
					earlyRead := server.ReadKeys(tls.QUICEncryptionLevelEarly)
					if (earlyRead != nil) != accepted || server.EarlyDataAccepted() != accepted {
						t.Fatal("server acceptance", round)
					}
					if accepted {
						if client.Complete() {
							t.Fatal("early exchange happened after client handshake")
						}
						payload, err := (wire.StreamFrame{StreamID: 0, Data: []byte("early payload"), Fin: true}).Append(nil)
						if err != nil {
							t.Fatal(err)
						}
						header, err := wire.AppendLongHeader(nil, v, wire.ZeroRTT, []byte("original"), []byte("client00"), nil, 0, 4, len(payload)+16)
						if err != nil {
							t.Fatal(err)
						}
						raw, err := earlyWrite.Seal(header, payload, 0)
						if err != nil {
							t.Fatal(err)
						}
						parsed, err := wire.ParseHeader(raw, 0)
						if err != nil {
							t.Fatal(err)
						}
						_, plain, pn, err := earlyRead.Open(raw, parsed.PNOffset, 0)
						if err != nil || pn != 0 || !bytes.Equal(plain, payload) {
							t.Fatal("early packet protection", err)
						}
					}
					if round == 1 && (mode == "invalid-acceptance" || mode == "reset-invalid-acceptance") {
						var failure error
						for _, flight := range server.Drain() {
							if failure = client.HandleCrypto(flight.Level, flight.Offset, flight.Data); failure != nil {
								break
							}
						}
						if failure != wire.ErrEarlyLimits || client.Complete() || client.EarlyDataAccepted() {
							t.Fatal("accepted reduced server capabilities", failure)
						}
						return
					}
					for i := 0; i < 20; i++ {
						if transfer(t, v, server, client, sk, sk, sn)+transfer(t, v, client, server, ck, ck, cn) == 0 {
							break
						}
					}
					if !client.Complete() || !server.Complete() || client.ConnectionState().DidResume != (round == 1) {
						t.Fatal("handshake/resumption")
					}
					if client.EarlyDataAccepted() != accepted || client.EarlyDataRejected() != (offered && !accepted) {
						t.Fatal("client early outcome")
					}
					if client.EarlyDataRejected() && client.WriteKeys(tls.QUICEncryptionLevelEarly) != nil {
						t.Fatal("rejected early key retained")
					}
					client.DiscardKeys(tls.QUICEncryptionLevelEarly)
					server.DiscardKeys(tls.QUICEncryptionLevelEarly)
					if client.WriteKeys(tls.QUICEncryptionLevelEarly) != nil || server.ReadKeys(tls.QUICEncryptionLevelEarly) != nil {
						t.Fatal("early keys survived discard")
					}
					client.Close()
					server.Close()
				}
			})
		}
	}
}
