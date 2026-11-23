package handshake

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"testing"

	"github.com/getlantern/photon/internal/packet"
	"github.com/getlantern/photon/internal/wire"
)

func TestTicketLimitRecord(t *testing.T) {
	limits := wire.DefaultTransportParameters().EarlyTransport()
	limits.ResetStreamAt = true
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		record, err := ticketState(v, limits)
		if err != nil {
			t.Fatal(err)
		}
		extra := [][]byte{[]byte("application metadata"), record}
		got, ok := restoredTicketTransport(extra, v)
		if !ok || got != limits {
			t.Fatal("lost record")
		}
		if !bytes.Equal(extra[0], []byte("application metadata")) {
			t.Fatal("modified application entry")
		}
		other := wire.Version1
		if v == other {
			other = wire.Version2
		}
		if _, ok := restoredTicketTransport(extra, other); ok {
			t.Fatal("cross-version record accepted")
		}
		if _, ok := restoredTicketTransport(append(extra, record), v); ok {
			t.Fatal("duplicate record accepted")
		}
		for n := len(ticketStatePrefix); n < len(record); n++ {
			if _, ok := restoredTicketTransport([][]byte{record[:n]}, v); ok {
				t.Fatal("truncated record accepted", n)
			}
		}
		if _, ok := restoredTicketTransport([][]byte{append(bytes.Clone(record), 0)}, v); ok {
			t.Fatal("trailing data accepted")
		}
	}
	if _, ok := restoredTicketTransport([][]byte{[]byte("unrelated")}, wire.Version1); ok {
		t.Fatal("absent record accepted")
	}
}

func TestTLSRestoresTicketLimits(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, mode := range []string{"preserved", "missing", "malformed", "duplicate", "cross-version", "legacy"} {
			t.Run(fmt.Sprintf("%x/%s", uint32(v), mode), func(t *testing.T) {
				cc, sc := configs(t)
				cc.ClientSessionCache = tls.NewLRUClientSessionCache(4)
				sc.WrapSession = func(cs tls.ConnectionState, ss *tls.SessionState) ([]byte, error) {
					switch mode {
					case "legacy":
						old, err := wire.DefaultTransportParameters().EarlyLimits().Marshal()
						if err != nil {
							return nil, err
						}
						ss.Extra[0] = append(ss.Extra[0][:len(ticketStatePrefix)+4], old...)
					case "missing":
						ss.Extra = nil
					case "malformed":
						ss.Extra[0] = append(ss.Extra[0], 0)
					case "duplicate":
						ss.Extra = append(ss.Extra, bytes.Clone(ss.Extra[0]))
					case "cross-version":
						other := wire.Version1
						if v == other {
							other = wire.Version2
						}
						record, err := ticketState(other, wire.DefaultTransportParameters().EarlyTransport())
						if err != nil {
							return nil, err
						}
						ss.Extra = [][]byte{record}
					}
					return sc.EncryptTicket(cs, ss)
				}
				cp, sp := wire.DefaultTransportParameters(), wire.DefaultTransportParameters()
				cp.InitialSourceConnectionID = []byte("client00")
				sp.InitialSourceConnectionID = []byte("server00")
				sp.OriginalDestinationConnectionID = []byte("original")
				sp.InitialMaxData = 8192
				sp.MaxIdleTimeout = 60000
				sp.MaxUDPPayloadSize = 1452
				sp.DisableActiveMigration = true
				previous := sp.EarlyTransport()
				for round := 0; round < 3; round++ {
					if round == 1 {
						sp.InitialMaxData = 4096
						sp.MaxIdleTimeout = 30000
						sp.MaxUDPPayloadSize = 1200
						sp.DisableActiveMigration = false
					}
					cb, err := cp.Marshal(false)
					if err != nil {
						t.Fatal(err)
					}
					sb, err := sp.Marshal(true)
					if err != nil {
						t.Fatal(err)
					}
					client, err := New(false, v, cc, cb)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { client.Close() })
					server, err := New(true, v, sc, sb)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { server.Close() })
					if err := client.Start(context.Background()); err != nil {
						t.Fatal(err)
					}
					if err := server.Start(context.Background()); err != nil {
						t.Fatal(err)
					}
					ck, sk, err := packet.InitialKeys(v, []byte("original"))
					if err != nil {
						t.Fatal(err)
					}
					cn, sn := make(map[tls.QUICEncryptionLevel]uint64), make(map[tls.QUICEncryptionLevel]uint64)
					for i := 0; i < 20; i++ {
						n := transfer(t, v, client, server, ck, ck, cn) + transfer(t, v, server, client, sk, sk, sn)
						if n == 0 {
							break
						}
					}
					wantResume := round > 0 && mode != "missing" && mode != "duplicate" && mode != "cross-version"
					if !client.Complete() || !server.Complete() || client.ConnectionState().DidResume != wantResume || server.ConnectionState().DidResume != wantResume {
						t.Fatal("handshake/resumption state", round)
					}
					want := previous
					if round == 2 {
						want = sp.EarlyTransport()
					}
					clientLimits, clientOK := client.ResumedTransport()
					if clientOK != (round > 0) || (clientOK && clientLimits != want) {
						t.Fatal("client did not restore issuing connection limits", round, clientLimits)
					}
					got, ok := server.ResumedTransport()
					if round == 0 && ok {
						t.Fatal("fresh handshake has restored limits")
					}
					if round == 1 && mode == "preserved" && (!ok || got != previous || sp.EarlyTransport().EarlyLimits.Supports(got.EarlyLimits)) {
						t.Fatal("old ticket credit not preserved")
					}
					if mode != "preserved" && ok {
						t.Fatal("unusable metadata restored")
					}
					if client.WriteKeys(tls.QUICEncryptionLevelEarly) != nil || server.ReadKeys(tls.QUICEncryptionLevelEarly) != nil {
						t.Fatal("early data enabled")
					}
					checkCachedLimitOwnership(t, cc.ClientSessionCache, v, sp.EarlyTransport())
					client.Close()
					server.Close()
				}

			})
		}
	}
}

// Exercise the same stored object concurrently, including its opaque ticket and
// Extra slices. Mutating one connection's decoded copy must not affect others.
func checkCachedLimitOwnership(t *testing.T, cache tls.ClientSessionCache, v wire.Version, want wire.EarlyTransport) {
	t.Helper()
	saved, ok := cache.Get("localhost")
	if !ok {
		t.Fatal("missing cached session")
	}
	ticket, state, err := saved.ResumptionState()
	if err != nil {
		t.Fatal(err)
	}
	before, err := state.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	ticketBefore := bytes.Clone(ticket)
	other := wire.Version1
	if v == other {
		other = wire.Version2
	}
	if _, ok := (resumeCache{ClientSessionCache: cache, version: other}).Get("localhost"); ok {
		t.Fatal("offered ticket across versions")
	}

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			private, ok := (resumeCache{ClientSessionCache: cache, version: v}).Get("localhost")
			if !ok {
				t.Error("cache copy failed")
				return
			}
			pt, ps, err := private.ResumptionState()
			if err != nil {
				t.Error(err)
				return
			}
			got, ok := restoredTicketTransport(ps.Extra, v)
			if !ok || got != want {
				t.Error("cached limits do not match issuing connection")
				return
			}
			for _, extra := range ps.Extra {
				clear(extra)
			}
			clear(pt)
			ps.EarlyData = true
		}()
	}
	wg.Wait()
	after, err := state.Bytes()
	if err != nil || !bytes.Equal(before, after) || !bytes.Equal(ticketBefore, ticket) {
		t.Fatal("shared cache state was mutated", err)
	}
}

func TestTicketVersionBinding(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		record, err := ticketState(v, wire.DefaultTransportParameters().EarlyTransport())
		if err != nil {
			t.Fatal(err)
		}
		if !ticketVersionMatches([][]byte{[]byte("application"), record}, v) {
			t.Fatal("lost version")
		}
		for n := len(ticketStatePrefix); n < len(ticketStatePrefix)+4; n++ {
			if ticketVersionMatches([][]byte{record[:n]}, v) {
				t.Fatal("truncated origin accepted")
			}
		}
		if ticketVersionMatches(nil, v) || ticketVersionMatches([][]byte{[]byte("unrelated")}, v) || ticketVersionMatches([][]byte{record, record}, v) {
			t.Fatal("unproven origin accepted")
		}
		if ticketVersionMatches([][]byte{record}, wire.Version(42)) {
			t.Fatal("unsupported origin accepted")
		}
		// The transport payload may be an older schema: version provenance still
		// permits 1-RTT, while the transport decoder rejects it for early data.
		legacy := append(bytes.Clone(record[:len(ticketStatePrefix)+4]), 0xff)
		if !ticketVersionMatches([][]byte{legacy}, v) {
			t.Fatal("schema upgrade prevented 1-RTT")
		}
		if _, ok := restoredTicketTransport([][]byte{legacy}, v); ok {
			t.Fatal("unknown transport schema accepted")
		}
	}
}
