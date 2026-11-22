package transport

import (
	"context"
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/token"
	"github.com/getlantern/photon/internal/wire"
)

func TestAdmissionWithoutRetry(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(fmt.Sprintf("%x", version), func(t *testing.T) {
			client, unused := testPair(t, version)
			defer client.Close()
			unused.Close()
			peer := netip.MustParseAddrPort("192.0.2.1:1234")
			policyCalls, selectCalls := 0, 0
			template := Config{Version: version, TLS: unused.config.TLS, Parameters: wire.DefaultTransportParameters()}
			template.VerifySourceAddress = func(addr netip.AddrPort) bool {
				policyCalls++
				if addr != peer {
					t.Fatal("incorrect source")
				}
				return false
			}
			template.ConfigureClient = func(cfg Config, addr netip.AddrPort) (Config, error) {
				selectCalls++
				if cfg.AddressVerified || addr != peer {
					t.Fatal("unverified source marked verified")
				}
				// Selection cannot manufacture validation or retain admission callbacks.
				cfg.AddressVerified = true
				cfg.VerifySourceAddress = template.VerifySourceAddress
				return cfg, nil
			}
			codec, err := token.NewRetryTokens([32]byte{9}, nil, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now()
			initial := drain(t, client, now)[0]
			damaged := append([]byte(nil), initial...)
			damaged[len(damaged)-1] ^= 1
			c, response, err := AdmitInitial(context.Background(), template, codec, damaged, peer, now)
			if err != nil || c != nil || response != nil || policyCalls != 0 || selectCalls != 0 {
				t.Fatal("invalid Initial reached policy", err)
			}
			server, response, err := AdmitInitial(context.Background(), template, codec, initial, peer, now)
			if err != nil || server == nil || response != nil {
				t.Fatal("admission", err)
			}
			defer server.Close()
			if policyCalls != 1 || selectCalls != 1 || server.budget.Validated() || server.config.AddressVerified || server.config.VerifySourceAddress != nil || server.config.ConfigureClient != nil {
				t.Fatal("admission validation or callback lifetime")
			}
			if server.budget.credit != uint64(3*len(initial)) {
				t.Fatal("incorrect receive credit", server.budget.credit)
			}
			if len(server.config.RetrySourceCID) != 0 {
				t.Fatal("fabricated Retry")
			}
			// Exhaust credit with a queued TLS flight, proving admission doesn't bypass
			// the existing send gate or consume packet numbers while blocked.
			credit := server.budget.credit
			server.budget.credit = 0
			pn := server.spaces[initialSpace].nextPN
			if b, err := server.NextDatagram(now); err != nil || b != nil || server.spaces[initialSpace].nextPN != pn {
				t.Fatal("unvalidated send escaped budget", err)
			}
			server.budget.credit = credit
			sent := 0
			for _, b := range drain(t, server, now) {
				sent += len(b)
				if err := client.HandleDatagram(b, now); err != nil {
					t.Fatal(err)
				}
			}
			if sent == 0 || sent > 3*len(initial) || server.budget.Validated() {
				t.Fatal("invalid first flight", sent)
			}
			drivePair(t, client, server, false, false)
			if !server.budget.Validated() || !server.confirmed || !client.confirmed {
				t.Fatal("Handshake did not validate source")
			}
		})
	}
}
