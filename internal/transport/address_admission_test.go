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

func TestAddressTokenAdmission(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, mode := range []string{"valid", "changed-port", "wrong-ip", "wrong-version", "expired", "tampered", "tampered-kind", "tampered-schema", "truncated", "foreign", "retired"} {
			t.Run(fmt.Sprintf("%x/%s", v, mode), func(t *testing.T) {
				client, unused := testPair(t, v)
				unused.Close()
				codec, _ := token.NewAddressTokens([32]byte{7}, nil, time.Hour)
				retries, _ := token.NewRetryTokens([32]byte{8}, nil, time.Minute)
				template := Config{Version: v, TLS: unused.config.TLS, Parameters: wire.DefaultTransportParameters(), AddressTokens: codec}
				now := time.Unix(10000, 0)
				address := netip.MustParseAddrPort("192.0.2.1:1234")
				issuedVersion := v
				if mode == "wrong-version" {
					issuedVersion = wire.Version1
					if issuedVersion == v {
						issuedVersion = wire.Version2
					}
				}
				opaque, err := codec.Issue(address, issuedVersion, now)
				if err != nil {
					t.Fatal(err)
				}
				switch mode {
				case "changed-port":
					address = netip.MustParseAddrPort("192.0.2.1:5678")
				case "wrong-ip":
					address = netip.MustParseAddrPort("192.0.2.2:1234")
				case "expired":
					now = now.Add(time.Hour)
				case "tampered":
					opaque[len(opaque)-1] ^= 1
				case "tampered-kind":
					opaque[0] ^= 1
				case "tampered-schema":
					opaque[1] ^= 1
				case "truncated":
					opaque = opaque[:len(opaque)-1]
				case "foreign":
					opaque = make([]byte, 64)
				case "retired":
					template.AddressTokens = nil
				}
				calls := 0
				template.ConfigureClient = func(cfg Config, peer netip.AddrPort) (Config, error) {
					calls++
					if peer != address {
						t.Fatal("selector peer", peer)
					}
					cfg.Version = 0         // Admission owns the selected wire version.
					cfg.AddressTokens = nil // Token policy belongs to the listener.
					return cfg, nil
				}
				// Initial serialization already supports the token slot. Client cache/API
				// integration is separate; this fixture supplies the previously issued value.
				client.retryToken = opaque
				packets := drain(t, client, now)
				server, response, err := AdmitInitial(context.Background(), template, retries, packets[0], address, now)
				valid := mode == "valid" || mode == "changed-port"
				if err != nil {
					t.Fatal(err)
				}
				if valid {
					if server == nil || response != nil || !server.budget.Validated() {
						t.Fatal("valid token not admitted")
					}
					if server.config.RetrySourceCID != nil {
						t.Fatal("NEW_TOKEN treated as Retry")
					}
				} else {
					if calls != 0 {
						t.Fatal("selector ran for invalid token")
					}
					if server != nil || len(response) == 0 {
						t.Fatal("invalid NEW_TOKEN did not fall back to Retry")
					}
					if err := client.HandleDatagram(response, now); err != nil {
						t.Fatal(err)
					}
					packets = drain(t, client, now)
					server, response, err = AdmitInitial(context.Background(), template, retries, packets[0], address, now)
					if err != nil || server == nil || response != nil {
						t.Fatal("Retry fallback failed", err)
					}
				}
				defer server.Close()
				if calls != 1 || server.config.Version != v || server.config.AddressTokens != template.AddressTokens || server.config.ConfigureClient != nil {
					t.Fatal("admission identity or selection lifetime", calls)
				}
				for _, p := range packets[1:] {
					if err := server.HandleDatagram(p, now); err != nil {
						t.Fatal(err)
					}
				}
				drivePair(t, client, server, false, false)
				if !client.confirmed || !server.confirmed {
					t.Fatal("handshake not confirmed")
				}
			})
		}
	}
}
