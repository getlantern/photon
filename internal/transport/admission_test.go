package transport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/token"
	"github.com/getlantern/photon/internal/wire"
)

func TestRetryAdmission(t *testing.T)           { testRetryAdmission(t, false) }
func TestCompatibleRetryAdmission(t *testing.T) { testRetryAdmission(t, true) }

func testRetryAdmission(t *testing.T, compatible bool) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		selected := v
		var client, unused *Conn
		if compatible {
			selected = wire.Version1
			if selected == v {
				selected = wire.Version2
			}
			client, unused = compatiblePair(t, v, selected)
		} else {
			client, unused = testPair(t, v)
		}
		unused.Close()
		template := Config{Version: v, TLS: unused.config.TLS, Parameters: wire.DefaultTransportParameters()}
		if compatible {
			template.CompatibleVersions = []wire.Version{selected, v}
		}
		codec, _ := token.NewRetryTokens([32]byte{9}, nil, time.Minute)
		addr := netip.MustParseAddrPort("192.0.2.1:1234")
		policyCalls := 0
		template.VerifySourceAddress = func(peer netip.AddrPort) bool {
			policyCalls++
			return true
		}
		calls := 0
		template.ConfigureClient = func(config Config, peer netip.AddrPort) (Config, error) {
			calls++
			if peer != addr || config.ConfigureClient != nil || !config.AddressVerified {
				t.Fatal("selector received wrong peer or recursive callback")
			}
			config.Parameters.InitialMaxData = 12345
			return config, nil
		}
		now := time.Unix(100, 0)
		initial := drain(t, client, now)[0]
		damaged := append([]byte(nil), initial...)
		damaged[len(damaged)-1] ^= 1
		for _, raw := range [][]byte{damaged, initial[:1199], []byte("garbage")} {
			c, response, err := AdmitInitial(context.Background(), template, codec, raw, addr, now)
			if c != nil || response != nil || err != nil {
				t.Fatal("invalid input admitted", err)
			}
		}
		c, retry, err := AdmitInitial(context.Background(), template, codec, initial, addr, now)
		if c != nil || len(retry) == 0 || len(retry) > len(initial) || err != nil {
			t.Fatal("Retry issuance", err)
		}
		header, err := wire.ParseHeader(retry, 0)
		if err != nil || header.Version != v || header.Type != wire.Retry {
			t.Fatal("wrong Retry version", err)
		}
		if err := client.HandleDatagram(retry, now); err != nil {
			t.Fatal(err)
		}
		packets := drain(t, client, now)
		other := netip.MustParseAddrPort("192.0.2.1:1235")
		c, response, err := AdmitInitial(context.Background(), template, codec, packets[0], other, now)
		if c != nil || len(response) == 0 || err != nil {
			t.Fatal("unreadable token did not fall back to Retry", err)
		}
		if calls != 0 || policyCalls != 2 {
			t.Fatal("selector ran before address verification")
		}
		server, response, err := AdmitInitial(context.Background(), template, codec, packets[0], addr, now)
		if err != nil || server == nil || response != nil {
			t.Fatal("admission failed", err)
		}
		defer server.Close()
		if calls != 1 || policyCalls != 2 || server.config.VerifySourceAddress != nil || server.config.ConfigureClient != nil || server.config.Parameters.InitialMaxData != 12345 || template.Parameters.InitialMaxData == 12345 {
			t.Fatal("selected configuration or callback lifetime", calls)
		}
		if !server.budget.Validated() {
			t.Fatal("verified token did not validate address")
		}
		for _, raw := range packets[1:] {
			if err := server.HandleDatagram(raw, now); err != nil {
				t.Fatal(err)
			}
		}
		drivePair(t, client, server, false, false)
		if client.peerParams == nil || client.peerParams.InitialMaxData != 12345 {
			t.Fatal("selected receive credit not advertised to client")
		}
		if client.config.Version != selected || server.config.Version != selected {
			t.Fatal("wrong admitted negotiated version")
		}
		if !client.confirmed || !server.confirmed {
			t.Fatal("admitted handshake did not complete")
		}
	}
}

func TestAdmissionSelectionRefusal(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(fmt.Sprintf("%x", version), func(t *testing.T) {
			client, unused := testPair(t, version)
			unused.Close()
			localErr := errors.New("private admission policy detail")
			calls := 0
			template := Config{Version: version, TLS: unused.config.TLS, Parameters: wire.DefaultTransportParameters()}
			template.ConfigureClient = func(cfg Config, peer netip.AddrPort) (Config, error) {
				calls++
				return cfg, localErr
			}
			codec, err := token.NewRetryTokens([32]byte{9}, nil, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			peer := netip.MustParseAddrPort("192.0.2.1:1234")
			now := time.Unix(100, 0)
			initial := drain(t, client, now)[0]
			conn, retry, err := AdmitInitial(context.Background(), template, codec, initial, peer, now)
			if err != nil || conn != nil || len(retry) == 0 || calls != 0 {
				t.Fatal("unverified admission", conn, err, calls)
			}
			if err := client.HandleDatagram(retry, now); err != nil {
				t.Fatal(err)
			}
			initial = drain(t, client, now)[0]
			var previous []byte
			for i := 0; i < 2; i++ {
				conn, response, err := AdmitInitial(context.Background(), template, codec, initial, peer, now)
				if conn != nil || !errors.Is(err, localErr) || len(response) == 0 || len(response) > len(initial) {
					t.Fatal("refusal", conn, err, len(response))
				}
				if i > 0 && !bytes.Equal(previous, response) {
					t.Fatal("repeated refusal changed protected packet")
				}
				previous = response
			}
			if calls != 2 {
				t.Fatal("selector count", calls)
			}
			err = client.HandleDatagram(previous, now)
			var closeErr *ConnectionTransportError
			if !errors.As(err, &closeErr) || !closeErr.Remote || closeErr.ErrorCode != 0x2 || closeErr.ErrorMessage != "" {
				t.Fatal("wire refusal", err)
			}
		})
	}
}
