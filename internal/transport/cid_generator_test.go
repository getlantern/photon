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

func TestCIDGeneratorOwnershipAndFailures(t *testing.T) {
	buffer := []byte("custom01")
	ids := newLocalIDs([]byte("initial0"))
	ids.generate = func() ([]byte, error) { return buffer, nil }
	frames, err := ids.issue(2)
	if err != nil || len(frames) != 1 {
		t.Fatal(err)
	}
	buffer[7] = '2'
	if string(frames[0].ConnectionID) != "custom01" || string(ids.active[1].cid) != "custom01" {
		t.Fatal("retained generator storage")
	}
	frames, err = ids.issue(3)
	if err != nil || len(frames) != 1 {
		t.Fatal(err)
	}
	before := len(ids.active)
	if _, err := ids.issue(4); err == nil || len(ids.active) != before {
		t.Fatal("duplicate generator output accepted")
	}
	ids.generate = func() ([]byte, error) { return []byte("short"), nil }
	if _, err := ids.issue(4); err == nil || len(ids.active) != before {
		t.Fatal("wrong length accepted")
	}
	sentinel := errors.New("generator failure")
	ids.generate = func() ([]byte, error) { return nil, sentinel }
	if _, err := ids.issue(4); !errors.Is(err, sentinel) {
		t.Fatal("lost generator error", err)
	}
	addresses := []netip.AddrPort{netip.MustParseAddrPort("192.0.2.1:443")}
	if _, err := makePreferredAddressWithGenerator(addresses, []byte("initial0"), func() ([]byte, error) { return []byte("initial0"), nil }); err == nil {
		t.Fatal("preferred duplicate escaped bound")
	}
}

func TestCIDGeneratorAdmissionAndMultipath(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			client, unused := testPair(t, version)
			defer client.Close()
			unused.Close()
			buffer := []byte("custom00")
			calls := 0
			generate := func() ([]byte, error) { calls++; buffer[7] = byte(calls); return buffer, nil }
			template := Config{AdmissionCIDLength: 8, Version: version, TLS: unused.config.TLS, Parameters: wire.DefaultTransportParameters(), GenerateConnectionID: generate, VerifySourceAddress: func(netip.AddrPort) bool { return false }}
			template.ConfigureClient = func(c Config, _ netip.AddrPort) (Config, error) {
				c.GenerateConnectionID = func() ([]byte, error) { return nil, errors.New("selector replaced CID policy") }
				return c, nil
			}
			retry, _ := token.NewRetryTokens([32]byte{9}, nil, time.Minute)
			now := time.Now()
			packets := drain(t, client, now)
			for _, generateBad := range []func() ([]byte, error){
				func() ([]byte, error) { return []byte("bad"), nil },
				func() ([]byte, error) { return nil, errors.New("initial generator failure") },
			} {
				bad := template
				bad.GenerateConnectionID = generateBad
				c, response, err := AdmitInitial(context.Background(), bad, retry, packets[0], netip.MustParseAddrPort("192.0.2.1:443"), now)
				if err == nil || c != nil || response != nil {
					t.Fatal("invalid initial generator allocated connection", err)
				}
			}
			server, response, err := AdmitInitial(context.Background(), template, retry, packets[0], netip.MustParseAddrPort("192.0.2.1:443"), now)
			if err != nil || server == nil || response != nil {
				t.Fatal("admission", err)
			}
			defer server.Close()
			initial := append([]byte(nil), server.config.LocalCID...)
			for _, b := range packets[1:] {
				if err := server.HandleDatagram(b, now); err != nil {
					t.Fatal(err)
				}
			}
			drivePair(t, client, server, false, false)
			if !bytes.Equal(server.config.LocalCID, initial) || calls < 2 {
				t.Fatal("initial ownership or replacement issuance")
			}
			p, err := adoptPathZero(server, 3, 3, 4)
			if err != nil {
				t.Fatal(err)
			}
			frames, err := p.issue(1)
			if err != nil || len(frames) == 0 {
				t.Fatal("multipath generation", err)
			}
			for _, f := range frames {
				if !bytes.HasPrefix(f.ConnectionID, []byte("custom")) {
					t.Fatal("multipath lost generator")
				}
			}
			preferred, err := makePreferredAddressWithGenerator([]netip.AddrPort{netip.MustParseAddrPort("192.0.2.1:443")}, initial, generate)
			if err != nil || !bytes.HasPrefix(preferred.ConnectionID, []byte("custom")) {
				t.Fatal("preferred generation", err)
			}
		})
	}
}
