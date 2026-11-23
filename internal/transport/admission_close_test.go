package transport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/packet"
	"github.com/getlantern/photon/internal/token"
	"github.com/getlantern/photon/internal/wire"
)

func TestExpiredRetryStatelessClose(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(fmt.Sprintf("%x", v), func(t *testing.T) {
			client, unused := testPair(t, v)
			unused.Close()
			cfg := Config{Version: v, TLS: unused.config.TLS, Parameters: wire.DefaultTransportParameters()}
			codec, err := token.NewRetryTokens([32]byte{7}, nil, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			address := netip.MustParseAddrPort("192.0.2.1:1234")
			now := time.Unix(100, 0)
			initial := drain(t, client, now)[0]
			_, retry, err := AdmitInitial(context.Background(), cfg, codec, initial, address, now)
			if err != nil || len(retry) == 0 {
				t.Fatal(err)
			}
			if err := client.HandleDatagram(retry, now); err != nil {
				t.Fatal(err)
			}
			second := drain(t, client, now)[0]
			conn, response, err := AdmitInitial(context.Background(), cfg, codec, second, address, now.Add(time.Second))
			if err != nil || conn != nil || len(response) == 0 || len(response) > len(second) {
				t.Fatal("stateless bounded close", err)
			}
			header, err := wire.ParseHeader(response, 0)
			if err != nil || header.Type != wire.Initial || header.Version != v || len(header.Token) != 0 {
				t.Fatal("close header", header, err)
			}
			original, err := wire.ParseHeader(second, 0)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(header.DCID, original.SCID) || !bytes.Equal(header.SCID, original.DCID) {
				t.Fatal("close connection IDs")
			}
			_, serverKeys, err := packet.InitialKeys(v, original.DCID)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, _, err := serverKeys.Open(response, header.PNOffset, 0); err != nil {
				t.Fatal("close encryption", err)
			}
			endpoint, err := NewServerEndpoint(cfg, codec, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer endpoint.Close()
			accepted, out, err := endpoint.HandleDatagram(context.Background(), second, address, now.Add(time.Second))
			if err != nil || accepted != nil || out == nil || out.Peer != address || out.Conn != nil || !bytes.Equal(out.Data, response) {
				t.Fatal("endpoint stateless response", err)
			}
			if len(endpoint.conns) != 0 || len(endpoint.router.entries) != 0 || endpoint.ready.Len() != 0 {
				t.Fatal("invalid token consumed endpoint capacity")
			}
			// The client must observe the actual wire error instead of timing out.
			err = client.HandleDatagram(response, now.Add(time.Second))
			var closed *PeerCloseError
			if !errors.As(err, &closed) || closed.Application || closed.Code != 0xb {
				t.Fatalf("client close: %v", err)
			}
			// No allocated server connection is needed for repeated requests.
			again, repeated, err := AdmitInitial(context.Background(), cfg, codec, second, address, now.Add(2*time.Second))
			if err != nil || again != nil || !bytes.Equal(response, repeated) {
				t.Fatal("repeated stateless response", err)
			}
		})
	}
}
