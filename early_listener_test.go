package quic

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/transport"
	"github.com/getlantern/photon/internal/wire"
)

// The client deliberately stops reading after Retry, so the server cannot have
// received a client Finished when Accept returns. No timing-dependent TLS gate.
func TestPublicEarlyListener(t *testing.T) {
	for _, version := range []Version{Version1, Version2} {
		ct, st := dialTLS(t)
		ct.ServerName = "127.0.0.1"
		l, err := ListenAddrEarly("127.0.0.1:0", st, &Config{Versions: []Version{version}, EnableDatagrams: true})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { l.Close() })
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		canceled, stop := context.WithCancel(ctx)
		stop()
		if _, err := l.Accept(canceled); !errors.Is(err, context.Canceled) {
			t.Fatal("canceled accept", err)
		}
		socket, err := net.DialUDP("udp", nil, l.Addr().(*net.UDPAddr))
		if err != nil {
			t.Fatal(err)
		}
		defer socket.Close()
		params := wire.DefaultTransportParameters()
		params.MaxDatagramFrameSize = 1200
		params.InitialMaxData = 8192
		params.InitialMaxStreamsUni = 2
		params.InitialMaxStreamDataUni = 4096
		client, err := transport.NewConn(transport.Config{Version: version, TLS: ct, LocalCID: []byte("client00"), PeerCID: []byte("original"), OriginalDCID: []byte("original"), Parameters: params})
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		if err := client.Start(ctx); err != nil {
			t.Fatal(err)
		}
		send := func() {
			t.Helper()
			for {
				data, err := client.NextDatagram(time.Now())
				if err != nil {
					t.Fatal(err)
				}
				if data == nil {
					return
				}
				if _, err := socket.Write(data); err != nil {
					t.Fatal(err)
				}
			}
		}
		send()
		socket.SetReadDeadline(time.Now().Add(time.Second))
		data := make([]byte, 2048)
		n, err := socket.Read(data)
		if err != nil {
			t.Fatal(err)
		}
		h, err := wire.ParseHeader(data[:n], 8)
		if err != nil || h.Type != wire.Retry {
			t.Fatal("expected Retry", err)
		}
		if err := client.HandleDatagram(data[:n], time.Now()); err != nil {
			t.Fatal(err)
		}
		send()
		accepted, err := l.Accept(ctx)
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-accepted.HandshakeComplete():
			t.Fatal("premature handshake completion")
		default:
		}
		if accepted.ConnectionState().TLS.HandshakeComplete || accepted.ConnectionState().Used0RTT {
			t.Fatal("incorrect initial state")
		}
		stream, err := accepted.OpenUniStreamSync(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := stream.Write([]byte("before client Finished")); err != nil {
			t.Fatal(err)
		}
		if err := stream.Close(); err != nil {
			t.Fatal(err)
		}
		if err := accepted.SendDatagram([]byte("early server response")); err != nil {
			t.Fatal(err)
		}
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
		select {
		case <-accepted.Context().Done():
		case <-ctx.Done():
			t.Fatal("early connection survived listener closure")
		}
		select {
		case <-accepted.HandshakeComplete():
			t.Fatal("failed handshake signaled success")
		default:
		}
	}
}
