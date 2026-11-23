package interop

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
)

func TestTransportStatelessResetRestartIndependent(t *testing.T) {
	for _, length := range []int{0, 1, 4, 8, 20} {
		t.Run(fmt.Sprintf("CID=%d", length), func(t *testing.T) { testTransportStatelessResetRestartIndependent(t, length) })
	}
}
func testTransportStatelessResetRestartIndependent(t *testing.T, length int) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			ct, st := tlsConfigs(t)
			socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer socket.Close()
			key := quic.StatelessResetKey{7}
			tr := &quic.Transport{Conn: socket, StatelessResetKey: &key, ConnectionIDLength: length}
			defer func() { tr.Close() }()
			l, err := tr.Listen(st, &quic.Config{Versions: []quic.Version{version}})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			client, err := dialUpstreamAddr(t, ctx, l.Addr().String(), ct, &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}})
			if err != nil {
				t.Fatal(err)
			}
			defer client.CloseWithError(0, "")
			if _, err := l.Accept(ctx); err != nil {
				t.Fatal(err)
			}
			stream, err := client.OpenUniStreamSync(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := tr.Close(); err != nil {
				t.Fatal(err)
			}
			tr = &quic.Transport{Conn: socket, StatelessResetKey: &key, ConnectionIDLength: length}
			if _, err := tr.Listen(st, nil); err != nil {
				t.Fatal(err)
			}
			if _, err := stream.Write(make([]byte, 1000)); err != nil {
				t.Fatal(err)
			}
			select {
			case <-client.Context().Done():
			case <-ctx.Done():
				t.Fatal("peer did not detect reset", ctx.Err())
			}
			var reset *upstream.StatelessResetError
			if !errors.As(context.Cause(client.Context()), &reset) {
				t.Fatal("wrong termination", context.Cause(client.Context()))
			}
		})
	}
}
