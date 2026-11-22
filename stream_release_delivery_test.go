package quic

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

// Draining a response retained across peer close releases the handle without
// any send state. That cleanup must not report unacknowledged output as
// delivered, nor replace the connection failure, while the Transport still runs.
func TestReleasedReadAfterPeerCloseIsNotDelivery(t *testing.T) {
	for _, version := range []Version{Version1, Version2} {
		for _, fin := range []bool{false, true} {
			t.Run(fmt.Sprintf("%v/fin=%v", version, fin), func(t *testing.T) {
				ct, st := dialTLS(t)
				listener, err := ListenAddr("127.0.0.1:0", st, &Config{Versions: []Version{version}})
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				udp, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
				if err != nil {
					t.Fatal(err)
				}
				defer udp.Close()
				socket := &migrationDropSocket{PacketConn: udp, writes: make(chan struct{}, 1)}
				tr := &Transport{Conn: socket}
				defer tr.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				client, err := tr.Dial(ctx, listener.Addr(), ct, &Config{Versions: []Version{version}})
				if err != nil {
					t.Fatal(err)
				}
				server, err := listener.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				stream, err := client.OpenStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := stream.Write([]byte("request")); err != nil {
					t.Fatal(err)
				}
				peer, err := server.AcceptStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				request := make([]byte, len("request"))
				if _, err := io.ReadFull(peer, request); err != nil {
					t.Fatal(err)
				}
				if _, err := peer.Write([]byte("response")); err != nil {
					t.Fatal(err)
				}
				if err := peer.Close(); err != nil {
					t.Fatal(err)
				}
				select {
				case <-stream.ReceiveContext().Done():
				case <-ctx.Done():
					t.Fatal("response not buffered", ctx.Err())
				}
				// Everything queued from here on is sealed but never reaches the peer.
				socket.drop.Store(true)
				if _, err := stream.Write([]byte("lost")); err != nil {
					t.Fatal(err)
				}
				if fin {
					if err := stream.Close(); err != nil {
						t.Fatal(err)
					}
				}
				if err := server.CloseWithError(7, "finished"); err != nil {
					t.Fatal(err)
				}
				select {
				case <-client.Context().Done():
				case <-ctx.Done():
					t.Fatal("peer close not observed", ctx.Err())
				}
				closed := context.Cause(client.Context())
				response, err := io.ReadAll(stream)
				if err != nil || string(response) != "response" {
					t.Fatalf("retained response %q: %v", response, err)
				}
				if err := stream.WaitAcknowledged(ctx); !errors.Is(err, closed) {
					t.Fatal("unacknowledged output reported as delivered:", err)
				}
				if err := stream.Release(ctx); !errors.Is(err, closed) {
					t.Fatal("release reported unacknowledged output as complete:", err)
				}
				if fin {
					return
				}
				if _, err := stream.Write([]byte("more")); !errors.Is(err, closed) {
					t.Fatal("write hid the connection failure:", err)
				}
				if err := stream.TryWriteAll([]byte("more")); !errors.Is(err, closed) {
					t.Fatal("try-write hid the connection failure:", err)
				}
				if err := stream.Close(); !errors.Is(err, closed) {
					t.Fatal("close hid the connection failure:", err)
				}
			})
		}
	}
}
