package quic

import (
	"context"
	"errors"
	"github.com/getlantern/photon/internal/transport"
	"io"
	"testing"
	"time"
)

func TestPublicStreamAcknowledged(t *testing.T) {
	for _, version := range []Version{Version1, Version2} {
		clientTLS, serverTLS := dialTLS(t)
		listener, err := ListenAddr("127.0.0.1:0", serverTLS, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { listener.Close() })
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		client, err := DialAddr(ctx, listener.Addr().String(), clientTLS, &Config{Versions: []Version{version}})
		if err != nil {
			t.Fatal(err)
		}
		defer client.CloseWithError(0, "")
		server, err := listener.Accept(ctx)
		if err != nil {
			t.Fatal(err)
		}
		stream, err := client.OpenUniStreamSync(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := stream.Write([]byte("first")); err != nil {
			t.Fatal(err)
		}
		if err := stream.WaitAcknowledged(ctx); err != nil {
			t.Fatal(err)
		}
		peer, err := server.AcceptUniStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		peer.SetReadDeadline(time.Now().Add(time.Second))
		data := make([]byte, 5)
		if _, err := io.ReadFull(peer, data); err != nil || string(data) != "first" {
			t.Fatal(string(data), err)
		}
		if _, err := stream.Write([]byte("second")); err != nil {
			t.Fatal("barrier closed stream", err)
		}
		if err := stream.Close(); err != nil {
			t.Fatal(err)
		}
		if err := stream.WaitAcknowledged(ctx); err != nil {
			t.Fatal(err)
		}
		rest, err := io.ReadAll(peer)
		if err != nil || string(rest) != "second" {
			t.Fatal(string(rest), err)
		}
		if err := stream.Release(ctx); err != nil {
			t.Fatal(err)
		}
		if err := stream.WaitAcknowledged(ctx); err != nil {
			t.Fatal("released delivery barrier", err)
		}
		reset, err := client.OpenUniStreamSync(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := reset.Write([]byte("discard")); err != nil {
			t.Fatal(err)
		}
		reset.CancelWrite(55)
		var streamErr *StreamError
		if err := reset.WaitAcknowledged(ctx); !errors.As(err, &streamErr) || streamErr.ErrorCode != 55 {
			t.Fatal("reset is not delivery", err)
		}
	}
}

func TestPublicAcknowledgedResetAfterFINRelease(t *testing.T) {
	for _, version := range []Version{Version1, Version2} {
		ct, st := dialTLS(t)
		listener, err := ListenAddr("127.0.0.1:0", st, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { listener.Close() })
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		client, err := DialAddr(ctx, listener.Addr().String(), ct, &Config{Versions: []Version{version}})
		if err != nil {
			t.Fatal(err)
		}
		defer client.CloseWithError(0, "")
		if _, err := listener.Accept(ctx); err != nil {
			t.Fatal(err)
		}
		stream, err := client.OpenUniStreamSync(ctx)
		if err != nil {
			t.Fatal(err)
		}
		// Queue FIN and cancel in one owner operation so its ACK cannot race ahead
		// of reset. The public handle's context closes on FIN before reset occurs.
		if err := client.server.Do(ctx, func(*transport.ServerEndpoint) error {
			if _, err := client.raw.WriteStream(stream.id, []byte("discarded"), true); err != nil {
				return err
			}
			return client.raw.CancelWrite(stream.id, 42)
		}); err != nil {
			t.Fatal(err)
		}
		if err := stream.Release(ctx); err != nil {
			t.Fatal(err)
		}
		var reset *StreamError
		if err := stream.WaitAcknowledged(ctx); !errors.As(err, &reset) || reset.ErrorCode != 42 || reset.Remote {
			t.Fatal("released reset reported as delivery", err)
		}
	}
}
