package quic

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/transport"
)

// Exercise the release callback at the public adapter boundary. The live
// engine stream represents a later generation reusing the rejected handle's ID;
// early network negotiation is covered separately by transport engine tests.
func TestRejectedStreamHandleCannotUseReusedID(t *testing.T) {
	for _, v := range []Version{Version1, Version2} {
		ct, st := dialTLS(t)
		l, err := ListenAddr("127.0.0.1:0", st, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { l.Close() })
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		c, err := DialAddr(ctx, l.Addr().String(), ct, &Config{Versions: []Version{v}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.CloseWithError(0, "") })
		peer, err := l.Accept(ctx)
		if err != nil {
			t.Fatal(err)
		}
		fresh, err := c.OpenStreamSync(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var stale []*Stream
		for _, closed := range []bool{false, true} {
			h := &streamHandle{conn: c, id: uint64(fresh.StreamID()), sendTerminal: context.Canceled}
			old := newStream(h)
			old.SendStream.closed = closed
			if err := c.server.Do(ctx, func(*transport.ServerEndpoint) error { h.markReleased(Err0RTTRejected); return nil }); err != nil {
				t.Fatal(err)
			}
			stale = append(stale, old)
			if _, err := old.Write([]byte("must not reach new stream")); !errors.Is(err, Err0RTTRejected) {
				t.Fatal("write", err)
			}
			if err := old.TryWriteAll([]byte("must not atomically reach new stream")); !errors.Is(err, Err0RTTRejected) {
				t.Fatal("atomic write", err)
			}
			called := false
			if _, err := old.WriteWithLimit([]byte("stale limited write"), func(maximum int) int { called = true; return maximum }); !errors.Is(err, Err0RTTRejected) || called {
				t.Fatal("limited write", err, called)
			}
			if _, err := old.Peek(make([]byte, 1)); !errors.Is(err, Err0RTTRejected) {
				t.Fatal("peek", err)
			}
			if _, err := old.Read(make([]byte, 1)); !errors.Is(err, Err0RTTRejected) {
				t.Fatal("read", err)
			}
			if err := old.Close(); !errors.Is(err, Err0RTTRejected) {
				t.Fatal("FIN", err)
			}
			if err := old.WaitAcknowledged(ctx); !errors.Is(err, Err0RTTRejected) {
				t.Fatal("false delivery success", err)
			}
			if err := old.Release(ctx); !errors.Is(err, Err0RTTRejected) {
				t.Fatal("release", err)
			}
			old.CancelRead(33)
			old.CancelWrite(34)
		}
		if _, err := fresh.Write([]byte("new generation")); err != nil {
			t.Fatal(err)
		}
		if err := fresh.Close(); err != nil {
			t.Fatal(err)
		}
		remote, err := peer.AcceptStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		remote.SetReadDeadline(time.Now().Add(time.Second))
		data, err := io.ReadAll(remote)
		if err != nil || string(data) != "new generation" {
			t.Fatal("stale handle modified live stream", string(data), err)
		}
		if _, err := remote.Write([]byte("reply")); err != nil {
			t.Fatal(err)
		}
		if err := remote.Close(); err != nil {
			t.Fatal(err)
		}
		data, err = io.ReadAll(fresh)
		if err != nil || string(data) != "reply" {
			t.Fatal("stale read cancellation affected live stream", string(data), err)
		}
		if err := fresh.WaitAcknowledged(ctx); err != nil {
			t.Fatal(err)
		}
		c.server.Close()
		for _, old := range stale {
			if err := old.WaitAcknowledged(ctx); !errors.Is(err, Err0RTTRejected) {
				t.Fatal("lost rejection after runtime shutdown", err)
			}
			if err := old.Release(ctx); !errors.Is(err, Err0RTTRejected) {
				t.Fatal(err)
			}
		}
	}
}
