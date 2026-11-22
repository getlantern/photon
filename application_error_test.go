package quic

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestPublicApplicationClosePropagation(t *testing.T) {
	for _, version := range []Version{Version1, Version2} {
		ct, st := dialTLS(t)
		listener, err := ListenAddr("127.0.0.1:0", st, &Config{EnableDatagrams: true})
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		client, err := DialAddr(ctx, listener.Addr().String(), ct, &Config{Versions: []Version{version}, EnableDatagrams: true})
		if err != nil {
			t.Fatal(err)
		}
		defer client.server.Close()
		peer, err := listener.Accept(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err = client.CloseWithError(0xabc, "done"); err != nil {
			t.Fatal(err)
		}
		for _, item := range []struct {
			conn   *Conn
			remote bool
		}{{client, false}, {peer, true}} {
			select {
			case <-item.conn.Context().Done():
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			check := func(err error) {
				t.Helper()
				var app *ApplicationError
				if !errors.As(err, &app) || app.Remote != item.remote || app.ErrorCode != 0xabc || app.ErrorMessage != "done" || !errors.Is(err, net.ErrClosed) {
					t.Fatal("application cause", err)
				}
			}
			check(context.Cause(item.conn.Context()))
			check(item.conn.SendDatagram([]byte("x")))
			_, err = item.conn.OpenStream()
			check(err)
		}
	}
}
