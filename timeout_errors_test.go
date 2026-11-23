package quic

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/transport"
)

func assertTransportTimeout(t *testing.T, err error, handshake bool) {
	t.Helper()
	var timeout net.Error
	if !errors.As(err, &timeout) || !timeout.Timeout() || timeout.Temporary() || !errors.Is(err, net.ErrClosed) {
		t.Fatalf("missing terminal timeout contract: %T %v", err, err)
	}
	var idle *IdleTimeoutError
	var hs *HandshakeTimeoutError
	if errors.As(err, &hs) != handshake || errors.As(err, &idle) == handshake {
		t.Fatalf("wrong timeout type: %T %v", err, err)
	}
}

func TestTransportTimeoutErrorWrapping(t *testing.T) {
	for _, handshake := range []bool{false, true} {
		var err error = &IdleTimeoutError{}
		if handshake {
			err = &HandshakeTimeoutError{}
		}
		assertTransportTimeout(t, fmt.Errorf("operation: %w", err), handshake)
	}
}

func TestPublicIdleTimeoutPropagation(t *testing.T) {
	for _, version := range []Version{Version1, Version2} {
		ct, st := dialTLS(t)
		listener, err := ListenAddr("127.0.0.1:0", st, &Config{EnableDatagrams: true})
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		c, err := DialAddr(ctx, listener.Addr().String(), ct, &Config{Versions: []Version{version}, EnableDatagrams: true})
		if err != nil {
			t.Fatal(err)
		}
		defer c.server.Close()
		if _, err := listener.Accept(ctx); err != nil {
			t.Fatal(err)
		}
		stream, err := c.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		// Advance only the serialized engine's expiry operation; avoid a wall-clock
		// wait and preserve the runtime's normal terminal-cause propagation.
		err = c.server.Do(ctx, func(*transport.ServerEndpoint) error {
			deadline := c.raw.IdleDeadline()
			if deadline.IsZero() {
				return errors.New("fixture lacks idle deadline")
			}
			return c.raw.HandleTimeout(deadline)
		})
		assertTransportTimeout(t, err, false)
		select {
		case <-c.Context().Done():
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		assertTransportTimeout(t, context.Cause(c.Context()), false)
		assertTransportTimeout(t, c.SendDatagram([]byte{1}), false)
		_, receiveErr := c.ReceiveDatagram(ctx)
		assertTransportTimeout(t, receiveErr, false)
		assertTransportTimeout(t, stream.WaitAcknowledged(ctx), false)
		assertTransportTimeout(t, stream.Close(), false)
		if _, err := stream.Read(make([]byte, 1)); err != nil {
			assertTransportTimeout(t, err, false)
		} else {
			t.Fatal("read succeeded after timeout")
		}
		if _, err := stream.Write([]byte{1}); err != nil {
			assertTransportTimeout(t, err, false)
		} else {
			t.Fatal("write succeeded after timeout")
		}
		if _, err := c.OpenStream(); err != nil {
			assertTransportTimeout(t, err, false)
		} else {
			t.Fatal("stream opened after timeout")
		}
	}
}
