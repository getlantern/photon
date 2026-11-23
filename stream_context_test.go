package quic

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

func TestPublicStreamContexts(t *testing.T) {
	for _, v := range []Version{Version1, Version2} {
		clientTLS, serverTLS := dialTLS(t)
		l, err := ListenAddr("127.0.0.1:0", serverTLS, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer l.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		c, err := DialAddr(ctx, l.Addr().String(), clientTLS, &Config{Versions: []Version{v}})
		if err != nil {
			t.Fatal(err)
		}
		defer c.server.Close()
		peer, err := l.Accept(ctx)
		if err != nil {
			t.Fatal(err)
		}
		s, err := c.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		if s.Context().Err() != nil {
			t.Fatal("new stream context canceled")
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if context.Cause(s.Context()) != context.Canceled {
			t.Fatal("normal close cause", context.Cause(s.Context()))
		}
		ps, err := peer.AcceptStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ps.Write([]byte("read side remains open")); err != nil {
			t.Fatal(err)
		}
		if err := ps.Close(); err != nil {
			t.Fatal(err)
		}
		select {
		case <-s.ReceiveContext().Done():
		case <-ctx.Done():
			t.Fatal("receive FIN not observed before read", ctx.Err())
		}
		if context.Cause(s.ReceiveContext()) != io.EOF {
			t.Fatal(context.Cause(s.ReceiveContext()))
		}
		s.SetReadDeadline(time.Now().Add(time.Second))
		if b, err := io.ReadAll(s); err != nil || string(b) != "read side remains open" {
			t.Fatal(string(b), err)
		}
		local, err := c.OpenUniStream()
		if err != nil {
			t.Fatal(err)
		}
		local.CancelWrite(9)
		local.CancelWrite(10)
		var stopped *StreamError
		if !errors.As(context.Cause(local.Context()), &stopped) || stopped.Remote || stopped.ErrorCode != 9 || stopped.StreamID != local.StreamID() {
			t.Fatal("local cancellation", context.Cause(local.Context()))
		}
		outgoing, err := c.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := outgoing.Write([]byte("start")); err != nil {
			t.Fatal(err)
		}
		receiver, err := peer.AcceptStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		receiver.CancelRead(42)
		select {
		case <-outgoing.Context().Done():
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		if !errors.As(context.Cause(outgoing.Context()), &stopped) || !stopped.Remote || stopped.ErrorCode != 42 || stopped.StreamID != outgoing.StreamID() {
			t.Fatal("remote cancellation", context.Cause(outgoing.Context()))
		}
		if _, err := outgoing.Write([]byte("late")); !errors.As(err, &stopped) || stopped.ErrorCode != 42 {
			t.Fatal("write lost cancellation", err)
		}
		alive, err := c.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		c.server.Close()
		select {
		case <-alive.Context().Done():
		case <-ctx.Done():
			t.Fatal("connection did not cancel stream", ctx.Err())
		}
		select {
		case <-alive.ReceiveContext().Done():
		case <-ctx.Done():
			t.Fatal("connection did not cancel receive context")
		}
		if context.Cause(alive.ReceiveContext()) != context.Cause(c.Context()) {
			t.Fatal("lost receive connection cause")
		}
		if context.Cause(alive.Context()) != context.Cause(c.Context()) {
			t.Fatal("lost connection cause")
		}
	}
}

func TestPublicReceiveErrorsAfterCleanup(t *testing.T) {
	for _, v := range []Version{Version1, Version2} {
		clientTLS, serverTLS := dialTLS(t)
		l, err := ListenAddr("127.0.0.1:0", serverTLS, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer l.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		c, err := DialAddr(ctx, l.Addr().String(), clientTLS, &Config{Versions: []Version{v}})
		if err != nil {
			t.Fatal(err)
		}
		defer c.server.Close()
		peer, err := l.Accept(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, remote := range []bool{false, true} {
			tx, err := c.OpenUniStream()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Write([]byte("x")); err != nil {
				t.Fatal(err)
			}
			rx, err := peer.AcceptUniStream(ctx)
			if err != nil {
				t.Fatal(err)
			}
			rx.SetReadDeadline(time.Now().Add(time.Second))
			// Consume existing data before testing cancellation of a pending read.
			if _, err := io.ReadFull(rx, make([]byte, 1)); err != nil {
				t.Fatal(err)
			}
			pending := make(chan error, 1)
			go func() { _, err := rx.Read(make([]byte, 1)); pending <- err }()
			if remote {
				tx.CancelWrite(32)
			} else {
				rx.CancelRead(32)
			}
			select {
			case err := <-pending:
				var stopped *StreamError
				if !errors.As(err, &stopped) || stopped.ErrorCode != 32 || stopped.StreamID != rx.StreamID() || stopped.Remote != remote {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			select {
			case <-rx.ReceiveContext().Done():
			case <-ctx.Done():
				t.Fatal("reset did not cancel receive context")
			}
			var receiveCause *StreamError
			if !errors.As(context.Cause(rx.ReceiveContext()), &receiveCause) || receiveCause.ErrorCode != 32 || receiveCause.Remote != remote || receiveCause.StreamID != rx.StreamID() {
				t.Fatal(context.Cause(rx.ReceiveContext()))
			}
			if err := rx.Release(ctx); err != nil {
				t.Fatal(err)
			}
			rx.CancelRead(99)
			var stopped *StreamError
			if _, err := rx.Read(make([]byte, 1)); !errors.As(err, &stopped) || stopped.ErrorCode != 32 || stopped.Remote != remote || stopped.StreamID != rx.StreamID() {
				t.Fatal("error lost after cleanup", err)
			}
		}
	}
}
