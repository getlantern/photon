package quic

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestStreamErrorIdentity(t *testing.T) {
	original := &StreamError{StreamID: 8, ErrorCode: 42, Remote: true}
	wrapped := fmt.Errorf("read failed: %w", original)
	for _, test := range []struct {
		target error
		want   bool
	}{
		{&StreamError{StreamID: 8, ErrorCode: 42, Remote: true}, true},
		{&StreamError{StreamID: 12, ErrorCode: 42, Remote: true}, false},
		{&StreamError{StreamID: 8, ErrorCode: 43, Remote: true}, false},
		{&StreamError{StreamID: 8, ErrorCode: 42, Remote: false}, false},
		{&StreamError{}, false},
		{(*StreamError)(nil), false},
		{context.Canceled, false},
	} {
		if got := errors.Is(wrapped, test.target); got != test.want {
			t.Fatalf("Is(%v)=%t", test.target, got)
		}
	}
	var found *StreamError
	if !errors.As(wrapped, &found) || found != original {
		t.Fatal("type discovery changed", wrapped)
	}
}

func TestPublicStreamCancellationIdentity(t *testing.T) {
	for _, version := range []Version{Version1, Version2} {
		ct, st := dialTLS(t)
		listener, err := ListenAddr("127.0.0.1:0", st, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		client, err := DialAddr(ctx, listener.Addr().String(), ct, &Config{Versions: []Version{version}})
		if err != nil {
			t.Fatal(err)
		}
		defer client.server.Close()
		server, err := listener.Accept(ctx)
		if err != nil {
			t.Fatal(err)
		}
		send, err := client.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		if _, err = send.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
		recv, err := server.AcceptStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		deadline, _ := ctx.Deadline()
		if err := recv.SetReadDeadline(deadline); err != nil {
			t.Fatal(err)
		}
		send.CancelWrite(42)
		_, localErr := send.Write([]byte("y"))
		// Buffered data may precede the reset; drain it before inspecting the cause.
		var remoteErr error
		buf := make([]byte, 16)
		for remoteErr == nil {
			_, remoteErr = recv.Read(buf)
		}
		for _, item := range []struct {
			err    error
			remote bool
		}{{localErr, false}, {remoteErr, true}} {
			exact := &StreamError{StreamID: send.StreamID(), ErrorCode: 42, Remote: item.remote}
			if !errors.Is(item.err, exact) || errors.Is(item.err, &StreamError{StreamID: send.StreamID() + 4, ErrorCode: 42, Remote: item.remote}) || errors.Is(item.err, &StreamError{StreamID: send.StreamID(), ErrorCode: 42, Remote: !item.remote}) {
				t.Fatal("cancel identity", item.err)
			}
		}
	}
}
