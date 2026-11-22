package http3

import (
	"context"
	"errors"
	"io"
	"testing"

	quic "github.com/getlantern/photon"
)

func TestTunnelPartialWriteError(t *testing.T) {
	cause := &quic.StreamError{Remote: true, ErrorCode: 0x10c}
	input, output := io.Pipe()
	defer input.Close()
	defer output.Close()
	tunnel := &Tunnel{writer: output, ctx: context.Background()}
	done := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(input, make([]byte, 2))
		input.CloseWithError(cause)
		done <- err
	}()
	n, err := tunnel.Write([]byte("abcd"))
	var h *Error
	var reset *quic.StreamError
	if n != 2 || !errors.As(err, &h) || h.ErrorCode != ErrCodeRequestCanceled || !h.Remote || !errors.As(err, &reset) || reset != cause {
		t.Fatal(n, err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	tunnel.ctx = ctx
	if n, err := tunnel.Write([]byte("x")); n != 0 || err != context.Canceled {
		t.Fatal(n, err)
	}
}

func TestTunnelReadCloseErrors(t *testing.T) {
	for _, abort := range []bool{false, true} {
		cause := &quic.PeerCloseError{Application: true, Code: 0x102, Reason: "peer failure"}
		body := &failingHTTPBody{err: cause}
		input, output := io.Pipe()
		tunnel := &Tunnel{reader: body, writer: output, ctx: context.Background()}
		b := make([]byte, 1)
		n, err := tunnel.Read(b)
		var h *Error
		if n != 1 || !errors.As(err, &h) || h.ErrorCode != ErrCodeInternalError || !errors.Is(err, cause) {
			t.Fatal(n, err)
		}
		if abort {
			err = tunnel.CloseWithError(0x10c, "abort")
		} else {
			err = tunnel.Close()
		}
		if !errors.As(err, &h) || h.ErrorMessage != "peer failure" || !errors.Is(err, cause) {
			t.Fatal(err)
		}
		if err = tunnel.Close(); !errors.As(err, &h) || body.closes != 1 {
			t.Fatal(err, body.closes)
		}
		if _, err = input.Read(b); err != io.EOF {
			t.Fatal("upload pipe not closed", err)
		}
		input.Close()
	}
}

func TestTunnelDatagramCancellationErrors(t *testing.T) {
	cause := &quic.StreamError{Remote: true, ErrorCode: 0x33}
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(cause)
	tunnel := &Tunnel{ctx: ctx}
	send := tunnel.SendDatagram(context.Background(), nil)
	data, receive := tunnel.ReceiveDatagram(context.Background())
	for _, err := range []error{send, receive} {
		var h *Error
		if !errors.As(err, &h) || h.ErrorCode != ErrCodeDatagramError || !errors.Is(err, cause) {
			t.Fatal(err)
		}
	}
	if data != nil {
		t.Fatal(data)
	}
	tunnel.ctx = context.Background()
	if err := tunnel.SendDatagram(context.Background(), nil); err != ErrHTTPDatagramsNotNegotiated {
		t.Fatal(err)
	}
	if _, err := tunnel.ReceiveDatagram(context.Background()); err != ErrHTTPDatagramsNotNegotiated {
		t.Fatal(err)
	}
}
