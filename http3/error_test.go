package http3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	quic "github.com/getlantern/photon"
	h3 "github.com/getlantern/photon/internal/http3wire"
)

func TestHTTPErrorIdentity(t *testing.T) {
	for _, remote := range []bool{false, true} {
		original := &quic.StreamError{StreamID: 8, Remote: remote, ErrorCode: 0x10c}
		wrapped := fmt.Errorf("reading DATA: %w", original)
		got := httpError(wrapped)
		var h *Error
		var s *quic.StreamError
		if !errors.As(got, &h) || h.Remote != remote || h.ErrorCode != ErrCodeRequestCanceled || errors.Unwrap(h) != wrapped || !errors.As(got, &s) || s != original {
			t.Fatal(got)
		}
		if !errors.Is(got, &Error{Remote: remote, ErrorCode: ErrCodeRequestCanceled, ErrorMessage: "ignored"}) || errors.Is(got, &Error{Remote: !remote, ErrorCode: ErrCodeRequestCanceled}) || errors.Is(got, &Error{Remote: remote, ErrorCode: ErrCodeMessageError}) {
			t.Fatal("identity", got)
		}
		if httpError(got) != got || httpError(fmt.Errorf("outer: %w", got)).Error() != "outer: "+got.Error() {
			t.Fatal("double conversion")
		}
	}
	close := &quic.PeerCloseError{Application: true, Code: 0x109, Reason: "settings"}
	got := httpError(close)
	var h *Error
	var peer *quic.PeerCloseError
	if !errors.As(got, &h) || !h.Remote || h.ErrorMessage != "settings" || h.ErrorCode != ErrCodeSettingsError || !errors.As(got, &peer) || peer != close {
		t.Fatal(got)
	}
	for _, err := range []error{nil, io.EOF, context.Canceled, context.DeadlineExceeded, &quic.IdleTimeoutError{}, &quic.StatelessResetError{}, &quic.PeerCloseError{Application: false, Code: 0x109}} {
		if httpError(err) != err {
			t.Fatal("converted non-application error", err)
		}
	}
	if got := (&Error{Remote: true, ErrorCode: ErrCode(1 << 40), ErrorMessage: "extension"}).Error(); got != "H3 error (0x10000000000): extension" {
		t.Fatal(got)
	}
	if got := ErrCode(1 << 40).String(); got != "unknown error code: 0x10000000000" {
		t.Fatal(got)
	}
}

type failingHTTPBody struct {
	err    error
	closes int
}

func (b *failingHTTPBody) Read(p []byte) (int, error) {
	if len(p) > 0 {
		p[0] = 'x'
		return 1, b.err
	}
	return 0, b.err
}
func (b *failingHTTPBody) Close() error { b.closes++; return b.err }

func TestResponseBodyHTTPError(t *testing.T) {
	cause := &quic.StreamError{Remote: true, ErrorCode: 0x10c}
	inner := &failingHTTPBody{err: cause}
	released := 0
	body := &reservedResponseBody{ReadCloser: inner, release: func() { released++ }}
	p := make([]byte, 1)
	n, err := body.Read(p)
	var h *Error
	if n != 1 || p[0] != 'x' || !errors.As(err, &h) || !errors.Is(err, cause) {
		t.Fatal(n, err)
	}
	for i := 0; i < 2; i++ {
		if err := body.Close(); !errors.As(err, &h) || !errors.Is(err, cause) {
			t.Fatal(err)
		}
	}
	if inner.closes != 1 || released != 1 {
		t.Fatal(inner.closes, released)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	body.request = ctx
	if n, err := body.Read(p); n != 0 || err != context.Canceled {
		t.Fatal(n, err)
	}
}

func TestHTTPApplicationErrorWrapping(t *testing.T) {
	for _, remote := range []bool{false, true} {
		cause := &quic.ApplicationError{Remote: remote, ErrorCode: 0x109, ErrorMessage: "settings"}
		err := httpError(cause)
		var h *Error
		var app *quic.ApplicationError
		if !errors.As(err, &h) || h.Remote != remote || h.ErrorCode != ErrCodeSettingsError || h.ErrorMessage != "settings" || !errors.As(err, &app) || app != cause {
			t.Fatal(err)
		}
	}
}

func TestLocalHTTPProtocolError(t *testing.T) {
	cause := &h3.Error{Code: h3.ExcessiveLoad, Reason: "field section limit"}
	original := fmt.Errorf("response: %w", cause)
	err := httpError(original)
	var protocol *Error
	if !errors.As(err, &protocol) || protocol.Remote || protocol.ErrorCode != ErrCodeExcessiveLoad || protocol.ErrorMessage != cause.Reason || !errors.Is(err, cause) || errors.Unwrap(protocol) != original {
		t.Fatal("local protocol error identity lost", err)
	}
	if httpError(err) != err {
		t.Fatal("wrapped twice")
	}
}
