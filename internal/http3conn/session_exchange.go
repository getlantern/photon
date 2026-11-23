package http3conn

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	quic "github.com/getlantern/photon"
	h3 "github.com/getlantern/photon/internal/http3wire"
)

// ExchangeRequest admits one already allocated client request stream and binds
// its exchange to request.Context and the session's GOAWAY-aware lease. A
// graceful peer close after GOAWAY permits buffered active responses to drain;
// request cancellation, rejected IDs and other session failures still abort.
// Encoder, Decoder and Abort in config are assigned by the session. Done remains
// an optional endpoint cleanup callback. The stream ID must never be reused.
// After argument validation, every failure closes Body and resets the stream;
// successful response Body.Close joins upload/cleanup before freeing admission.
// The caller still allocates streams, owns transport release, and chooses retry
// policy. ErrRequestRejected identifies GOAWAY rejection, not generic failure.
func (s *Session) ExchangeRequest(request *http.Request, stream ClientExchangeStream, config ClientExchangeConfig) (response *http.Response, err error) {
	if request == nil || stream == nil || config.MaxEncodedHeaders == 0 || config.MaxEncodedHeaders > uint64(^uint(0)>>1) || config.MaxInformational < 0 {
		return nil, fmt.Errorf("http3: invalid managed exchange configuration")
	}
	transferred := false
	done := config.Done
	cleanup := func() {
		if done != nil {
			done()
		}
	}
	defer func() {
		if !transferred {
			code := uint64(0x10c)
			if errors.Is(err, ErrPeerDraining) || errors.Is(err, ErrRequestLimit) {
				code = 0x10b
			}
			// These are locally abandoned streams; no request bytes have been sent.
			stream.CancelRead(quic.StreamErrorCode(code))
			stream.CancelWrite(quic.StreamErrorCode(code))
			if request.Body != nil {
				request.Body.Close()
			}
			cleanup()
		}
	}()
	if request.Context().Err() != nil {
		return nil, context.Cause(request.Context())
	}
	if _, err := HTTPRequestHeaders(request); err != nil {
		return nil, err
	}
	lease, err := s.BeginRequest(uint64(stream.StreamID()))
	if err != nil {
		if errors.Is(err, ErrPeerDraining) && config.RejectedBeforeSend != nil {
			config.RejectedBeforeSend()
		}
		return nil, err
	}
	ctx, cancel := context.WithCancelCause(request.Context())
	interrupted := make(chan struct{})
	observeLease := func() {
		cause := context.Cause(lease.Context())
		var closed *quic.PeerCloseError
		goaway := s.goaway.Load()
		// A graceful peer close after GOAWAY ends admission, but complete response
		// bytes may still be awaiting application reads. Transport reads continue
		// to reject incomplete streams. Caller cancellation remains authoritative.
		if errors.As(cause, &closed) && closed.Application && closed.Code == 0x100 && goaway != nil && !goaway.Rejects(uint64(stream.StreamID())) {
			return
		}
		cancel(cause)
	}
	stop := context.AfterFunc(lease.Context(), func() { defer close(interrupted); observeLease() })
	if lease.Context().Err() != nil {
		observeLease()
	}

	var once sync.Once
	cleanup = func() {
		once.Do(func() {
			if !stop() {
				<-interrupted
			}
			cancel(context.Canceled)
			defer lease.Finish()
			if done != nil {
				done()
			}
		})
	}
	encoder, err := s.WaitEncoder(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}
		return nil, err
	}
	if config.RequireHTTPDatagrams && (s.datagrams == nil || s.settings == nil || !s.settings.CanSendHTTPDatagrams()) {
		return nil, ErrHTTPDatagramsNotNegotiated
	}
	config.ExtendedConnect = s.settings != nil && s.settings.CanSendExtendedConnect()
	if ExtendedConnectProtocol(request) != "" && !config.ExtendedConnect {
		return nil, ErrExtendedConnectNotNegotiated
	}
	config.Encoder, config.Decoder = encoder, s.decoder
	config.Abort = func(code uint64, reason string) {
		s.cancel(&h3.Error{Code: code, Reason: reason})
		s.transport.Abort(code, reason)
	}
	config.Done = cleanup
	transferred = true
	return ExchangeRequest(ctx, request, stream, config)
}
