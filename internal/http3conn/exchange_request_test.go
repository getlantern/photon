package http3conn

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
)

type exchangeStream struct {
	*serveStream
	sendCtx    context.Context
	sendCancel context.CancelCauseFunc
}

func (s *exchangeStream) Context() context.Context { return s.sendCtx }
func (s *exchangeStream) CancelWrite(code quic.StreamErrorCode) {
	s.serveStream.CancelWrite(code)
	s.sendCancel(&quic.StreamError{ErrorCode: code})
}
func exchangeFixture(t *testing.T, source io.Reader) (*exchangeStream, ClientExchangeConfig, *atomic.Int32) {
	t.Helper()
	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(context.Canceled) })
	stream := &exchangeStream{serveStream: &serveStream{source: source}, sendCtx: ctx, sendCancel: cancel}
	base := serveConfig(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	done := new(atomic.Int32)
	return stream, ClientExchangeConfig{Encoder: base.Encoder, Decoder: base.Decoder, MaxEncodedHeaders: 4096, MaxInformational: 4, Abort: base.Abort, Done: func() { done.Add(1) }}, done
}
func TestExchangeRequestEarlyResponseStopsBlockedUpload(t *testing.T) {
	for _, remote := range []bool{false, true} {
		wire := readerHeaders(t, responseFields("200")...)
		wire = append(wire, readerFrame(t, 0, []byte("early"))...)
		stream, config, done := exchangeFixture(t, bytes.NewReader(wire))
		pipe, producer := io.Pipe()
		defer producer.Close()
		defer pipe.Close()
		entered, closed := make(chan struct{}), make(chan struct{})
		body := &uploadBody{read: func(p []byte) (int, error) { close(entered); return pipe.Read(p) }, close: func() error { defer close(closed); return pipe.Close() }}
		request, _ := http.NewRequest("POST", "https://example.com/", body)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		response, err := ExchangeRequest(ctx, request, stream, config)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		select {
		case <-entered:
		case <-ctx.Done():
			cancel()
			t.Fatal("response blocked upload start")
		}
		if remote {
			stream.sendCancel(&quic.StreamError{Remote: true, ErrorCode: 0x100})
			select {
			case <-closed:
			case <-ctx.Done():
				cancel()
				t.Fatal("peer STOP_SENDING did not close blocked body")
			}
		}
		payload, err := io.ReadAll(response.Body)
		if err != nil || string(payload) != "early" || done.Load() != 0 {
			cancel()
			t.Fatal(string(payload), err, done.Load())
		}
		if err := response.Body.Close(); err != nil {
			cancel()
			t.Fatal(err)
		}
		if err := response.Body.Close(); err != nil {
			cancel()
			t.Fatal(err)
		}
		cancel()
		if body.closes.Load() != 1 || done.Load() != 1 {
			t.Fatal("ownership", body.closes.Load(), done.Load())
		}
	}
}
func TestExchangeRequestUploadFailureInterruptsResponse(t *testing.T) {
	pipe, producer := io.Pipe()
	defer producer.Close()
	defer pipe.Close()
	stream, config, done := exchangeFixture(t, pipe)
	stream.interrupt = func() { pipe.CloseWithError(context.Canceled) }
	sentinel := errors.New("upload source failed")
	body := &uploadBody{read: func([]byte) (int, error) { return 0, sentinel }}
	request, _ := http.NewRequest("POST", "https://example.com/", body)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	response, err := ExchangeRequest(ctx, request, stream, config)
	if response != nil || !errors.Is(err, sentinel) || done.Load() != 1 || body.closes.Load() != 1 {
		t.Fatal(response, err, done.Load(), body.closes.Load())
	}
}
func TestExchangeRequestResponseFailureAbortsAndCleans(t *testing.T) {
	stream, config, done := exchangeFixture(t, bytes.NewReader(readerFrame(t, 4, nil)))
	var aborted atomic.Uint64
	config.Abort = func(code uint64, _ string) { aborted.Store(code) }
	body := &uploadBody{read: func([]byte) (int, error) { return 0, io.EOF }}
	request, _ := http.NewRequest("GET", "https://example.com/", body)
	response, err := ExchangeRequest(context.Background(), request, stream, config)
	if err == nil || response != nil || aborted.Load() != 0x105 || done.Load() != 1 || body.closes.Load() != 1 {
		t.Fatal(response, err, aborted.Load(), done.Load(), body.closes.Load())
	}
}

func TestExchangeRequestPreservesParentCancellation(t *testing.T) {
	responsePipe, responseWriter := io.Pipe()
	defer responsePipe.Close()
	defer responseWriter.Close()
	stream, config, done := exchangeFixture(t, responsePipe)
	stream.interrupt = func() { responsePipe.CloseWithError(io.ErrClosedPipe) }
	uploadPipe, uploadWriter := io.Pipe()
	defer uploadPipe.Close()
	defer uploadWriter.Close()
	entered := make(chan struct{})
	body := &uploadBody{read: func(p []byte) (int, error) { close(entered); return uploadPipe.Read(p) }, close: uploadPipe.Close}
	request, _ := http.NewRequest("POST", "https://example.com/", body)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(context.Canceled)
	cause := errors.New("request lease rejected")
	result := make(chan error, 1)
	go func() { _, err := ExchangeRequest(ctx, request, stream, config); result <- err }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("upload missing")
	}
	cancel(cause)
	select {
	case err := <-result:
		if !errors.Is(err, cause) {
			t.Fatal("cause lost", err)
		}
	case <-time.After(time.Second):
		t.Fatal("exchange did not join")
	}
	if body.closes.Load() != 1 || done.Load() != 1 {
		t.Fatal(body.closes.Load(), done.Load())
	}
}

func TestExchangeRequestCloseReportsLateUploadFailure(t *testing.T) {
	stream, config, _ := exchangeFixture(t, bytes.NewReader(readerHeaders(t, responseFields("200")...)))
	entered, fail := make(chan struct{}), make(chan struct{})
	sentinel := errors.New("late upload failure")
	body := &uploadBody{read: func([]byte) (int, error) { close(entered); <-fail; return 0, sentinel }}
	request, _ := http.NewRequest("POST", "https://example.com/", body)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	response, err := ExchangeRequest(ctx, request, stream, config)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, err := io.ReadAll(response.Body); err != nil {
		t.Fatal(err)
	}
	close(fail)
	select {
	case <-stream.Context().Done():
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := response.Body.Close(); !errors.Is(err, sentinel) {
		t.Fatal("upload failure hidden", err)
	}
}

func TestExchangeRequestPreflightReleasesOwnership(t *testing.T) {
	stream, config, done := exchangeFixture(t, bytes.NewReader(nil))
	body := &uploadBody{read: func([]byte) (int, error) { t.Error("body read during preflight"); return 0, io.EOF }}
	request, _ := http.NewRequest("POST", "https://example.com/", body)
	request.Header.Set("Connection", "keep-alive")
	if response, err := ExchangeRequest(context.Background(), request, stream, config); err == nil || response != nil {
		t.Fatal(response, err)
	}
	if stream.Len() != 0 || len(stream.readCodes) != 1 || len(stream.writeCodes) != 1 || body.closes.Load() != 1 || done.Load() != 1 {
		t.Fatal("preflight ownership not released")
	}
}

func TestExchangeRequestCompletedResponseClosesAfterDecoderShutdown(t *testing.T) {
	stream, config, done := exchangeFixture(t, bytes.NewReader(readerHeaders(t, responseFields("200")...)))
	request, _ := http.NewRequest("GET", "https://example.com/", nil)
	response, err := ExchangeRequest(context.Background(), request, stream, config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(response.Body); err != nil {
		t.Fatal(err)
	}
	config.Decoder.Close(&quic.PeerCloseError{Application: true, Code: 0x100})
	if err := response.Body.Close(); err != nil {
		t.Fatal("completed response cleanup failed", err)
	}
	if err := response.Body.Close(); err != nil || done.Load() != 1 {
		t.Fatal("cleanup ownership", err, done.Load())
	}
}
