package http3conn

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type uploadBody struct {
	read   func([]byte) (int, error)
	close  func() error
	closes atomic.Int32
}

func (b *uploadBody) Read(p []byte) (int, error) { return b.read(p) }
func (b *uploadBody) Close() error {
	b.closes.Add(1)
	if b.close != nil {
		return b.close()
	}
	return nil
}
func uploadWriter(t *testing.T, sink *messageSink) *MessageWriter {
	t.Helper()
	writer := responseWriter(t, sink, func(name string) bool { return name == "x-checksum" })
	writer.message, _ = NewMessage(MessageConfig{Kind: RequestFields})
	return writer
}
func TestWriteHTTPRequestBodyAndEOFTrailers(t *testing.T) {
	request, _ := http.NewRequest("POST", "https://example.com/", nil)
	request.Trailer = http.Header{"X-Checksum": nil}
	body := &uploadBody{read: func(p []byte) (int, error) { request.Trailer.Set("X-Checksum", "ok"); return copy(p, "abc"), io.EOF }}
	request.Body = body
	sink := new(messageSink)
	if err := WriteHTTPRequest(context.Background(), request, uploadWriter(t, sink), func() { t.Error("unexpected interrupt") }); err != nil {
		t.Fatal(err)
	}
	if body.closes.Load() != 1 || sink.closes != 1 {
		t.Fatal("completion ownership", body.closes.Load(), sink.closes)
	}
	received, _, err := ReadHTTPRequest(context.Background(), requestReader(t, sink.Bytes(), false), BodyConfig{Close: func(bool) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer received.Body.Close()
	payload, err := io.ReadAll(received.Body)
	if err != nil || string(payload) != "abc" || received.Trailer.Get("X-Checksum") != "ok" {
		t.Fatal(string(payload), received.Trailer, err)
	}
}
func TestWriteHTTPRequestFailureOwnership(t *testing.T) {
	sentinel := errors.New("body failed")
	for _, mode := range []string{"read", "close", "length-short", "length-long", "new-trailer", "permission", "headers", "wire", "no-progress"} {
		t.Run(mode, func(t *testing.T) {
			request, _ := http.NewRequest("POST", "https://example.com/", nil)
			reader := strings.NewReader("abc")
			body := &uploadBody{read: reader.Read}
			request.Body = body
			sink := new(messageSink)
			writer := uploadWriter(t, sink)
			switch mode {
			case "read":
				body.read = func(p []byte) (int, error) { return copy(p, "abc"), sentinel }
			case "close":
				body.close = func() error { return sentinel }
			case "length-short":
				request.ContentLength = 4
			case "length-long":
				request.ContentLength = 2
			case "new-trailer":
				body.read = func([]byte) (int, error) { request.Trailer = http.Header{"X-Checksum": {"late"}}; return 0, io.EOF }
			case "permission":
				request.Trailer = http.Header{"X-Unpermitted": nil}
			case "headers":
				request.Header.Set("Connection", "keep-alive")
			case "wire":
				sink.failCall = 1
			case "no-progress":
				body.read = func([]byte) (int, error) { return 0, nil }
			}
			err := WriteHTTPRequest(context.Background(), request, writer, func() {})
			if err == nil || body.closes.Load() != 1 || sink.closes != 0 {
				t.Fatal(err, body.closes.Load(), sink.closes)
			}
			if (mode == "permission" || mode == "headers") && sink.Len() != 0 {
				t.Fatal("preflight failure wrote bytes")
			}
			if mode == "no-progress" && !errors.Is(err, io.ErrNoProgress) {
				t.Fatal(err)
			}
		})
	}
}
func TestWriteHTTPRequestCancellationClosesBlockedBody(t *testing.T) {
	pipe, producer := io.Pipe()
	defer producer.Close()
	defer pipe.Close()
	entered := make(chan struct{})
	body := &uploadBody{read: func(p []byte) (int, error) { close(entered); return pipe.Read(p) }, close: pipe.Close}
	request, _ := http.NewRequest("POST", "https://example.com/", body)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(context.Canceled)
	cause := errors.New("early response stopped upload")
	sink := new(messageSink)
	writer := uploadWriter(t, sink)
	var interrupts atomic.Int32
	done := make(chan error, 1)
	go func() { done <- WriteHTTPRequest(ctx, request, writer, func() { interrupts.Add(1) }) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("body read missing")
	}
	cancel(cause)
	select {
	case err := <-done:
		if !errors.Is(err, cause) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("body close did not unblock upload")
	}
	if body.closes.Load() != 1 || interrupts.Load() != 1 || sink.closes != 0 {
		t.Fatal(body.closes.Load(), interrupts.Load(), sink.closes)
	}
}

type blockedUploadSink struct {
	entered, stop chan struct{}
	closes        atomic.Int32
}

func (s *blockedUploadSink) Write([]byte) (int, error) {
	close(s.entered)
	<-s.stop
	return 0, io.ErrClosedPipe
}
func (s *blockedUploadSink) Close() error { s.closes.Add(1); return nil }
func TestWriteHTTPRequestCancellationInterruptsHeaderWrite(t *testing.T) {
	sink := &blockedUploadSink{entered: make(chan struct{}), stop: make(chan struct{})}
	writer := uploadWriter(t, new(messageSink))
	writer.stream = sink
	body := &uploadBody{read: func([]byte) (int, error) { t.Error("body read before headers sent"); return 0, io.EOF }}
	request, _ := http.NewRequest("POST", "https://example.com/", body)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- WriteHTTPRequest(ctx, request, writer, func() { close(sink.stop) }) }()
	select {
	case <-sink.entered:
	case <-time.After(time.Second):
		t.Fatal("header write missing")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("header write did not unblock")
	}
	if body.closes.Load() != 1 || sink.closes.Load() != 0 {
		t.Fatal(body.closes.Load(), sink.closes.Load())
	}
}

// Bodies may opt into failure propagation without changing ordinary net/http
// body ownership. The cancellation callback and deferred cleanup share one close.
type causeUploadBody struct {
	uploadBody
	cause       error
	errorCloses atomic.Int32
}

func (b *causeUploadBody) CloseWithError(err error) error {
	b.errorCloses.Add(1)
	b.cause = err
	return b.uploadBody.Close()
}

func TestWriteHTTPRequestCloseCause(t *testing.T) {
	for _, mode := range []string{"success", "read", "wire", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			cause := errors.New("upload failure")
			body := &causeUploadBody{uploadBody: uploadBody{read: strings.NewReader("abc").Read}}
			request, _ := http.NewRequest("POST", "https://example.com/", body)
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(context.Canceled)
			sink := new(messageSink)
			switch mode {
			case "read":
				body.read = func([]byte) (int, error) { return 0, cause }
			case "wire":
				sink.failCall = 1
			case "canceled":
				cancel(cause)
			}
			err := WriteHTTPRequest(ctx, request, uploadWriter(t, sink), func() {})
			if body.closes.Load() != 1 {
				t.Fatal("body closed more than once", body.closes.Load())
			}
			if mode == "success" {
				if err != nil || body.errorCloses.Load() != 0 || body.cause != nil {
					t.Fatal(err, body.cause)
				}
			} else if err == nil || body.errorCloses.Load() != 1 || body.cause != err {
				t.Fatal("failure cause", err, body.cause, body.errorCloses.Load())
			}
		})
	}
}
