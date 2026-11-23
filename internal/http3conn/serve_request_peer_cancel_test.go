package http3conn

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
)

type completionServeStream struct {
	*serveStream
	send, receive context.Context
}

func (s *completionServeStream) Context() context.Context        { return s.send }
func (s *completionServeStream) ReceiveContext() context.Context { return s.receive }

func TestServeRequestPeerCancellationCancelsIdleHandler(t *testing.T) {
	for _, mode := range []string{"reset", "stop-sending", "fin"} {
		t.Run(mode, func(t *testing.T) {
			send, cancelSend := context.WithCancelCause(context.Background())
			defer cancelSend(context.Canceled)
			receive, cancelReceive := context.WithCancelCause(context.Background())
			defer cancelReceive(context.Canceled)
			stream := &completionServeStream{serveStream: &serveStream{source: bytes.NewReader(readerHeaders(t, requestFields("/")...))}, send: send, receive: receive}
			entered, release := make(chan struct{}), make(chan struct{})
			// The handler performs no stream I/O, so only its context can wake it.
			config := serveConfig(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(entered)
				select {
				case <-r.Context().Done():
				case <-release:
				}
			}))
			done := make(chan error, 1)
			go func() { done <- ServeRequest(context.Background(), stream, config) }()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("handler not dispatched")
			}
			cause := &quic.StreamError{Remote: true, ErrorCode: 0x10c}
			switch mode {
			case "reset":
				cancelReceive(cause)
			case "stop-sending":
				cancelSend(cause)
			case "fin":
				cancelReceive(io.EOF)
				close(release)
			}
			select {
			case err := <-done:
				if mode == "fin" {
					if err != nil || stream.closes != 1 || len(stream.writeCodes) != 0 {
						t.Fatal("request FIN canceled the response", err, stream.closes, stream.writeCodes)
					}
				} else if err != error(cause) || stream.closes != 0 || len(stream.writeCodes) != 1 {
					t.Fatal(err, stream.closes, stream.writeCodes)
				}
			case <-time.After(time.Second):
				close(release)
				<-done
				t.Fatal("peer stream cancellation did not cancel the handler context")
			}
		})
	}
}
