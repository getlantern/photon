package http3

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
)

func TestClientRejectsServerBidirectionalStream(t *testing.T) {
	origin := httptest.NewTLSServer(nil)
	serverTLS := origin.TLS.Clone()
	origin.Close()
	serverTLS.NextProtos = []string{"h3"}
	listener, err := quic.ListenAddr("127.0.0.1:0", serverTLS, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	peerSaw := make(chan error, 1)
	go func() {
		conn, err := listener.Accept(ctx)
		if err != nil {
			peerSaw <- err
			return
		}
		defer conn.CloseWithError(0x100, "")
		stream, err := conn.OpenStreamSync(ctx)
		if err == nil {
			// A stream exists for the peer only once a frame names it.
			_, err = stream.Write([]byte{0})
		}
		if err != nil {
			peerSaw <- err
			return
		}
		select {
		case <-conn.Context().Done():
			peerSaw <- context.Cause(conn.Context())
		case <-ctx.Done():
			peerSaw <- ctx.Err()
		}
	}()
	// The stream can arrive during initialization, which then fails the same way.
	want := &Error{ErrorCode: ErrCodeStreamCreationError}
	client, err := Dial(ctx, "https://"+listener.Addr().String(), &tls.Config{InsecureSkipVerify: true}, nil, nil)
	if err != nil && !errors.Is(err, want) {
		t.Fatal(err)
	}
	var closed *quic.PeerCloseError
	if err := <-peerSaw; !errors.As(err, &closed) || !closed.Application || closed.Code != uint64(ErrCodeStreamCreationError) {
		t.Fatal("server-initiated bidirectional stream was not a connection error:", err)
	}
	if client == nil {
		return
	}
	defer client.Close()
	// No SETTINGS ever arrive, so only the connection error can end this request.
	request, _ := http.NewRequestWithContext(ctx, "GET", "https://"+listener.Addr().String()+"/", nil)
	if _, err := client.RoundTrip(request); !errors.Is(err, want) {
		t.Fatal("request did not report the protocol error:", err)
	}
	done := make(chan struct{})
	go func() { defer close(done); client.Close() }()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("Close did not join the stream monitor")
	}
}
