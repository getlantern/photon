package http3

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
)

func testServerTLS() *tls.Config {
	origin := httptest.NewTLSServer(nil)
	config := origin.TLS.Clone()
	origin.Close()
	config.NextProtos = []string{NextProtoH3}
	return config
}

func dialTestServer(ctx context.Context, t *testing.T, server *Server, config *ClientConfig) (*ClientConn, string) {
	t.Helper()
	address := listenTestServer(ctx, t, server)
	client, err := Dial(ctx, "https://"+address, &tls.Config{InsecureSkipVerify: true}, nil, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	return client, "https://" + address
}

func await[T any](ctx context.Context, t *testing.T, what string, values <-chan T) T {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-ctx.Done():
		t.Fatal("timed out waiting for", what)
		panic("unreachable")
	}
}

// takenStream is what a handler under test hands back after HTTPStream.
type takenStream struct {
	stream  *Stream
	request *http.Request
	err     error
}

// takeOver returns a handler that sets status, takes its stream over and
// returns, leaving the stream to the test.
func takeOver(status int, taken chan<- takenStream) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if status != 0 {
			w.WriteHeader(status)
		}
		stream, err := w.(HTTPStreamer).HTTPStream()
		taken <- takenStream{stream: stream, request: r, err: err}
	})
}

// The stream must stay usable after the handler returned: the response body is
// only written then, and the request body is only sent and read then.
func TestHTTPStreamOutlivesHandler(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	taken := make(chan takenStream, 1)
	client, origin := dialTestServer(ctx, t, &Server{Handler: takeOver(http.StatusAccepted, taken)}, nil)
	input, output := io.Pipe()
	request, _ := http.NewRequestWithContext(ctx, "POST", origin+"/", input)
	response, err := client.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		t.Fatal(response.Status)
	}
	server := await(ctx, t, "takeover", taken)
	if server.err != nil {
		t.Fatal(server.err)
	}
	stream := server.stream
	if stream.StreamID() != 0 || stream.QUICStream() == nil || stream.QUICStream().StreamID() != 0 {
		t.Fatal("wrong stream", stream.StreamID())
	}
	if _, err := stream.Write([]byte("after ")); err != nil {
		t.Fatal(err)
	}
	if err := stream.TryWriteAll([]byte("handler")); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-stream.Context().Done():
	case <-ctx.Done():
		t.Fatal("Close left the send context open")
	}
	// The handler returned before the response body was even written.
	body, err := io.ReadAll(response.Body)
	if err != nil || string(body) != "after handler" {
		t.Fatalf("%q %v", body, err)
	}
	if _, err := output.Write([]byte("request body")); err != nil {
		t.Fatal(err)
	}
	output.Close()
	// Sending is finished but receiving is not, so the request is still live.
	if err := server.request.Context().Err(); err != nil {
		t.Fatal("request context ended with the handler:", context.Cause(server.request.Context()))
	}
	received, err := io.ReadAll(stream)
	if err != nil || string(received) != "request body" {
		t.Fatalf("%q %v", received, err)
	}
	// Both directions are finished, so the request ends.
	select {
	case <-server.request.Context().Done():
	case <-ctx.Done():
		t.Fatal("request never ended after its stream finished")
	}
}

func TestHTTPStreamAfterBodyFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := make(chan error, 1)
	client, origin := dialTestServer(ctx, t, &Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "body")
		_, err := w.(HTTPStreamer).HTTPStream()
		result <- err
		// The refusal leaves the ordinary response intact.
		io.WriteString(w, " continues")
	})}, nil)
	request, _ := http.NewRequestWithContext(ctx, "GET", origin+"/", nil)
	response, err := client.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if err := await(ctx, t, "handler", result); err == nil {
		t.Fatal("took over a stream after writing the response body")
	}
	if body, err := io.ReadAll(response.Body); err != nil || string(body) != "body continues" {
		t.Fatalf("%q %v", body, err)
	}
}

// Without a final status no response is sent, and none can be.
func TestHTTPStreamWithoutHeader(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	failures := make(chan string, 8)
	client, origin := dialTestServer(ctx, t, &Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(failures)
		stream, err := w.(HTTPStreamer).HTTPStream()
		if err != nil {
			failures <- "takeover: " + err.Error()
			return
		}
		if again, err := w.(HTTPStreamer).HTTPStream(); err != nil || again != stream {
			failures <- "second HTTPStream did not return the same stream"
		}
		if _, err := stream.Write([]byte("x")); err == nil {
			failures <- "wrote DATA without a response header"
		}
		if err := stream.TryWriteAll([]byte("x")); err == nil {
			failures <- "queued DATA without a response header"
		}
		if _, err := w.Write([]byte("x")); err != http.ErrHijacked {
			failures <- "ResponseWriter.Write after takeover: " + errorText(err)
		}
		func() {
			defer func() {
				if recover() == nil {
					failures <- "WriteHeader after takeover did not panic"
				}
			}()
			w.WriteHeader(http.StatusOK)
		}()
		stream.CancelRead(quic.StreamErrorCode(ErrCodeRequestCanceled))
		stream.CancelWrite(quic.StreamErrorCode(ErrCodeConnectError))
	})}, nil)
	request, _ := http.NewRequestWithContext(ctx, "GET", origin+"/", nil)
	response, err := client.RoundTrip(request)
	if err == nil {
		response.Body.Close()
		t.Fatal("received a response that was never sent:", response.Status)
	}
	if want := (&Error{Remote: true, ErrorCode: ErrCodeConnectError}); !errors.Is(err, want) {
		t.Fatal(err)
	}
	for failure := range failures {
		t.Error(failure)
	}
}

func errorText(err error) string {
	if err == nil {
		return "<nil>"
	}
	return err.Error()
}

// Shutdown must treat a taken-over stream as a request still in progress.
func TestHTTPStreamHoldsShutdown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	taken := make(chan takenStream, 1)
	server := &Server{Handler: takeOver(http.StatusOK, taken)}
	client, origin := dialTestServer(ctx, t, server, nil)
	input, output := io.Pipe()
	request, _ := http.NewRequestWithContext(ctx, "POST", origin+"/", input)
	response, err := client.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	stream := await(ctx, t, "takeover", taken)
	if stream.err != nil {
		t.Fatal(stream.err)
	}
	shutdown := make(chan error, 1)
	go func() { shutdown <- server.Shutdown(ctx) }()
	// GOAWAY proves Shutdown is under way; it waits for the stream from here on.
	select {
	case <-client.session.GoawayReceived():
	case <-ctx.Done():
		t.Fatal("no GOAWAY")
	}
	later, _ := http.NewRequestWithContext(ctx, "GET", origin+"/", nil)
	if _, err := client.RoundTrip(later); !errors.Is(err, ErrPeerDraining) {
		t.Fatal("request admitted after GOAWAY:", err)
	}
	// The stream must have survived GOAWAY in both directions.
	if _, err := stream.stream.Write([]byte("down")); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(response.Body, make([]byte, 4)); err != nil {
		t.Fatal(err)
	}
	if _, err := output.Write([]byte("up")); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(stream.stream, make([]byte, 2)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-shutdown:
		t.Fatal("Shutdown returned with a taken-over stream open:", err)
	default:
	}
	output.Close()
	if err := stream.stream.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(stream.stream); err != nil {
		t.Fatal(err)
	}
	if err := await(ctx, t, "Shutdown", shutdown); err != nil {
		t.Fatal(err)
	}
}

func echoRequestBody(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Method", r.Method)
	w.WriteHeader(http.StatusOK)
	w.(http.Flusher).Flush()
	buffer := make([]byte, 1024)
	for {
		n, err := r.Body.Read(buffer)
		if n > 0 {
			w.Write(buffer[:n])
			w.(http.Flusher).Flush()
		}
		if err != nil {
			return
		}
	}
}

func TestRequestStream(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, origin := dialTestServer(ctx, t, &Server{Handler: http.HandlerFunc(echoRequestBody)}, &ClientConfig{MaxConcurrentRequests: 1})
	stream, err := client.OpenRequestStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write([]byte("early")); err == nil {
		t.Fatal("wrote before the request header")
	}
	if _, err := stream.Read(make([]byte, 1)); err == nil {
		t.Fatal("read before the response")
	}
	if _, err := stream.ReadResponse(); err == nil {
		t.Fatal("read a response before the request")
	}
	withBody, _ := http.NewRequestWithContext(ctx, "POST", origin+"/", bytes.NewReader([]byte("x")))
	if err := stream.SendRequestHeader(withBody); err == nil {
		t.Fatal("accepted a request with a body")
	}
	request, _ := http.NewRequestWithContext(ctx, "POST", origin+"/echo", nil)
	if err := stream.SendRequestHeader(request); err != nil {
		t.Fatal(err)
	}
	if err := stream.SendRequestHeader(request); err == nil {
		t.Fatal("sent the request header twice")
	}
	response, err := stream.ReadResponse()
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != 200 || response.Header.Get("X-Method") != "POST" || response.Request != nil || response.TLS != nil {
		t.Fatal(response.StatusCode, response.Header, response.Request, response.TLS)
	}
	// Request and response bodies interleave on the one stream.
	for _, chunk := range []string{"first", "second"} {
		if _, err := stream.Write([]byte(chunk)); err != nil {
			t.Fatal(err)
		}
		echo := make([]byte, len(chunk))
		if _, err := io.ReadFull(stream, echo); err != nil || string(echo) != chunk {
			t.Fatalf("%q %v", echo, err)
		}
	}
	if err := stream.TryWriteAll([]byte("third")); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	// The response body and the stream are the same receive direction.
	rest, err := io.ReadAll(response.Body)
	if err != nil || string(rest) != "third" {
		t.Fatalf("%q %v", rest, err)
	}
	if stream.StreamID() != 0 {
		t.Fatal(stream.StreamID())
	}
	// With one slot, a second stream can open only once the first released it.
	second, err := client.OpenRequestStream(ctx)
	if err != nil {
		t.Fatal("finished stream kept its request slot:", err)
	}
	if len(client.slots) != 1 {
		t.Fatal("open stream holds no request slot")
	}
	// An unused stream is finished by canceling both directions.
	second.CancelRead(quic.StreamErrorCode(ErrCodeRequestCanceled))
	if len(client.slots) != 1 {
		t.Fatal("slot released with the send direction open")
	}
	second.CancelWrite(quic.StreamErrorCode(ErrCodeRequestCanceled))
	third, err := client.OpenRequestStream(ctx)
	if err != nil {
		t.Fatal("canceled stream kept its request slot:", err)
	}
	// Closing the response body is the other way to finish receiving.
	request, _ = http.NewRequestWithContext(ctx, "GET", origin+"/", nil)
	if err := third.SendRequestHeader(request); err != nil {
		t.Fatal(err)
	}
	if err := third.Close(); err != nil {
		t.Fatal(err)
	}
	if response, err = third.ReadResponse(); err != nil {
		t.Fatal(err)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if fourth, err := client.OpenRequestStream(ctx); err != nil {
		t.Fatal("closed response body kept its request slot:", err)
	} else {
		fourth.CancelRead(0x10c)
		fourth.CancelWrite(0x10c)
	}
}

// A slot wait is a real wait: it ends with its context, not with a stream.
func TestRequestStreamSlotWaitHonorsContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, _ := dialTestServer(ctx, t, &Server{Handler: http.HandlerFunc(echoRequestBody)}, &ClientConfig{MaxConcurrentRequests: 1})
	held, err := client.OpenRequestStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { held.CancelRead(0x10c); held.CancelWrite(0x10c) }()
	cause := errors.New("stopped waiting")
	waiting, stop := context.WithCancelCause(ctx)
	stop(cause)
	if _, err := client.OpenRequestStream(waiting); !errors.Is(err, cause) {
		t.Fatal("opened a stream beyond the request limit:", err)
	}
}

// Both ends raw: an Extended CONNECT stream with DATA and datagrams each way.
func TestRequestStreamToHTTPStream(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	taken := make(chan takenStream, 1)
	server := &Server{DatagramHandlers: map[string]http.Handler{"raw-test": takeOver(http.StatusOK, taken)}}
	client, origin := dialTestServer(ctx, t, server, &ClientConfig{EnableDatagrams: true})
	stream, err := client.OpenRequestStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	request, _ := http.NewRequestWithContext(ctx, http.MethodConnect, origin+"/session", nil)
	request.Proto = "raw-test"
	if err := stream.SendRequestHeader(request); err != nil {
		t.Fatal(err)
	}
	response, err := stream.ReadResponse()
	if err != nil || response.StatusCode != 200 {
		t.Fatal(response, err)
	}
	peer := await(ctx, t, "takeover", taken)
	if peer.err != nil {
		t.Fatal(peer.err)
	}
	if ExtendedConnectProtocol(peer.request) != "raw-test" || peer.request.URL.Path != "/session" || peer.stream.StreamID() != stream.StreamID() {
		t.Fatal(ExtendedConnectProtocol(peer.request), peer.request.URL, peer.stream.StreamID())
	}
	// The response implies the server's SETTINGS, which datagrams need.
	select {
	case <-client.ReceivedSettings():
	case <-ctx.Done():
		t.Fatal("no SETTINGS")
	}
	if err := stream.SendDatagram([]byte("to server")); err != nil {
		t.Fatal(err)
	}
	if payload, err := peer.stream.ReceiveDatagram(ctx); err != nil || string(payload) != "to server" {
		t.Fatalf("%q %v", payload, err)
	}
	if err := peer.stream.SendDatagram([]byte("to client")); err != nil {
		t.Fatal(err)
	}
	if payload, err := stream.ReceiveDatagram(ctx); err != nil || string(payload) != "to client" {
		t.Fatalf("%q %v", payload, err)
	}
	// An expired read deadline is not the end of the stream.
	if err := stream.SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Read(make([]byte, 1)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatal(err)
	}
	if err := peer.stream.SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.stream.Read(make([]byte, 1)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatal(err)
	}
	stream.SetDeadline(time.Time{})
	peer.stream.SetDeadline(time.Time{})
	peer.stream.SetPriority(1, false)
	if _, err := peer.stream.Write([]byte("down")); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write([]byte("up")); err != nil {
		t.Fatal(err)
	}
	down, up := make([]byte, 4), make([]byte, 2)
	if _, err := io.ReadFull(stream, down); err != nil || string(down) != "down" {
		t.Fatalf("%q %v", down, err)
	}
	if _, err := io.ReadFull(peer.stream, up); err != nil || string(up) != "up" {
		t.Fatalf("%q %v", up, err)
	}
	// A reset of one direction is reported there and leaves the other alone.
	stream.CancelWrite(quic.StreamErrorCode(ErrCodeConnectError))
	if _, err := peer.stream.Read(up); !errors.Is(err, &Error{Remote: true, ErrorCode: ErrCodeConnectError}) {
		t.Fatal(err)
	}
	if _, err := peer.stream.Write([]byte("still")); err != nil {
		t.Fatal("peer reset of one direction closed the other:", err)
	}
	if err := peer.stream.Close(); err != nil {
		t.Fatal(err)
	}
	if rest, err := io.ReadAll(stream); err != nil || string(rest) != "still" {
		t.Fatalf("%q %v", rest, err)
	}
	// Datagram reception ends with the receive direction.
	if _, err := peer.stream.ReceiveDatagram(ctx); !errors.Is(err, ErrDatagramRequestClosed) {
		t.Fatal(err)
	}
}

// Datagram semantics come from DatagramHandlers, not from taking a stream over.
func TestHTTPStreamWithoutDatagramSemantics(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	taken := make(chan takenStream, 1)
	server := &Server{EnableDatagrams: true, Handler: takeOver(http.StatusOK, taken)}
	client, origin := dialTestServer(ctx, t, server, &ClientConfig{EnableDatagrams: true})
	request, _ := http.NewRequestWithContext(ctx, "GET", origin+"/", nil)
	response, err := client.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	peer := await(ctx, t, "takeover", taken)
	if peer.err != nil {
		t.Fatal(peer.err)
	}
	if err := peer.stream.SendDatagram([]byte("x")); !errors.Is(err, ErrHTTPDatagramsNotNegotiated) {
		t.Fatal(err)
	}
	if _, err := peer.stream.ReceiveDatagram(ctx); !errors.Is(err, ErrHTTPDatagramsNotNegotiated) {
		t.Fatal(err)
	}
	peer.stream.CancelRead(quic.StreamErrorCode(ErrCodeNoError))
	if err := peer.stream.Close(); err != nil {
		t.Fatal(err)
	}
}

// A GOAWAY refuses new raw streams and keeps the one already open.
func TestRequestStreamGoaway(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	server := &Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		select {
		case <-release:
			io.WriteString(w, "kept")
		case <-r.Context().Done():
		}
	})}
	client, origin := dialTestServer(ctx, t, server, nil)
	stream, err := client.OpenRequestStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	request, _ := http.NewRequestWithContext(ctx, "GET", origin+"/", nil)
	if err := stream.SendRequestHeader(request); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	await(ctx, t, "handler", entered)
	shutdown := make(chan error, 1)
	go func() { shutdown <- server.Shutdown(ctx) }()
	select {
	case <-client.session.GoawayReceived():
	case <-ctx.Done():
		t.Fatal("no GOAWAY")
	}
	if _, err := client.OpenRequestStream(ctx); !errors.Is(err, ErrPeerDraining) {
		t.Fatal("stream opened after GOAWAY:", err)
	}
	close(release)
	response, err := stream.ReadResponse()
	if err != nil {
		t.Fatal("GOAWAY reset a stream below its limit:", err)
	}
	if body, err := io.ReadAll(response.Body); err != nil || string(body) != "kept" {
		t.Fatalf("%q %v", body, err)
	}
	if err := await(ctx, t, "Shutdown", shutdown); err != nil {
		t.Fatal(err)
	}
}

// Closing the client ends every raw stream and joins their cleanup.
func TestRequestStreamClientClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, origin := dialTestServer(ctx, t, &Server{Handler: http.HandlerFunc(echoRequestBody)}, nil)
	stream, err := client.OpenRequestStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	request, _ := http.NewRequestWithContext(ctx, "POST", origin+"/", nil)
	if err := stream.SendRequestHeader(request); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.ReadResponse(); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if len(client.slots) != 0 {
		t.Fatal("Close returned before the raw stream released its slot")
	}
	if _, err := stream.Read(make([]byte, 1)); !errors.Is(err, ErrClientClosed) {
		t.Fatal(err)
	}
	if _, err := stream.Write([]byte("x")); !errors.Is(err, ErrClientClosed) {
		t.Fatal(err)
	}
	if _, err := client.OpenRequestStream(ctx); !errors.Is(err, ErrClientClosed) {
		t.Fatal(err)
	}
}

// A taken-over stream ends with its connection: nothing is left waiting on it.
func TestHTTPStreamConnectionLoss(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	taken := make(chan takenStream, 1)
	server := &Server{Handler: takeOver(http.StatusOK, taken)}
	client, origin := dialTestServer(ctx, t, server, nil)
	// The request body never ends, so only the connection can end the Read.
	input, output := io.Pipe()
	defer output.Close()
	request, _ := http.NewRequestWithContext(ctx, "POST", origin+"/", input)
	response, err := client.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	peer := await(ctx, t, "takeover", taken)
	if peer.err != nil {
		t.Fatal(peer.err)
	}
	result := make(chan error, 1)
	go func() { _, err := peer.stream.Read(make([]byte, 1)); result <- err }()
	client.CloseWithError(quic.ApplicationErrorCode(ErrCodeConnectError), "gone")
	lost := &Error{Remote: true, ErrorCode: ErrCodeConnectError}
	if err := await(ctx, t, "Read", result); !errors.Is(err, lost) {
		t.Fatal(err)
	}
	select {
	case <-peer.request.Context().Done():
	case <-ctx.Done():
		t.Fatal("request outlived its connection")
	}
	if _, err := peer.stream.Write([]byte("x")); !errors.Is(err, lost) {
		t.Fatal(err)
	}
	// Shutdown has nothing left to wait for.
	if err := server.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

// A handler may finish receiving before it takes the stream over. The stream
// then has only its send direction left to finish.
func TestHTTPStreamAfterRequestBodyFinished(t *testing.T) {
	for _, mode := range []string{"read to EOF", "closed"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			taken := make(chan takenStream, 1)
			server := &Server{MaxConcurrentRequests: 1, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if mode == "closed" {
					r.Body.Close()
				} else if body, err := io.ReadAll(r.Body); err != nil || string(body) != "request body" {
					taken <- takenStream{err: fmt.Errorf("request body %q: %v", body, err)}
					return
				}
				w.WriteHeader(http.StatusOK)
				stream, err := w.(HTTPStreamer).HTTPStream()
				if err == nil {
					if _, err = stream.Write([]byte("done")); err == nil {
						err = stream.Close()
					}
				}
				taken <- takenStream{stream: stream, request: r, err: err}
			})}
			client, origin := dialTestServer(ctx, t, server, nil)
			request, _ := http.NewRequestWithContext(ctx, "POST", origin+"/", strings.NewReader("request body"))
			response, err := client.RoundTrip(request)
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(response.Body)
			response.Body.Close()
			if err != nil || string(body) != "done" {
				t.Fatalf("%q %v", body, err)
			}
			peer := await(ctx, t, "takeover", taken)
			if peer.err != nil {
				t.Fatal(peer.err)
			}
			select {
			case <-peer.request.Context().Done():
			case <-ctx.Done():
				t.Fatal("request waits for a receive direction that finished before takeover")
			}
			// Its request slot, the server's only one, is free again.
			request, _ = http.NewRequestWithContext(ctx, "POST", origin+"/", strings.NewReader("request body"))
			if response, err = client.RoundTrip(request); err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
		})
	}
}

// A request-body read can be in progress when the handler takes the stream
// over. When it then reaches the end of the request, the stream must learn of
// it like any later read: only its send direction is left to finish.
func TestHTTPStreamBodyReadAcrossTakeover(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	taken := make(chan takenStream, 1)
	finished := make(chan error, 1)
	server := &Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reading := make(chan struct{})
		go func() {
			close(reading)
			_, err := io.Copy(io.Discard, r.Body)
			finished <- err
		}()
		<-reading
		w.WriteHeader(http.StatusOK)
		stream, err := w.(HTTPStreamer).HTTPStream()
		taken <- takenStream{stream: stream, request: r, err: err}
	})}
	client, origin := dialTestServer(ctx, t, server, nil)
	input, output := io.Pipe()
	request, _ := http.NewRequestWithContext(ctx, "POST", origin+"/", input)
	response, err := client.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	peer := await(ctx, t, "takeover", taken)
	if peer.err != nil {
		t.Fatal(peer.err)
	}
	// Only now does the request end, under the read that predates the takeover.
	output.Close()
	if err := await(ctx, t, "request body read", finished); err != nil {
		t.Fatal(err)
	}
	if err := peer.stream.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-peer.request.Context().Done():
	case <-ctx.Done():
		t.Fatal("stream waits for a receive direction that a read in progress at takeover finished")
	}
}
