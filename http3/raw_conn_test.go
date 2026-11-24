package http3

import (
	"compress/gzip"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
)

// rawServer is an application that owns its accept loops, as a WebTransport
// server would, and hands HTTP/3 its streams.
type rawServer struct {
	listener *quic.Listener
	conns    chan *quic.Conn
	raw      chan *RawServerConn
	// handled reports, for each request stream, whether its handler had
	// finished when HandleRequestStream returned.
	handled  chan bool
	finished atomic.Bool
	loops    sync.WaitGroup
}

func startRawServer(ctx context.Context, t *testing.T, server *Server, alpn string) *rawServer {
	t.Helper()
	config := testServerTLS()
	config.NextProtos = []string{alpn}
	transport := &quic.Config{}
	if server.QUICConfig != nil {
		transport = server.QUICConfig.Clone()
	}
	transport.EnableDatagrams = transport.EnableDatagrams || server.EnableDatagrams
	listener, err := quic.ListenAddr("127.0.0.1:0", config, transport)
	if err != nil {
		t.Fatal(err)
	}
	app := &rawServer{listener: listener, conns: make(chan *quic.Conn, 1), raw: make(chan *RawServerConn, 1), handled: make(chan bool, 16)}
	app.loops.Add(1)
	go func() {
		defer app.loops.Done()
		conn, err := listener.Accept(ctx)
		if err != nil {
			return
		}
		app.conns <- conn
		if alpn != NextProtoH3 {
			return
		}
		raw, err := server.NewRawServerConn(conn)
		if err != nil {
			t.Error(err)
			return
		}
		app.raw <- raw
		app.loops.Add(2)
		go func() {
			defer app.loops.Done()
			for {
				stream, err := conn.AcceptUniStream(ctx)
				if err != nil {
					return
				}
				go raw.HandleUnidirectionalStream(stream)
			}
		}()
		go func() {
			defer app.loops.Done()
			for {
				stream, err := conn.AcceptStream(ctx)
				if err != nil {
					return
				}
				app.loops.Add(1)
				go func() {
					defer app.loops.Done()
					raw.HandleRequestStream(stream)
					app.handled <- app.finished.Load()
				}()
			}
		}()
	}()
	t.Cleanup(func() { server.Close(); listener.Close(); app.loops.Wait() })
	return app
}

func (s *rawServer) origin() string { return "https://" + s.listener.Addr().String() }

func dialRaw(ctx context.Context, t *testing.T, address, alpn string, config *quic.Config) *quic.Conn {
	t.Helper()
	conn, err := quic.DialAddr(ctx, address, &tls.Config{InsecureSkipVerify: true, NextProtos: []string{alpn}}, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.CloseWithError(quic.ApplicationErrorCode(ErrCodeNoError), "") })
	return conn
}

func TestRawServerConn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	entered, release := make(chan struct{}, 1), make(chan struct{})
	var app *rawServer
	server := &Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		<-release
		io.WriteString(w, "raw")
		app.finished.Store(true)
	})}
	app = startRawServer(ctx, t, server, NextProtoH3)
	client, err := Dial(ctx, app.origin(), &tls.Config{InsecureSkipVerify: true}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	result := make(chan error, 1)
	go func() {
		request, _ := http.NewRequestWithContext(ctx, "GET", app.origin()+"/", nil)
		response, err := client.RoundTrip(request)
		if err == nil {
			var body []byte
			if body, err = io.ReadAll(response.Body); err == nil && string(body) != "raw" {
				err = errors.New("wrong body: " + string(body))
			}
			response.Body.Close()
		}
		result <- err
	}()
	await(ctx, t, "handler", entered)
	select {
	case <-app.handled:
		t.Fatal("HandleRequestStream returned while its handler was running")
	default:
	}
	close(release)
	if err := await(ctx, t, "response", result); err != nil {
		t.Fatal(err)
	}
	if !await(ctx, t, "HandleRequestStream", app.handled) {
		t.Fatal("HandleRequestStream returned before its handler finished")
	}
	// Shutdown drains a raw connection like any other, then leaves closing it
	// to the client, as GOAWAY asks.
	shutdown := make(chan error, 1)
	go func() { shutdown <- server.Shutdown(ctx) }()
	select {
	case <-client.session.GoawayReceived():
	case <-client.session.Done():
		// A GOAWAY racing the close is still a GOAWAY.
		if _, draining := client.session.Goaway(); !draining {
			t.Fatal("raw connection closed without GOAWAY:", context.Cause(client.Context()))
		}
	case <-ctx.Done():
		t.Fatal("raw connection sent no GOAWAY")
	}
	// The requests GOAWAY let finish are done; closing is now the client's.
	client.Close()
	if err := await(ctx, t, "Shutdown", shutdown); err != nil {
		t.Fatal(err)
	}
	select {
	case <-client.Context().Done():
	case <-ctx.Done():
		t.Fatal("drained raw connection stayed open")
	}
	var closed *quic.PeerCloseError
	if cause := context.Cause(client.Context()); errors.As(cause, &closed) {
		t.Fatal("the server closed the drained connection itself:", cause)
	}
	raw := await(ctx, t, "raw connection", app.raw)
	select {
	case <-raw.done:
	case <-ctx.Done():
		t.Fatal("raw connection never finished serving")
	}
	if _, err := server.NewRawServerConn(await(ctx, t, "connection", app.conns)); !errors.Is(err, ErrServerClosed) {
		t.Fatal("stopped server admitted a raw connection:", err)
	}
}

// After Shutdown, a request stream the application hands over is refused.
func TestRawServerConnRejectsStreamAfterShutdown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	taken := make(chan takenStream, 1)
	server := &Server{Handler: takeOver(http.StatusOK, taken)}
	app := startRawServer(ctx, t, server, NextProtoH3)
	client, err := Dial(ctx, app.origin(), &tls.Config{InsecureSkipVerify: true}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	// A taken-over stream keeps the drain, and so the connection, open.
	held, err := client.OpenRequestStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	request, _ := http.NewRequestWithContext(ctx, "GET", app.origin()+"/", nil)
	if err := held.SendRequestHeader(request); err != nil {
		t.Fatal(err)
	}
	peer := await(ctx, t, "takeover", taken)
	if peer.err != nil {
		t.Fatal(peer.err)
	}
	shutdown := make(chan error, 1)
	go func() { shutdown <- server.Shutdown(ctx) }()
	select {
	case <-client.session.GoawayReceived():
	case <-ctx.Done():
		t.Fatal("no GOAWAY")
	}
	// The client honors GOAWAY, so reach beneath it for a stream that does not.
	late, err := client.conn.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := late.Write([]byte{0}); err != nil {
		t.Fatal(err)
	}
	_, err = io.ReadAll(late)
	var reset *quic.StreamError
	if !errors.As(err, &reset) || !reset.Remote || reset.ErrorCode != quic.StreamErrorCode(ErrCodeRequestRejected) {
		t.Fatal("late stream was not rejected:", err)
	}
	if await(ctx, t, "HandleRequestStream", app.handled) {
		t.Fatal("rejected stream reached a handler")
	}
	select {
	case err := <-shutdown:
		t.Fatal("Shutdown returned with a taken-over stream open:", err)
	default:
	}
	held.CancelRead(quic.StreamErrorCode(ErrCodeNoError))
	held.CancelWrite(quic.StreamErrorCode(ErrCodeNoError))
	peer.stream.CancelRead(quic.StreamErrorCode(ErrCodeNoError))
	peer.stream.CancelWrite(quic.StreamErrorCode(ErrCodeNoError))
	// The requests GOAWAY let finish are done; closing is now the client's.
	client.Close()
	if err := await(ctx, t, "Shutdown", shutdown); err != nil {
		t.Fatal(err)
	}
}

func TestRawServerConnAdmission(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server := &Server{MaxConnections: 1}
	app := startRawServer(ctx, t, server, NextProtoH3)
	dialRaw(ctx, t, app.listener.Addr().String(), NextProtoH3, nil)
	conn := await(ctx, t, "connection", app.conns)
	await(ctx, t, "raw connection", app.raw)
	if _, err := server.NewRawServerConn(conn); err == nil {
		t.Fatal("served one connection twice")
	}
	if _, err := (&Server{EnableDatagrams: true}).NewRawServerConn(conn); err == nil {
		t.Fatal("admitted a connection without the datagram support it needs")
	}
	if _, err := (&Server{MaxHeaderBytes: -1}).NewRawServerConn(conn); err == nil {
		t.Fatal("admitted a connection with an invalid configuration")
	}
	select {
	case <-conn.Context().Done():
		t.Fatal("refused admission closed the caller's connection")
	default:
	}
}

// An unknown unidirectional stream type is refused, not left to pin credit.
func TestRawServerConnUnknownUnidirectionalStream(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	app := startRawServer(ctx, t, &Server{}, NextProtoH3)
	conn := dialRaw(ctx, t, app.listener.Addr().String(), NextProtoH3, nil)
	stream, err := conn.OpenUniStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write([]byte{0x21}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-stream.Context().Done():
	case <-ctx.Done():
		t.Fatal("unknown stream was never stopped")
	}
	var stopped *quic.StreamError
	if cause := context.Cause(stream.Context()); !errors.As(cause, &stopped) || !stopped.Remote || stopped.ErrorCode != quic.StreamErrorCode(ErrCodeStreamCreationError) {
		t.Fatal(cause)
	}
	select {
	case <-conn.Context().Done():
		t.Fatal("unknown stream type closed the connection:", context.Cause(conn.Context()))
	default:
	}
}

// rawClient hands a raw client connection its unidirectional streams and
// offers the server's bidirectional streams to the test.
func rawClient(ctx context.Context, t *testing.T, transport *Transport, address string) (*RawClientConn, <-chan *quic.Stream) {
	t.Helper()
	open := make(chan struct{})
	close(open)
	return gatedRawClient(ctx, t, transport, address, open)
}

// gatedRawClient hands HTTP/3 no unidirectional stream until gate is closed, so
// the server's SETTINGS cannot have been processed before then.
func gatedRawClient(ctx context.Context, t *testing.T, transport *Transport, address string, gate <-chan struct{}) (*RawClientConn, <-chan *quic.Stream) {
	t.Helper()
	config := &quic.Config{}
	if transport.QUICConfig != nil {
		config = transport.QUICConfig.Clone()
	}
	config.EnableDatagrams = config.EnableDatagrams || transport.EnableDatagrams
	conn := dialRaw(ctx, t, address, NextProtoH3, config)
	raw := transport.NewRawClientConn(conn)
	bidirectional := make(chan *quic.Stream, 1)
	var loops sync.WaitGroup
	loops.Add(2)
	go func() {
		defer loops.Done()
		for {
			stream, err := conn.AcceptUniStream(ctx)
			if err != nil {
				return
			}
			select {
			case <-gate:
			case <-ctx.Done():
				return
			}
			go raw.HandleUnidirectionalStream(stream)
		}
	}()
	go func() {
		defer loops.Done()
		for {
			stream, err := conn.AcceptStream(ctx)
			if err != nil {
				return
			}
			bidirectional <- stream
		}
	}()
	t.Cleanup(func() { raw.Close(); loops.Wait() })
	return raw, bidirectional
}

func TestRawClientConn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server := &Server{AdditionalSettings: map[uint64]uint64{0xface: 7}, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, r.Host)
	})}
	address := listenTestServer(ctx, t, server)
	gate := make(chan struct{})
	raw, _ := gatedRawClient(ctx, t, &Transport{}, address, gate)
	// With the control stream withheld there are no SETTINGS to have received.
	select {
	case <-raw.ReceivedSettings():
		t.Fatal("SETTINGS received without the control stream")
	default:
	}
	if raw.Settings() != nil {
		t.Fatal("settings before receipt")
	}
	close(gate)
	// The connection is not pinned to an origin: any authority may be requested.
	for _, authority := range []string{address, "other.example"} {
		request, _ := http.NewRequestWithContext(ctx, "GET", "https://"+authority+"/", nil)
		response, err := raw.RoundTrip(request)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil || string(body) != authority {
			t.Fatalf("%q %v", body, err)
		}
	}
	// A response proves the handed-over control stream delivered SETTINGS.
	select {
	case <-raw.ReceivedSettings():
	case <-ctx.Done():
		t.Fatal("no SETTINGS through handed-over streams")
	}
	if settings := raw.Settings(); settings == nil || settings.Other[0xface] != 7 {
		t.Fatal(settings)
	}
	stream, err := raw.OpenRequestStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Raw request streams are not pinned either.
	request, _ := http.NewRequestWithContext(ctx, "GET", "https://other.example/", nil)
	if err := stream.SendRequestHeader(request); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	response, err := stream.ReadResponse()
	if err != nil {
		t.Fatal(err)
	}
	if body, err := io.ReadAll(stream); err != nil || string(body) != "other.example" || response.StatusCode != 200 {
		t.Fatalf("%q %v", body, err)
	}
	if raw.LocalAddr() == nil || raw.RemoteAddr().String() != address {
		t.Fatal(raw.LocalAddr(), raw.RemoteAddr())
	}
	if err := raw.CloseWithError(quic.ApplicationErrorCode(ErrCodeNoError), "done"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-raw.Context().Done():
	case <-ctx.Done():
		t.Fatal("CloseWithError left the connection open")
	}
}

// A raw client's application decides what a server-initiated bidirectional
// stream is: its own to keep, or HTTP/3's to refuse.
func TestRawClientConnServerBidirectionalStream(t *testing.T) {
	for _, mode := range []string{"kept", "refused"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			app := startRawServer(ctx, t, &Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.WriteString(w, "still serving")
			})}, NextProtoH3)
			raw, bidirectional := rawClient(ctx, t, &Transport{}, app.listener.Addr().String())
			serverConn := await(ctx, t, "connection", app.conns)
			opened, err := serverConn.OpenStreamSync(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := opened.Write([]byte("extension")); err != nil {
				t.Fatal(err)
			}
			stream := await(ctx, t, "server stream", bidirectional)
			if mode == "refused" {
				raw.HandleBidirectionalStream(stream)
				select {
				case <-serverConn.Context().Done():
				case <-ctx.Done():
					t.Fatal("refused stream left the connection open")
				}
				var closed *quic.PeerCloseError
				if cause := context.Cause(serverConn.Context()); !errors.As(cause, &closed) || !closed.Application || closed.Code != uint64(ErrCodeStreamCreationError) {
					t.Fatal(cause)
				}
				request, _ := http.NewRequestWithContext(ctx, "GET", app.origin()+"/", nil)
				if _, err := raw.RoundTrip(request); !errors.Is(err, &Error{ErrorCode: ErrCodeStreamCreationError}) {
					t.Fatal(err)
				}
				return
			}
			payload := make([]byte, len("extension"))
			if _, err := io.ReadFull(stream, payload); err != nil || string(payload) != "extension" {
				t.Fatalf("%q %v", payload, err)
			}
			if _, err := stream.Write([]byte("reply")); err != nil {
				t.Fatal(err)
			}
			if _, err := io.ReadFull(opened, payload[:5]); err != nil || string(payload[:5]) != "reply" {
				t.Fatalf("%q %v", payload[:5], err)
			}
			// HTTP/3 carries on beside the stream the application kept.
			request, _ := http.NewRequestWithContext(ctx, "GET", app.origin()+"/", nil)
			response, err := raw.RoundTrip(request)
			if err != nil {
				t.Fatal("kept stream broke HTTP/3:", err)
			}
			body, err := io.ReadAll(response.Body)
			response.Body.Close()
			if err != nil || string(body) != "still serving" {
				t.Fatalf("%q %v", body, err)
			}
			select {
			case <-serverConn.Context().Done():
				t.Fatal("kept stream closed the connection:", context.Cause(serverConn.Context()))
			default:
			}
		})
	}
}

// A client that owns its accept loops still refuses server-initiated
// bidirectional streams itself, and negotiates gzip unless told not to.
func TestTransportNewClientConn(t *testing.T) {
	for _, disable := range []bool{false, true} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		app := startRawServer(ctx, t, &Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Accept-Encoding") != "gzip" {
				io.WriteString(w, "plain")
				return
			}
			w.Header().Set("Content-Encoding", "gzip")
			compressed := gzip.NewWriter(w)
			io.WriteString(compressed, "compressed")
			compressed.Close()
		})}, NextProtoH3)
		conn := dialRaw(ctx, t, app.listener.Addr().String(), NextProtoH3, nil)
		client := (&Transport{DisableCompression: disable}).NewClientConn(conn)
		defer client.Close()
		request, _ := http.NewRequestWithContext(ctx, "GET", app.origin()+"/", nil)
		response, err := client.RoundTrip(request)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		want := "compressed"
		if disable {
			want = "plain"
		}
		if err != nil || string(body) != want || response.Uncompressed == disable || response.Request != request {
			t.Fatalf("%q %v %v", body, err, response.Uncompressed)
		}
		serverConn := await(ctx, t, "connection", app.conns)
		opened, err := serverConn.OpenStreamSync(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := opened.Write([]byte{0}); err != nil {
			t.Fatal(err)
		}
		select {
		case <-serverConn.Context().Done():
		case <-ctx.Done():
			t.Fatal("server-initiated bidirectional stream was not a connection error")
		}
		var closed *quic.PeerCloseError
		if cause := context.Cause(serverConn.Context()); !errors.As(cause, &closed) || !closed.Application || closed.Code != uint64(ErrCodeStreamCreationError) {
			t.Fatal(cause)
		}
	}
}

type recordedLogs struct {
	slog.Handler
	records chan slog.Record
}

func (l recordedLogs) Enabled(context.Context, slog.Level) bool { return true }
func (l recordedLogs) Handle(_ context.Context, record slog.Record) error {
	l.records <- record
	return nil
}
func recordLogs() (*slog.Logger, <-chan slog.Record) {
	records := make(chan slog.Record, 16)
	return slog.New(recordedLogs{Handler: slog.DiscardHandler, records: records}), records
}

// Setup runs in the background, so its failure arrives through the connection.
func TestTransportNewClientConnSetupFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	app := startRawServer(ctx, t, &Server{}, "not-h3")
	conn := dialRaw(ctx, t, app.listener.Addr().String(), "not-h3", nil)
	// Setup fails at once; let the server accept the connection before it does.
	serverConn := await(ctx, t, "connection", app.conns)
	logger, records := recordLogs()
	client := (&Transport{Logger: logger}).NewClientConn(conn)
	request, _ := http.NewRequestWithContext(ctx, "GET", app.origin()+"/", nil)
	if _, err := client.RoundTrip(request); err == nil {
		t.Fatal("request succeeded on a connection that never negotiated h3")
	}
	if _, err := client.OpenRequestStream(ctx); err == nil {
		t.Fatal("stream opened on a connection that never negotiated h3")
	}
	if record := await(ctx, t, "log record", records); record.Level != slog.LevelDebug {
		t.Fatal(record)
	}
	select {
	case <-serverConn.Context().Done():
	case <-ctx.Done():
		t.Fatal("failed setup left the connection open")
	}
	var closed *quic.PeerCloseError
	if cause := context.Cause(serverConn.Context()); !errors.As(cause, &closed) || !closed.Application || closed.Code != uint64(ErrCodeInternalError) {
		t.Fatal(cause)
	}
	if client.Settings() != nil {
		t.Fatal("settings on a failed connection")
	}
	done := make(chan struct{})
	go func() { defer close(done); client.Close() }()
	await(ctx, t, "Close", done)
}

func TestServerIdleTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const idle = 100 * time.Millisecond
	server := &Server{IdleTimeout: idle, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Outlast the idle period: a request in progress is not idleness.
		select {
		case <-time.After(3 * idle):
			io.WriteString(w, "slow")
		case <-r.Context().Done():
		}
	})}
	client, origin := dialTestServer(ctx, t, server, nil)
	request, _ := http.NewRequestWithContext(ctx, "GET", origin+"/", nil)
	response, err := client.RoundTrip(request)
	if err != nil {
		t.Fatal("connection closed under an active request:", err)
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil || string(body) != "slow" {
		t.Fatalf("%q %v", body, err)
	}
	finished := time.Now()
	select {
	case <-client.Context().Done():
	case <-ctx.Done():
		t.Fatal("idle connection stayed open")
	}
	var closed *quic.PeerCloseError
	if cause := context.Cause(client.Context()); !errors.As(cause, &closed) || !closed.Application || closed.Code != uint64(ErrCodeNoError) || closed.Reason != "idle timeout" {
		t.Fatal(cause)
	}
	if elapsed := time.Since(finished); elapsed < idle/2 {
		t.Fatal("connection closed without an idle period:", elapsed)
	}
}

func TestServerLogger(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	logger, records := recordLogs()
	reported := make(chan error, 1)
	server := &Server{Logger: logger, ErrorHandler: func(_ context.Context, _ uint64, err error) { reported <- err }, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("handler failure")
	})}
	client, origin := dialTestServer(ctx, t, server, nil)
	request, _ := http.NewRequestWithContext(ctx, "GET", origin+"/", nil)
	if _, err := client.RoundTrip(request); err == nil {
		t.Fatal("panicking handler produced a response")
	}
	record := await(ctx, t, "log record", records)
	var value any
	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key == "panic" {
			value = attr.Value.Any()
		}
		return true
	})
	if record.Level != slog.LevelError || value != "handler failure" {
		t.Fatal(record.Level, record.Message, value)
	}
	// The logger adds to the error handler; it does not replace it.
	if err := await(ctx, t, "error handler", reported); err == nil {
		t.Fatal("no error reported")
	}
}

// A peer can stall HTTP/3 setup by granting no unidirectional streams while its
// PINGs keep QUIC alive. That is idle time too, and must not hold the
// connection's place in the server forever.
func TestServerIdleTimeoutDuringSetup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const idle = 100 * time.Millisecond
	server := &Server{IdleTimeout: idle}
	address := listenTestServer(ctx, t, server)
	conn := dialRaw(ctx, t, address, NextProtoH3, &quic.Config{MaxIncomingUniStreams: -1, KeepAlivePeriod: idle / 5})
	select {
	case <-conn.Context().Done():
	case <-ctx.Done():
		t.Fatal("connection stalled in setup outlived the idle timeout")
	}
	var closed *quic.PeerCloseError
	if cause := context.Cause(conn.Context()); !errors.As(cause, &closed) || !closed.Application || closed.Code != uint64(ErrCodeNoError) || closed.Reason != "idle timeout" {
		t.Fatal(cause)
	}
	// The stalled connection gave its admission slot back.
	deadline := time.Now().Add(3 * time.Second)
	for {
		server.mu.Lock()
		admitted := len(server.connections)
		server.mu.Unlock()
		if admitted == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("stalled connection kept its admission slot")
		}
		time.Sleep(time.Millisecond)
	}
}

// openDisposable opens unidirectional streams of an unknown type, one after
// the other. HTTP/3 refuses each, and the next needs the credit that returns.
func openDisposable(ctx context.Context, t *testing.T, conn *quic.Conn, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		stream, err := conn.OpenUniStreamSync(ctx)
		if err != nil {
			t.Fatalf("unidirectional stream %d got no credit: %v", i, err)
		}
		if _, err := stream.Write([]byte{0x21}); err != nil {
			t.Fatal(err)
		}
		select {
		case <-stream.Context().Done():
		case <-ctx.Done():
			t.Fatalf("unidirectional stream %d was never refused", i)
		}
	}
}

// With RequireStreamCreditRelease the peer gets a stream back only once its
// owner releases it. HTTP/3 owns the streams it handles, on every accept path.
func TestStreamCreditRelease(t *testing.T) {
	// Two request streams; the three critical streams and one more.
	limited := &quic.Config{MaxIncomingStreams: 2, MaxIncomingUniStreams: 4, RequireStreamCreditRelease: true}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ok") })
	requests := func(ctx context.Context, t *testing.T, client *ClientConn, origin string) {
		t.Helper()
		for i := 0; i < 6; i++ {
			request, _ := http.NewRequestWithContext(ctx, "GET", origin+"/", nil)
			response, err := client.RoundTrip(request)
			if err != nil {
				t.Fatalf("request %d got no stream credit: %v", i, err)
			}
			io.Copy(io.Discard, response.Body)
			response.Body.Close()
		}
	}
	t.Run("served", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		client, origin := dialTestServer(ctx, t, &Server{Handler: handler, QUICConfig: limited}, nil)
		requests(ctx, t, client, origin)
		openDisposable(ctx, t, client.conn, 4)
	})
	t.Run("raw server", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		app := startRawServer(ctx, t, &Server{Handler: handler, QUICConfig: limited}, NextProtoH3)
		client, err := Dial(ctx, app.origin(), &tls.Config{InsecureSkipVerify: true}, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		requests(ctx, t, client, app.origin())
		openDisposable(ctx, t, client.conn, 4)
	})
	t.Run("client", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		app := startRawServer(ctx, t, &Server{Handler: handler}, NextProtoH3)
		client, err := Dial(ctx, app.origin(), &tls.Config{InsecureSkipVerify: true}, limited, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		requests(ctx, t, client, app.origin())
		openDisposable(ctx, t, await(ctx, t, "connection", app.conns), 4)
	})
	t.Run("raw client", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		app := startRawServer(ctx, t, &Server{Handler: handler}, NextProtoH3)
		raw, _ := rawClient(ctx, t, &Transport{QUICConfig: limited}, app.listener.Addr().String())
		requests(ctx, t, raw.ClientConn, app.origin())
		openDisposable(ctx, t, await(ctx, t, "connection", app.conns), 4)
	})
}

// SETTINGS that arrived are published even if the session has ended by the
// time anyone looks: a select with both channels ready may pick either.
func TestForwardSettingsAfterSessionEnded(t *testing.T) {
	closed := make(chan struct{})
	close(closed)
	for i := 0; i < 1000; i++ {
		published := make(chan struct{})
		forwardSettings(closed, closed, published)
		select {
		case <-published:
		default:
			t.Fatal("received SETTINGS lost to the session's end")
		}
	}
	published := make(chan struct{})
	forwardSettings(nil, closed, published)
	select {
	case <-published:
		t.Fatal("published SETTINGS that never arrived")
	default:
	}
}
