package http3conn

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	h3 "github.com/getlantern/photon/internal/http3wire"
	"github.com/getlantern/photon/internal/qpack"
)

// rawTestStream ends its send context on Close, as a QUIC stream does.
type rawTestStream struct{ *exchangeStream }

func (s *rawTestStream) Close() error {
	s.sendCancel(context.Canceled)
	return s.exchangeStream.Close()
}

func (s *rawTestStream) codes() (reads, writes []quic.StreamErrorCode) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append(reads, s.readCodes...), append(writes, s.writeCodes...)
}

func rawFixture(t *testing.T, source io.Reader) (*Session, *rawTestStream, ClientStreamConfig, *atomic.Int32) {
	t.Helper()
	stream, config, done := exchangeFixture(t, source)
	session := managedExchangeSession(t, config)
	return session, &rawTestStream{stream}, ClientStreamConfig{MaxEncodedHeaders: 4096, MaxInformational: 4, Done: func() { done.Add(1) }}, done
}

func awaitRelease(t *testing.T, session *Session, done *atomic.Int32) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for done.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("raw stream never released its state")
		}
		time.Sleep(time.Millisecond)
	}
	lease, err := session.BeginRequest(4)
	if err != nil {
		t.Fatal("finished stream kept its lease:", err)
	}
	lease.Finish()
}

func TestClientStreamLifecycle(t *testing.T) {
	wire := readerHeaders(t, responseFields("103")...)
	wire = append(wire, readerHeaders(t, responseFields("200")...)...)
	wire = append(wire, readerFrame(t, h3.Data, []byte("abc"))...)
	wire = append(wire, readerFrame(t, 0x21, []byte("unknown"))...)
	wire = append(wire, readerFrame(t, h3.Data, []byte("def"))...)
	session, transport, config, done := rawFixture(t, bytes.NewReader(wire))
	interim := 0
	config.Information = func(status int, _ http.Header) error { interim = status; return nil }
	stream, err := session.openRequestStream(context.Background(), transport, nil, config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write([]byte("early")); err == nil {
		t.Fatal("wrote before the request header")
	}
	if err := stream.TryWriteAll([]byte("early")); err == nil {
		t.Fatal("queued before the request header")
	}
	if _, err := stream.Read(make([]byte, 1)); err == nil {
		t.Fatal("read before the response")
	}
	if _, err := stream.ReadResponse(); err == nil {
		t.Fatal("read a response before the request")
	}
	withBody, _ := http.NewRequest("POST", "https://example.com/", bytes.NewReader([]byte("x")))
	if err := stream.SendRequestHeader(withBody); err == nil {
		t.Fatal("accepted a request with a body")
	}
	request, _ := http.NewRequest("POST", "https://example.com/raw", nil)
	if err := stream.SendRequestHeader(request); err != nil {
		t.Fatal(err)
	}
	if err := stream.SendRequestHeader(request); err == nil {
		t.Fatal("sent the request header twice")
	}
	if n, err := stream.Write([]byte("ping")); n != 4 || err != nil {
		t.Fatal(n, err)
	}
	sent := requestReader(t, transport.Bytes(), false)
	if event, err := sent.Next(context.Background()); err != nil || event.Control.Method != "POST" || event.Control.Path != "/raw" {
		t.Fatal(event, err)
	}
	if event, err := sent.Next(context.Background()); err != nil || event.Header != (h3.Header{Type: h3.Data, Length: 4}) {
		t.Fatal(event, err)
	}
	response, err := stream.ReadResponse()
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != 200 || response.Request != nil || interim != 103 {
		t.Fatal(response.StatusCode, response.Request, interim)
	}
	if _, err := stream.ReadResponse(); err == nil {
		t.Fatal("read the response twice")
	}
	body, err := io.ReadAll(stream)
	if err != nil || string(body) != "abcdef" {
		t.Fatalf("%q %v", body, err)
	}
	// Reading to EOF finishes only one direction.
	if _, err := session.BeginRequest(4); !errors.Is(err, ErrRequestLimit) {
		t.Fatal("stream released with its send direction open:", err, done.Load())
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	awaitRelease(t, session, done)
	if reads, writes := transport.codes(); done.Load() != 1 || transport.closes != 1 || len(reads) != 0 || len(writes) != 0 {
		t.Fatal(done.Load(), transport.closes, reads, writes)
	}
}

func TestClientStreamDeclaredLength(t *testing.T) {
	session, transport, config, _ := rawFixture(t, bytes.NewReader(nil))
	stream, err := session.openRequestStream(context.Background(), transport, nil, config)
	if err != nil {
		t.Fatal(err)
	}
	request, _ := http.NewRequest("POST", "https://example.com/", nil)
	request.ContentLength = 3
	if err := stream.SendRequestHeader(request); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write([]byte("ab")); err != nil {
		t.Fatal(err)
	}
	var message *MessageError
	if err := stream.Close(); !errors.As(err, &message) || transport.closes != 0 {
		t.Fatal("sent FIN short of the declared length:", err)
	}
	stream.CancelRead(0x10c)
	stream.CancelWrite(0x10c)
}

func TestClientStreamCancelReadWakesBlockedRead(t *testing.T) {
	input, output := io.Pipe()
	defer output.Close()
	waiting := &enteredReader{Reader: input, entered: make(chan struct{})}
	session, transport, config, done := rawFixture(t, io.MultiReader(bytes.NewReader(readerHeaders(t, responseFields("200")...)), waiting))
	transport.interrupt = func() { output.CloseWithError(&quic.StreamError{ErrorCode: 0x77}) }
	stream, err := session.openRequestStream(context.Background(), transport, nil, config)
	if err != nil {
		t.Fatal(err)
	}
	request, _ := http.NewRequest("GET", "https://example.com/", nil)
	if err := stream.SendRequestHeader(request); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.ReadResponse(); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { _, err := stream.Read(make([]byte, 1)); result <- err }()
	select {
	case <-waiting.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("read never reached the stream")
	}
	stream.CancelRead(0x77)
	select {
	case err := <-result:
		var reset *quic.StreamError
		if !errors.As(err, &reset) || reset.ErrorCode != 0x77 {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("CancelRead left Read blocked")
	}
	// Canceling one direction leaves the other, and the stream's state, alone.
	if _, err := stream.Write([]byte("still open")); err != nil {
		t.Fatal(err)
	}
	if _, writes := transport.codes(); len(writes) != 0 || done.Load() != 0 {
		t.Fatal(writes, done.Load())
	}
	stream.CancelWrite(0x78)
	awaitRelease(t, session, done)
	if reads, writes := transport.codes(); len(reads) != 1 || reads[0] != 0x77 || len(writes) != 1 || writes[0] != 0x78 {
		t.Fatal(reads, writes)
	}
}

func TestClientStreamEndOfLeaseResetsBlockedRead(t *testing.T) {
	for _, mode := range []string{"goaway", "owner"} {
		t.Run(mode, func(t *testing.T) {
			input, output := io.Pipe()
			defer output.Close()
			waiting := &enteredReader{Reader: input, entered: make(chan struct{})}
			session, transport, config, done := rawFixture(t, io.MultiReader(bytes.NewReader(readerHeaders(t, responseFields("200")...)), waiting))
			transport.interrupt = func() { output.CloseWithError(&quic.StreamError{ErrorCode: 0x10c}) }
			owner, abandon := context.WithCancelCause(context.Background())
			defer abandon(nil)
			stream, err := session.openRequestStream(owner, transport, nil, config)
			if err != nil {
				t.Fatal(err)
			}
			request, _ := http.NewRequest("GET", "https://example.com/", nil)
			if err := stream.SendRequestHeader(request); err != nil {
				t.Fatal(err)
			}
			if _, err := stream.ReadResponse(); err != nil {
				t.Fatal(err)
			}
			result := make(chan error, 1)
			go func() { _, err := stream.Read(make([]byte, 1)); result <- err }()
			select {
			case <-waiting.entered:
			case <-time.After(3 * time.Second):
				t.Fatal("read never reached the stream")
			}
			want := ErrRequestRejected
			if mode == "goaway" {
				session.receiveGoaway(PeerGoaway{ID: 0, RequestIDs: true})
			} else {
				want = errors.New("owner closed")
				abandon(want)
			}
			select {
			case err := <-result:
				if !errors.Is(err, want) {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("ended lease left Read blocked")
			}
			awaitDone(t, done)
			if reads, writes := transport.codes(); len(reads) == 0 || reads[0] != 0x10c || len(writes) != 1 || writes[0] != 0x10c {
				t.Fatal(reads, writes)
			}
		})
	}
}

func TestClientStreamRefusedAfterGoaway(t *testing.T) {
	session, transport, config, done := rawFixture(t, bytes.NewReader(nil))
	session.receiveGoaway(PeerGoaway{ID: 0, RequestIDs: true})
	if _, err := session.openRequestStream(context.Background(), transport, nil, config); !errors.Is(err, ErrPeerDraining) {
		t.Fatal(err)
	}
	if reads, writes := transport.codes(); done.Load() != 1 || len(reads) != 1 || reads[0] != 0x10b || len(writes) != 1 || writes[0] != 0x10b {
		t.Fatal(done.Load(), reads, writes)
	}
}

func TestClientStreamHeaderWaitsForSettings(t *testing.T) {
	session, transport, config, _ := rawFixture(t, bytes.NewReader(nil))
	encoder := session.encoder.Swap(nil)
	session.ready = make(chan struct{})
	stream, err := session.openRequestStream(context.Background(), transport, nil, config)
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("caller gave up")
	cancel(cause)
	request, _ := http.NewRequestWithContext(canceled, "GET", "https://example.com/", nil)
	if err := stream.SendRequestHeader(request); !errors.Is(err, context.Canceled) && !errors.Is(err, cause) {
		t.Fatal(err)
	}
	if transport.Len() != 0 {
		t.Fatal("request bytes sent without peer SETTINGS")
	}
	// Nothing was sent, so the header can still be sent once SETTINGS arrive.
	session.encoder.Store(encoder)
	close(session.ready)
	request, _ = http.NewRequest("GET", "https://example.com/", nil)
	if err := stream.SendRequestHeader(request); err != nil {
		t.Fatal(err)
	}
	if transport.Len() == 0 {
		t.Fatal("no request header sent")
	}
	stream.CancelRead(0x10c)
	stream.CancelWrite(0x10c)
}

func TestClientStreamMalformedResponseScope(t *testing.T) {
	for _, mode := range []string{"connection", "request"} {
		t.Run(mode, func(t *testing.T) {
			// DATA before HEADERS is a connection error; an invalid status only
			// spoils the request.
			wire := readerFrame(t, h3.Data, []byte("x"))
			if mode == "request" {
				wire = readerHeaders(t, fs(":status", "200"), fs("content-length", "1"), fs("content-length", "2"))
			}
			session, transport, config, done := rawFixture(t, bytes.NewReader(wire))
			stream, err := session.openRequestStream(context.Background(), transport, nil, config)
			if err != nil {
				t.Fatal(err)
			}
			request, _ := http.NewRequest("GET", "https://example.com/", nil)
			if err := stream.SendRequestHeader(request); err != nil {
				t.Fatal(err)
			}
			_, err = stream.ReadResponse()
			var protocol *h3.Error
			var message *MessageError
			aborts := session.transport.(*dispatchTransport)
			awaitDone(t, done)
			if mode == "connection" {
				if !errors.As(err, &protocol) || protocol.Code != h3.FrameUnexpected {
					t.Fatal(err)
				}
				aborts.check(t, h3.FrameUnexpected)
				return
			}
			if !errors.As(err, &message) {
				t.Fatal(err)
			}
			aborts.mu.Lock()
			count := aborts.aborts
			aborts.mu.Unlock()
			if reads, writes := transport.codes(); count != 0 || len(reads) == 0 || reads[0] != 0x10e || len(writes) == 0 || writes[0] != 0x10e {
				t.Fatal("malformed response did not reset just the request:", count, reads, writes)
			}
		})
	}
}

func awaitDone(t *testing.T, done *atomic.Int32) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for done.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("raw stream never released its state")
		}
		time.Sleep(time.Millisecond)
	}
}

// deadlineSink accepts limit bytes, then reports an expired write deadline once.
type deadlineSink struct {
	bytes.Buffer
	limit  int
	closes int
}

func (s *deadlineSink) Write(p []byte) (int, error) {
	if s.limit >= 0 && len(p) > s.limit {
		n := s.limit
		s.limit = -1
		s.Buffer.Write(p[:n])
		return n, os.ErrDeadlineExceeded
	}
	if s.limit >= 0 {
		s.limit -= len(p)
	}
	return s.Buffer.Write(p)
}
func (s *deadlineSink) Close() error { s.closes++; return nil }

func TestMessageWriterResumesDataFrameAfterDeadline(t *testing.T) {
	// Byte offsets inside the DATA frame "\x00\x06abcdef": its header, the
	// boundary, and its payload.
	for _, offset := range []int{0, 1, 2, 5} {
		sink := &deadlineSink{limit: -1}
		encoder, err := NewFieldEncoder(SettingsSnapshot{MaxFieldSectionSize: 4096}, encoderLimits(), io.Discard)
		if err != nil {
			t.Fatal(err)
		}
		writer, err := NewMessageWriter(sink, encoder, 0, MessageConfig{Kind: ResponseFields, RequestMethod: "GET"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := writer.Headers(responseFields("200")); err != nil {
			t.Fatal(err)
		}
		headers := sink.Len()
		sink.limit = offset
		n, err := writer.Write([]byte("abcdef"))
		if want := max(offset-2, 0); n != want || !h3.IsDeadline(err) {
			t.Fatal(offset, n, err)
		}
		if err := writer.Close(); !errors.Is(err, errUnfinishedData) || sink.closes != 0 {
			t.Fatal("closed inside a DATA frame:", offset, err)
		}
		if err := writer.Headers([]qpack.DecodedField{}); !errors.Is(err, errUnfinishedData) {
			t.Fatal("sent trailers inside a DATA frame:", offset, err)
		}
		// The retry carries the frame's remaining payload and more.
		rest := []byte("abcdef")[n:]
		if m, err := writer.Write(append(rest, "gh"...)); m != len(rest)+2 || err != nil {
			t.Fatal(offset, m, err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(offset, err)
		}
		want := append(readerFrame(t, h3.Data, []byte("abcdef")), readerFrame(t, h3.Data, []byte("gh"))...)
		if got := sink.Bytes()[headers:]; !bytes.Equal(got, want) {
			t.Fatalf("offset %d: wire %x, want %x", offset, got, want)
		}
	}
}

func TestMessageWriterHeadersDeadlineIsTerminal(t *testing.T) {
	sink := &deadlineSink{limit: 1}
	writer := responseWriterOn(t, sink, "GET")
	if err := writer.Headers(responseFields("200")); !h3.IsDeadline(err) {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("x")); !h3.IsDeadline(err) {
		t.Fatal("writer resumed after a partial HEADERS frame:", err)
	}
}

// tryStream admits TryWriteAll calls only while open.
type tryStream struct {
	deadlineSink
	open  bool
	tries [][]byte
}

func (s *tryStream) TryWriteAll(p []byte) error {
	if !s.open {
		return quic.ErrWouldBlock
	}
	s.tries = append(s.tries, bytes.Clone(p))
	s.Buffer.Write(p)
	return nil
}

func TestMessageWriterTryWrite(t *testing.T) {
	stream := &tryStream{deadlineSink: deadlineSink{limit: -1}}
	writer := responseWriterOn(t, stream, "GET")
	if err := writer.Headers(append(responseFields("200"), fs("content-length", "5"))); err != nil {
		t.Fatal(err)
	}
	headers := stream.Len()
	// A refused frame must not count against the declared length.
	if err := writer.TryWrite([]byte("abc")); !errors.Is(err, quic.ErrWouldBlock) {
		t.Fatal(err)
	}
	if stream.Len() != headers {
		t.Fatal("refused frame reached the stream")
	}
	// Interrupt a blocking write, then finish its frame and add one atomically.
	stream.limit = 3
	if n, err := writer.Write([]byte("abc")); n != 1 || !h3.IsDeadline(err) {
		t.Fatal(n, err)
	}
	if err := writer.TryWrite([]byte("bcde")); !errors.Is(err, quic.ErrWouldBlock) {
		t.Fatal(err)
	}
	stream.open = true
	if err := writer.TryWrite([]byte("bcde")); err != nil {
		t.Fatal(err)
	}
	if len(stream.tries) != 1 || !bytes.Equal(stream.tries[0], append([]byte("bc"), readerFrame(t, h3.Data, []byte("de"))...)) {
		t.Fatalf("%x", stream.tries)
	}
	if err := writer.Close(); err != nil {
		t.Fatal("length accounting drifted:", err)
	}
	want := append(readerFrame(t, h3.Data, []byte("abc")), readerFrame(t, h3.Data, []byte("de"))...)
	if got := stream.Bytes()[headers:]; !bytes.Equal(got, want) {
		t.Fatalf("wire %x, want %x", got, want)
	}
}

func responseWriterOn(t *testing.T, stream io.WriteCloser, method string) *MessageWriter {
	t.Helper()
	encoder, err := NewFieldEncoder(SettingsSnapshot{MaxFieldSectionSize: 4096}, encoderLimits(), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := NewMessageWriter(stream, encoder, 0, MessageConfig{Kind: ResponseFields, RequestMethod: method}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return writer
}

// interruptedReader reports an expired read deadline before every byte.
type interruptedReader struct {
	data    []byte
	expired bool
}

func (r *interruptedReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	if r.expired = !r.expired; r.expired {
		return 0, os.ErrDeadlineExceeded
	}
	p[0], r.data = r.data[0], r.data[1:]
	return 1, nil
}

func TestMessageBodyResumesAfterDeadline(t *testing.T) {
	wire := readerHeaders(t, append(responseFields("200"), fs("trailer", "x-end"))...)
	wire = append(wire, readerFrame(t, h3.Data, []byte("ab"))...)
	wire = append(wire, readerFrame(t, 0x21, []byte("skipped"))...)
	wire = append(wire, readerFrame(t, h3.Data, []byte("cd"))...)
	wire = append(wire, readerHeaders(t, fs("x-end", "done"))...)
	reader := responseReader(t, nil)
	reader.frames = h3.NewReader(&interruptedReader{data: wire})
	retry := func(read func() error) {
		t.Helper()
		for expired := 0; ; expired++ {
			err := read()
			if err == nil {
				if expired == 0 {
					t.Fatal("never interrupted")
				}
				return
			}
			if !h3.IsDeadline(err) {
				t.Fatal(err)
			}
		}
	}
	// The HEADERS block is interrupted at every byte, as is everything after.
	var event MessageEvent
	retry(func() (err error) { event, err = reader.Next(context.Background()); return err })
	if event.Control.Status != 200 {
		t.Fatal(event)
	}
	var trailers string
	body, err := NewMessageBody(context.Background(), reader, BodyConfig{Close: func(bool) error { return nil }, Trailers: func(fields []qpack.DecodedField) error {
		trailers = fields[0].Value
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	var content bytes.Buffer
	for {
		buffer := make([]byte, 8)
		n, err := body.Read(buffer)
		content.Write(buffer[:n])
		if err == io.EOF {
			break
		}
		if err != nil && !h3.IsDeadline(err) {
			t.Fatal(err)
		}
	}
	if content.String() != "abcd" || trailers != "done" {
		t.Fatalf("%q %q", content.String(), trailers)
	}
}

func TestIdleTimer(t *testing.T) {
	expirations := 0
	timer := newIdleTimer(time.Hour, func() { expirations++ })
	defer timer.timer.Stop()
	if !timer.begin() {
		t.Fatal("fresh timer refused a stream")
	}
	// A callback that fired before the stream arrived finds the stream active,
	// however long ago the idle period ran out.
	timer.mu.Lock()
	timer.deadline = time.Now()
	timer.mu.Unlock()
	timer.fire(func() { expirations++ })
	if expirations != 0 || timer.hasExpired() {
		t.Fatal("expired under an active stream")
	}
	timer.end()
	// The same callback delivered after the stream ended must not cut the new
	// idle period short.
	timer.fire(func() { expirations++ })
	if expirations != 0 || timer.hasExpired() || !timer.begin() {
		t.Fatal("stale callback expired the connection", expirations)
	}
	timer.end()
	timer.mu.Lock()
	timer.deadline = time.Now()
	timer.mu.Unlock()
	timer.fire(func() { expirations++ })
	if expirations != 1 || !timer.hasExpired() || timer.begin() {
		t.Fatal("idle period did not expire the connection", expirations)
	}
	var none *idleTimer
	if !none.begin() || none.hasExpired() {
		t.Fatal("absent timer expired")
	}
	none.end()
}

func TestServeRequestsIdleTimeout(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	config := serveConfig(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
	}))
	session, transport := requestLoopSession(t, config)
	acceptor := &requestAcceptor{streams: make(chan ServerRequestStream, 1)}
	acceptor.streams <- requestStream(t, 0)
	const idle = 30 * time.Millisecond
	done := make(chan error, 1)
	go func() {
		done <- session.ServeRequests(context.Background(), acceptor, ServeRequestsConfig{MaxActive: 1, IdleTimeout: idle, Request: config})
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("handler missing")
	}
	// An active request holds the connection open well past the idle period.
	select {
	case err := <-done:
		t.Fatal("connection closed under an active request:", err)
	case <-time.After(5 * idle):
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("idle connection stayed open")
	}
	transport.check(t, 0x100)
}

func TestRequestPrioritiesUnordered(t *testing.T) {
	ordered, _ := NewRequestPriorities(4)
	if err := ordered.register(requestStream(t, 8)); err != nil {
		t.Fatal(err)
	}
	if err := ordered.register(requestStream(t, 4)); err == nil {
		t.Fatal("ordered registration went backwards")
	}
	unordered, _ := NewRequestPriorities(4)
	unordered.Unordered = true
	for _, id := range []quic.StreamID{8, 4, 0} {
		if err := unordered.register(requestStream(t, id)); err != nil {
			t.Fatal(id, err)
		}
	}
	if err := unordered.register(requestStream(t, 4)); err == nil {
		t.Fatal("registered one stream twice")
	}
	if len(unordered.active) != 3 || unordered.nextID != 12 {
		t.Fatal(len(unordered.active), unordered.nextID)
	}
}

func TestTakeOverNeedsQUICStream(t *testing.T) {
	stream := &serveStream{source: bytes.NewReader(readerHeaders(t, requestFields("/")...))}
	config := serveConfig(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := w.(*HTTPResponseWriter).TakeOver(); err == nil {
			t.Error("took over a stream that is not a QUIC stream")
		}
		// The refusal leaves an ordinary response possible.
		w.WriteHeader(204)
	}))
	if err := ServeRequest(context.Background(), stream, config); err != nil {
		t.Fatal(err)
	}
	if stream.closes != 1 {
		t.Fatal("response not finished", stream.closes)
	}
}

// recordedControl stands in for the QUIC stream beneath a raw stream.
type recordedControl struct {
	mu                    sync.Mutex
	urgency               int8
	incremental           bool
	read, write, deadline time.Time
}

func (c *recordedControl) SetDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deadline = t
	return nil
}
func (c *recordedControl) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.read = t
	return nil
}
func (c *recordedControl) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.write = t
	return nil
}
func (c *recordedControl) SetPriority(urgency int8, incremental bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.urgency, c.incremental = urgency, incremental
}

func TestRawStreamForwardsStreamControls(t *testing.T) {
	session, transport, config, _ := rawFixture(t, bytes.NewReader(nil))
	stream, err := session.openRequestStream(context.Background(), transport, nil, config)
	if err != nil {
		t.Fatal(err)
	}
	control := &recordedControl{urgency: 3, incremental: true}
	stream.control = control
	stream.SetPriority(1, false)
	read, write, both := time.Unix(1, 0), time.Unix(2, 0), time.Unix(3, 0)
	if stream.SetReadDeadline(read) != nil || stream.SetWriteDeadline(write) != nil || stream.SetDeadline(both) != nil {
		t.Fatal("deadline setters failed")
	}
	if control.urgency != 1 || control.incremental || !control.read.Equal(read) || !control.write.Equal(write) || !control.deadline.Equal(both) {
		t.Fatalf("%+v", control)
	}
	stream.CancelRead(0x10c)
	stream.CancelWrite(0x10c)
}

// blockedFixture is a raw stream whose decoder has a dynamic table, so a
// section can wait for an insertion that the test controls.
func blockedFixture(t *testing.T, source io.Reader, feedback io.Writer) (*Session, *FieldDecoder, *ClientStream, *rawTestStream, *atomic.Int32) {
	t.Helper()
	stream, config, done := exchangeFixture(t, source)
	decoder, err := NewFieldDecoder(decoderLimits(), feedback)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { decoder.Close(context.Canceled) })
	if err := decoderUpdate(t, decoder, qpack.EncoderInstruction{Type: qpack.SetDynamicCapacity, Index: 128}); err != nil {
		t.Fatal(err)
	}
	config.Decoder = decoder
	session := managedExchangeSession(t, config)
	transport := &rawTestStream{stream}
	raw, err := session.openRequestStream(context.Background(), transport, nil, ClientStreamConfig{MaxEncodedHeaders: 4096, MaxInformational: 4, Done: func() { done.Add(1) }})
	if err != nil {
		t.Fatal(err)
	}
	raw.control = &recordedControl{}
	request, _ := http.NewRequest("GET", "https://example.com/", nil)
	if err := raw.SendRequestHeader(request); err != nil {
		t.Fatal(err)
	}
	return session, decoder, raw, transport, done
}

func awaitBlockedSection(t *testing.T, decoder *FieldDecoder) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		decoder.mu.Lock()
		blocked := len(decoder.pending) == 1
		decoder.mu.Unlock()
		if blocked {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("section never waited for the dynamic table")
		}
		runtime.Gosched()
	}
}

// A section waiting for the dynamic table reads nothing from the request
// stream, so the receive controls have to reach the wait itself.
func TestClientStreamReceiveControlsReachQPACKWait(t *testing.T) {
	// Required Insert Count 1, Base 1, dynamic entry 0: the whole section.
	blocked := readerFrame(t, h3.Headers, []byte{2, 0, 0x80})
	for _, section := range []string{"headers", "trailers"} {
		for _, control := range []string{"deadline", "cancel"} {
			t.Run(section+"/"+control, func(t *testing.T) {
				wire, insert := blocked, qpack.EncoderInstruction{Type: qpack.InsertLiteralName, Name: ":status", Value: "200"}
				if section == "trailers" {
					wire = append(readerHeaders(t, responseFields("200")...), readerFrame(t, h3.Data, []byte("body"))...)
					wire = append(wire, blocked...)
					insert = qpack.EncoderInstruction{Type: qpack.InsertLiteralName, Name: "x-end", Value: "done"}
				}
				_, decoder, stream, transport, _ := blockedFixture(t, bytes.NewReader(wire), io.Discard)
				defer func() { stream.CancelRead(0x10c); stream.CancelWrite(0x10c) }()
				var response *http.Response
				// receive is the call that waits: ReadResponse for the response
				// header, Read for trailers.
				receive := func() (err error) {
					response, err = stream.ReadResponse()
					return err
				}
				if section == "trailers" {
					if err := receive(); err != nil {
						t.Fatal(err)
					}
					body := make([]byte, 4)
					if _, err := io.ReadFull(stream, body); err != nil || string(body) != "body" {
						t.Fatalf("%q %v", body, err)
					}
					receive = func() error {
						_, err := stream.Read(make([]byte, 1))
						return err
					}
				}
				result := make(chan error, 1)
				go func() { result <- receive() }()
				awaitBlockedSection(t, decoder)
				if control == "cancel" {
					stream.CancelRead(0x77)
				} else {
					stream.SetReadDeadline(time.Now().Add(-time.Second))
				}
				var err error
				select {
				case err = <-result:
				case <-time.After(3 * time.Second):
					t.Fatal("receive control left the QPACK wait blocked")
				}
				if control == "cancel" {
					var reset *quic.StreamError
					if !errors.As(err, &reset) || reset.Remote || reset.ErrorCode != 0x77 {
						t.Fatal(err)
					}
					if reads, _ := transport.codes(); len(reads) == 0 || reads[0] != 0x77 {
						t.Fatal(reads)
					}
					if err := receive(); err == nil {
						t.Fatal("received after CancelRead")
					}
					return
				}
				if !errors.Is(err, os.ErrDeadlineExceeded) {
					t.Fatal(err)
				}
				// The deadline cost nothing: the section is still waiting, and
				// the repeated call gets it once the insertion arrives.
				awaitBlockedSection(t, decoder)
				if err := receive(); !errors.Is(err, os.ErrDeadlineExceeded) {
					t.Fatal("expired deadline no longer applies:", err)
				}
				stream.SetReadDeadline(time.Time{})
				if err := decoderUpdate(t, decoder, insert); err != nil {
					t.Fatal(err)
				}
				err = receive()
				if section == "headers" {
					if err != nil || response.StatusCode != 200 {
						t.Fatal(response, err)
					}
				} else if err != io.EOF || response.Trailer.Get("X-End") != "done" {
					t.Fatal(err, response.Trailer)
				}
			})
		}
	}
}

// blockedWriter is a decoder stream out of flow-control credit.
type blockedWriter struct {
	release chan struct{}
	writes  atomic.Int32
}

func (w *blockedWriter) Write(p []byte) (int, error) {
	w.writes.Add(1)
	<-w.release
	return len(p), nil
}

// A response read through its end has no QPACK work left to cancel, so its
// release must not depend on the decoder stream.
func TestClientStreamEOFSkipsQPACKCancellation(t *testing.T) {
	wire := append(readerHeaders(t, responseFields("200")...), readerFrame(t, h3.Data, []byte("body"))...)
	feedback := &blockedWriter{release: make(chan struct{})}
	defer close(feedback.release)
	session, _, stream, _, done := blockedFixture(t, bytes.NewReader(wire), feedback)
	if _, err := stream.ReadResponse(); err != nil {
		t.Fatal(err)
	}
	if body, err := io.ReadAll(stream); err != nil || string(body) != "body" {
		t.Fatalf("%q %v", body, err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	awaitRelease(t, session, done)
	if writes := feedback.writes.Load(); writes != 0 {
		t.Fatal("complete response wrote QPACK cancellation feedback:", writes)
	}
}

// stoppedStream fails Close the way a QUIC stream does after STOP_SENDING.
type stoppedStream struct {
	*rawTestStream
	code quic.StreamErrorCode
}

func (s *stoppedStream) Close() error {
	s.sendCancel(context.Canceled)
	return &quic.StreamError{ErrorCode: s.code, Remote: true}
}

// A server may stop a request's send direction with H3_NO_ERROR once it has
// responded. Close then has nothing left to do, which is not a failure.
func TestRawStreamCloseAfterPeerStopped(t *testing.T) {
	for code, stopped := range map[quic.StreamErrorCode]bool{0x100: true, 0x10c: false} {
		session, transport, config, _ := rawFixture(t, bytes.NewReader(nil))
		stream, err := session.openRequestStream(context.Background(), &stoppedStream{rawTestStream: transport, code: code}, nil, config)
		if err != nil {
			t.Fatal(err)
		}
		request, _ := http.NewRequest("GET", "https://example.com/", nil)
		if err := stream.SendRequestHeader(request); err != nil {
			t.Fatal(err)
		}
		if err := stream.Close(); (err == nil) != stopped {
			t.Fatalf("code %#x: %v", code, err)
		}
		stream.CancelRead(0x10c)
	}
}

// gatedWriter is a decoder stream that runs out of flow-control credit once
// armed: the write after that blocks until released.
type gatedWriter struct {
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newGatedWriter() *gatedWriter {
	return &gatedWriter{entered: make(chan struct{}), release: make(chan struct{})}
}
func (w *gatedWriter) Write(p []byte) (int, error) {
	if w.armed.Load() {
		w.once.Do(func() { close(w.entered) })
		<-w.release
	}
	return len(p), nil
}

// returnsPromptly runs a call that must not wait for the peer.
func returnsPromptly(t *testing.T, what string, call func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { defer close(done); call() }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal(what, "blocked behind QPACK feedback")
	}
}

// A section that decodes at once still owes the peer an acknowledgment, on a
// decoder stream whose credit the peer controls. Neither the read nor the
// receive controls may wait for that credit.
func TestClientStreamReadNotHeldByFeedbackCredit(t *testing.T) {
	dynamic := readerFrame(t, h3.Headers, []byte{2, 0, 0x80})
	for _, section := range []string{"headers", "trailers"} {
		t.Run(section, func(t *testing.T) {
			wire, insert := dynamic, qpack.EncoderInstruction{Type: qpack.InsertLiteralName, Name: ":status", Value: "200"}
			if section == "trailers" {
				wire = append(readerHeaders(t, responseFields("200")...), dynamic...)
				insert = qpack.EncoderInstruction{Type: qpack.InsertLiteralName, Name: "x-end", Value: "done"}
			}
			feedback := newGatedWriter()
			defer close(feedback.release)
			session, decoder, stream, _, done := blockedFixture(t, bytes.NewReader(wire), feedback)
			if err := decoderUpdate(t, decoder, insert); err != nil {
				t.Fatal(err)
			}
			decoder.Flushed()
			feedback.armed.Store(true)
			var response *http.Response
			var err error
			returnsPromptly(t, "ReadResponse", func() { response, err = stream.ReadResponse() })
			if err != nil || response.StatusCode != 200 {
				t.Fatal(response, err)
			}
			if section == "trailers" {
				returnsPromptly(t, "Read", func() { _, err = stream.Read(make([]byte, 1)) })
				if err != io.EOF || response.Trailer.Get("X-End") != "done" {
					t.Fatal(err, response.Trailer)
				}
			}
			// The acknowledgment is stuck, and stays stuck throughout.
			select {
			case <-feedback.entered:
			case <-time.After(3 * time.Second):
				t.Fatal("section was never acknowledged")
			}
			returnsPromptly(t, "SetReadDeadline", func() { stream.SetReadDeadline(time.Now().Add(-time.Second)) })
			returnsPromptly(t, "Body.Close", func() { response.Body.Close() })
			returnsPromptly(t, "CancelRead", func() { stream.CancelRead(0x77) })
			returnsPromptly(t, "CancelWrite", func() { stream.CancelWrite(0x77) })
			awaitRelease(t, session, done)
		})
	}
}

// Round 3's scenario: ReadResponse timed out with its HEADERS section waiting
// for the dynamic table, sending is finished, and CancelRead starts the
// stream's cleanup while the decoder stream has no credit. A ReadResponse
// retried at that moment must not wait for the peer.
func TestClientStreamRetryWhileCleanupLacksFeedbackCredit(t *testing.T) {
	feedback := newGatedWriter()
	defer close(feedback.release)
	feedback.armed.Store(true)
	wire := readerFrame(t, h3.Headers, []byte{2, 0, 0x80})
	session, decoder, stream, _, done := blockedFixture(t, bytes.NewReader(wire), feedback)
	result := make(chan error, 1)
	go func() { _, err := stream.ReadResponse(); result <- err }()
	awaitBlockedSection(t, decoder)
	stream.SetReadDeadline(time.Now().Add(-time.Second))
	select {
	case err := <-result:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("read deadline left ReadResponse blocked")
	}
	stream.SetReadDeadline(time.Time{})
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	returnsPromptly(t, "CancelRead", func() { stream.CancelRead(0x77) })
	// The cancellation is on its way to a decoder stream that will not take it.
	select {
	case <-feedback.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("no QPACK cancellation for the abandoned section")
	}
	go func() { _, err := stream.ReadResponse(); result <- err }()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("response read after CancelRead")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("retried ReadResponse waits for decoder-stream credit")
	}
	// Nor does the stream's release.
	awaitRelease(t, session, done)
}

// An abandoned request gives its slot back whether or not the peer lets the
// QPACK cancellation through.
func TestClientStreamAbandonedReleasesWithoutFeedbackCredit(t *testing.T) {
	feedback := newGatedWriter()
	defer close(feedback.release)
	feedback.armed.Store(true)
	session, _, stream, _, done := blockedFixture(t, bytes.NewReader(nil), feedback)
	stream.CancelRead(0x10c)
	stream.CancelWrite(0x10c)
	awaitRelease(t, session, done)
	select {
	case <-feedback.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("no QPACK cancellation for the abandoned request")
	}
}

// A request-body read that began before the handler took its stream over
// returns afterwards, and must then behave as a read on the taken-over stream
// would: directions stay independent, the end of receiving is reported once,
// and only connection and message errors fail the request.
func TestServerBodyReadAcrossTakeover(t *testing.T) {
	type expectation struct {
		ended   bool   // receiving is over
		failed  bool   // the request task failed
		aborted uint64 // connection error code
		reset   quic.StreamErrorCode
	}
	reset := &quic.StreamError{ErrorCode: 0x10c, Remote: true}
	lost := &quic.PeerCloseError{Application: true, Code: 0x100}
	rows := []struct {
		name   string
		length string // declared request Content-Length
		// finish makes the waiting read return, after the takeover.
		finish func(raw *RawStream, body *serverRequestBody, peer *io.PipeWriter)
		want   func(error) bool
		expect expectation
	}{
		{"FIN", "", func(_ *RawStream, _ *serverRequestBody, peer *io.PipeWriter) { peer.Close() },
			func(err error) bool { return err == io.EOF }, expectation{ended: true}},
		{"trailers", "", func(_ *RawStream, _ *serverRequestBody, peer *io.PipeWriter) {
			peer.Write(readerHeaders(t, fs("x-end", "done")))
			peer.Close()
		}, func(err error) bool { return err == io.EOF }, expectation{ended: true}},
		{"peer reset", "", func(_ *RawStream, _ *serverRequestBody, peer *io.PipeWriter) { peer.CloseWithError(reset) },
			func(err error) bool { return err == reset }, expectation{ended: true}},
		{"CancelRead", "", func(raw *RawStream, _ *serverRequestBody, _ *io.PipeWriter) { raw.CancelRead(0x77) },
			func(err error) bool { var local *quic.StreamError; return errors.As(err, &local) && !local.Remote }, expectation{ended: true}},
		{"body close", "", func(_ *RawStream, body *serverRequestBody, _ *io.PipeWriter) { body.Close() },
			func(err error) bool { return err != nil && err != io.EOF }, expectation{ended: true}},
		{"deadline", "", func(_ *RawStream, _ *serverRequestBody, peer *io.PipeWriter) {
			peer.CloseWithError(os.ErrDeadlineExceeded)
		},
			func(err error) bool { return errors.Is(err, os.ErrDeadlineExceeded) }, expectation{}},
		{"connection error", "", func(_ *RawStream, _ *serverRequestBody, peer *io.PipeWriter) { peer.CloseWithError(lost) },
			func(err error) bool { return err == lost }, expectation{ended: true}},
		{"malformed frame", "", func(_ *RawStream, _ *serverRequestBody, peer *io.PipeWriter) {
			peer.Write(readerFrame(t, h3.Settings, nil))
		}, func(err error) bool { var protocol *h3.Error; return errors.As(err, &protocol) },
			expectation{ended: true, failed: true, aborted: h3.FrameUnexpected, reset: 0x10d}},
		{"malformed message", "5", func(_ *RawStream, _ *serverRequestBody, peer *io.PipeWriter) { peer.Close() },
			func(err error) bool { var message *MessageError; return errors.As(err, &message) },
			expectation{ended: true, failed: true, reset: 0x10e}},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			pipe, peer := io.Pipe()
			defer peer.Close()
			waiting := &enteredReader{Reader: pipe, entered: make(chan struct{})}
			fields := requestFields("/")
			if row.length != "" {
				fields = append(fields, fs("content-length", row.length))
			}
			reader := requestReader(t, nil, false)
			reader.frames = h3.NewReader(io.MultiReader(bytes.NewReader(readerHeaders(t, fields...)), waiting))
			if _, err := reader.Next(context.Background()); err != nil {
				t.Fatal(err)
			}
			transport, _, _ := exchangeFixture(t, bytes.NewReader(nil))
			// As a QUIC stream does, a canceled read fails with a local error.
			transport.interrupt = func() { peer.CloseWithError(&quic.StreamError{ErrorCode: 0x77}) }
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			var aborted atomic.Uint64
			task := &requestTask{ctx: ctx, stream: transport, cancel: cancel, abort: func(code uint64, _ string) { aborted.Store(code) }}
			var trailers string
			message, err := NewMessageBody(ctx, reader, BodyConfig{
				Trailers: func(fields []qpack.DecodedField) error { trailers = fields[0].Value; return nil },
				// ServeRequest's rule for a body closed before its end.
				Close: func(complete bool) error {
					if !complete {
						transport.CancelRead(0x100)
					}
					return nil
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			body := &serverRequestBody{ReadCloser: message, task: task}
			result := make(chan error, 1)
			go func() { _, err := body.Read(make([]byte, 1)); result <- err }()
			select {
			case <-waiting.entered:
			case <-time.After(3 * time.Second):
				t.Fatal("read never started")
			}
			raw := newRawStream(transport, nil, task, nil)
			raw.body = message
			if err := task.takeOver(raw); err != nil {
				t.Fatal(err)
			}
			select {
			case <-raw.received:
				t.Fatal("receiving over while a read is still waiting")
			default:
			}
			row.finish(raw, body, peer)
			select {
			case err := <-result:
				if !row.want(err) {
					t.Fatal("read returned", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("read never returned")
			}
			// The read has returned, so its effect on the stream is settled.
			var got expectation
			select {
			case <-raw.received:
				got.ended = true
			default:
			}
			got.failed, got.aborted = task.result() != nil, aborted.Load()
			transport.mu.Lock()
			if len(transport.writeCodes) > 0 {
				got.reset = transport.writeCodes[0]
			}
			transport.mu.Unlock()
			if got != row.expect {
				t.Fatalf("got %+v, want %+v", got, row.expect)
			}
			if !row.expect.failed && ctx.Err() != nil {
				t.Fatal("read ended the request:", context.Cause(ctx))
			}
			if row.name == "trailers" && trailers != "done" {
				t.Fatal("trailers lost:", trailers)
			}
			// Sending is untouched unless the request itself failed.
			if !row.expect.failed {
				if _, err := transport.Write([]byte("still writable")); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

// finSink fails to queue FIN while its deadline is expired, as a QUIC stream
// with a full send queue does.
type finSink struct {
	deadlineSink
	expired bool
}

func (s *finSink) Close() error {
	if s.expired {
		return os.ErrDeadlineExceeded
	}
	return s.deadlineSink.Close()
}

// A write deadline can expire while FIN waits for the stream. Nothing was sent,
// so Close can be repeated once the deadline is cleared.
func TestMessageWriterCloseRetriesAfterDeadline(t *testing.T) {
	sink := &finSink{deadlineSink: deadlineSink{limit: -1}, expired: true}
	writer := responseWriterOn(t, sink, "GET")
	if err := writer.Headers(responseFields("200")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); !h3.IsDeadline(err) {
		t.Fatal(err)
	}
	sink.expired = false
	if err := writer.Close(); err != nil || sink.closes != 1 {
		t.Fatal("FIN deadline was not retryable:", err, sink.closes)
	}
	if _, err := writer.Write([]byte("late")); err != io.ErrClosedPipe {
		t.Fatal(err)
	}
}

// expiredTry refuses its first TryWriteAll with an expired deadline.
type expiredTry struct {
	tryStream
	expired bool
}

func (s *expiredTry) TryWriteAll(p []byte) error {
	if s.expired {
		s.expired = false
		return os.ErrDeadlineExceeded
	}
	return s.tryStream.TryWriteAll(p)
}

// An expired deadline queued nothing, so it neither ends the writer nor counts
// against the declared length.
func TestMessageWriterTryWriteAfterDeadline(t *testing.T) {
	stream := &expiredTry{tryStream: tryStream{deadlineSink: deadlineSink{limit: -1}, open: true}, expired: true}
	writer := responseWriterOn(t, stream, "GET")
	if err := writer.Headers(append(responseFields("200"), fs("content-length", "3"))); err != nil {
		t.Fatal(err)
	}
	if err := writer.TryWrite([]byte("abc")); !h3.IsDeadline(err) {
		t.Fatal(err)
	}
	if err := writer.TryWrite([]byte("abc")); err != nil {
		t.Fatal("deadline was not retryable:", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
}

// outcomeBody is a request body whose only Read returns err.
type outcomeBody struct{ err error }

func (b outcomeBody) Read([]byte) (int, error) { return 0, b.err }
func (b outcomeBody) Close() error             { return nil }

func takeoverFixture(t *testing.T, outcome error) (*requestTask, *serverRequestBody, *RawStream, *exchangeStream, context.Context) {
	t.Helper()
	transport, _, _ := exchangeFixture(t, bytes.NewReader(nil))
	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(nil) })
	task := &requestTask{ctx: ctx, stream: transport, cancel: cancel, abort: func(uint64, string) {}}
	body := &serverRequestBody{ReadCloser: outcomeBody{outcome}, task: task}
	raw := newRawStream(transport, nil, task, nil)
	raw.body = body.ReadCloser
	return task, body, raw, transport, ctx
}

// Round 5's interleaving, in both orders. A read returns a peer reset and the
// handler takes the stream over at the same moment. Whichever is decided first
// under the task's lock, the two agree on who owns the stream.
func TestTakeoverRacesReadOutcome(t *testing.T) {
	reset := &quic.StreamError{ErrorCode: 0x10c, Remote: true}
	t.Run("takeover first", func(t *testing.T) {
		task, body, raw, transport, ctx := takeoverFixture(t, reset)
		// The read has its result; the takeover completes before the result is
		// acted on. It is then a raw stream's read.
		task.beforeDecision = func() {
			if err := task.takeOver(raw); err != nil {
				t.Error(err)
			}
		}
		if _, err := body.Read(make([]byte, 1)); err != reset {
			t.Fatal(err)
		}
		if _, writes := (&rawTestStream{transport}).codes(); len(writes) != 0 || ctx.Err() != nil || task.result() != nil {
			t.Fatal("read reset a taken-over stream:", writes, context.Cause(ctx), task.result())
		}
		select {
		case <-raw.received:
		default:
			t.Fatal("raw stream not told that receiving is over")
		}
		if _, err := transport.Write([]byte("still writable")); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("read first", func(t *testing.T) {
		task, body, raw, transport, ctx := takeoverFixture(t, reset)
		// The read has failed the request but not yet reset anything. A takeover
		// attempted now must be refused: there is no stream left to take.
		var refused error
		task.afterDecision = func() { refused = task.takeOver(raw) }
		if _, err := body.Read(make([]byte, 1)); err != reset {
			t.Fatal(err)
		}
		if refused != reset || task.owner() != nil {
			t.Fatal("failed request was taken over:", refused)
		}
		if _, writes := (&rawTestStream{transport}).codes(); len(writes) != 1 || ctx.Err() == nil {
			t.Fatal("ordinary request survived its reset:", writes)
		}
	})
}

// Takeover and a terminal read outcome, concurrently and many times. Whatever
// the order, a successful takeover leaves sending alone unless the error was a
// connection or message error, the end of receiving reaches the raw stream
// exactly once (a second close of its channel would panic), and the stream's
// wait can finish, so its slot is not leaked.
func TestTakeoverConcurrentWithReadOutcome(t *testing.T) {
	outcomes := map[string]error{
		"FIN":        io.EOF,
		"reset":      &quic.StreamError{ErrorCode: 0x10c, Remote: true},
		"connection": &quic.PeerCloseError{Application: true, Code: 0x100},
		"protocol":   &h3.Error{Code: h3.FrameUnexpected, Reason: "test"},
		"message":    &MessageError{"test"},
	}
	for name, outcome := range outcomes {
		t.Run(name, func(t *testing.T) {
			var protocol *h3.Error
			var message *MessageError
			fatal := errors.As(outcome, &protocol) || errors.As(outcome, &message)
			for i := 0; i < 500; i++ {
				task, body, raw, transport, ctx := takeoverFixture(t, outcome)
				start := make(chan struct{})
				var taken error
				var wg sync.WaitGroup
				wg.Add(3)
				go func() { defer wg.Done(); <-start; body.Read(make([]byte, 1)) }()
				go func() { defer wg.Done(); <-start; taken = task.takeOver(raw) }()
				// A second report of the same end must not reach the stream twice.
				go func() { defer wg.Done(); <-start; body.Close() }()
				close(start)
				wg.Wait()
				_, writes := (&rawTestStream{transport}).codes()
				failed := task.result() != nil
				if taken != nil {
					if !failed {
						t.Fatal("takeover refused on a healthy request:", taken)
					}
					continue
				}
				if failed != fatal || (len(writes) != 0) != fatal || (ctx.Err() != nil) != fatal {
					t.Fatal("after takeover:", failed, writes, context.Cause(ctx))
				}
				select {
				case <-raw.received:
				default:
					t.Fatal("end of receiving never reached the raw stream")
				}
				// With sending finished too, the stream's wait is over.
				if !fatal {
					transport.sendCancel(context.Canceled)
				}
				done := make(chan struct{})
				go func() { defer close(done); raw.wait(ctx) }()
				select {
				case <-done:
				case <-time.After(3 * time.Second):
					t.Fatal("stream still waiting: its slot would leak")
				}
			}
		})
	}
}

// finDeadlineStream fails to queue FIN while its deadline is expired.
type finDeadlineStream struct {
	*datagramTestStream
	expired bool
}

func (s *finDeadlineStream) Close() error {
	if s.expired {
		return os.ErrDeadlineExceeded
	}
	return s.datagramTestStream.Close()
}

// A Close that times out before FIN is queued has ended nothing: the stream
// still sends datagrams and DATA, and Close can be repeated. Once FIN is
// queued, datagram sending is over.
func TestCloseDeadlineKeepsStreamUsable(t *testing.T) {
	s, unused, _ := datagramSendSession(t, datagramSinkFunc(func(context.Context, []byte) error { return nil }))
	unused.Remove()
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(context.Canceled)
	receiveCtx, receiveCancel := context.WithCancel(context.Background())
	defer receiveCancel()
	transport := &finDeadlineStream{datagramTestStream: &datagramTestStream{exchangeStream: &exchangeStream{serveStream: &serveStream{source: bytes.NewReader(nil)}, sendCtx: ctx, sendCancel: cancel}, receiveCtx: receiveCtx, receiveCancel: receiveCancel}, expired: true}
	bound, err := s.BindDatagramStream(transport, true)
	if err != nil {
		t.Fatal(err)
	}
	defer bound.Release(context.Background())
	writer := responseWriterOn(t, bound, "GET")
	if err := writer.Headers(responseFields("200")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); !h3.IsDeadline(err) {
		t.Fatal(err)
	}
	if err := bound.SendDatagram(context.Background(), []byte("after the deadline")); err != nil {
		t.Fatal("timed-out Close ended datagram sending:", err)
	}
	if _, err := writer.Write([]byte("more")); err != nil {
		t.Fatal("timed-out Close ended the message:", err)
	}
	transport.expired = false
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := bound.SendDatagram(context.Background(), []byte("after FIN")); !errors.Is(err, ErrDatagramSendClosed) {
		t.Fatal("datagram sent after FIN:", err)
	}
}

// finBlockedStream cannot queue FIN until its send side is reset, as when the
// send queue is full: Close waits, and a reset ends that wait.
type finBlockedStream struct {
	*datagramTestStream
	entered chan struct{}
}

func (s *finBlockedStream) Close() error {
	close(s.entered)
	<-s.sendCtx.Done()
	return context.Cause(s.sendCtx)
}

// Close holds datagram sending off while FIN waits for room, but a reset must
// still reach the transport: that is what ends the wait.
func TestResetInterruptsBlockedClose(t *testing.T) {
	for _, tc := range []struct {
		name  string
		reset func(*DatagramStream, *Session)
	}{
		{"CancelWrite", func(bound *DatagramStream, _ *Session) { bound.CancelWrite(0x10c) }},
		{"AbortRequest", func(bound *DatagramStream, s *Session) {
			_ = s.datagrams.AbortRequest(context.Background(), uint64(bound.StreamID()), 0x33)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, unused, _ := datagramSendSession(t, datagramSinkFunc(func(context.Context, []byte) error { return nil }))
			unused.Remove()
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(context.Canceled)
			receiveCtx, receiveCancel := context.WithCancel(context.Background())
			defer receiveCancel()
			transport := &finBlockedStream{datagramTestStream: &datagramTestStream{id: 4, exchangeStream: &exchangeStream{serveStream: &serveStream{source: bytes.NewReader(nil)}, sendCtx: ctx, sendCancel: cancel}, receiveCtx: receiveCtx, receiveCancel: receiveCancel}, entered: make(chan struct{})}
			bound, err := s.BindDatagramStream(transport, true)
			if err != nil {
				t.Fatal(err)
			}
			defer bound.Release(context.Background())
			defer cancel(context.Canceled) // a failed run must not leave Close blocked
			closed := make(chan error, 1)
			go func() { closed <- bound.Close() }()
			<-transport.entered
			reset := make(chan struct{})
			go func() { tc.reset(bound, s); close(reset) }()
			guard := time.NewTimer(5 * time.Second)
			defer guard.Stop()
			select {
			case <-reset:
			case <-guard.C:
				t.Fatal("reset waited behind the blocked Close")
			}
			select {
			case err := <-closed:
				if err == nil || h3.IsDeadline(err) {
					t.Fatal("Close after reset:", err)
				}
			case <-guard.C:
				t.Fatal("Close was not interrupted by the reset")
			}
			if err := bound.SendDatagram(context.Background(), []byte("after reset")); err == nil {
				t.Fatal("datagram sent after reset")
			}
		})
	}
}

// tryRawStream queues TryWriteAll calls until its send direction ends.
type tryRawStream struct{ *rawTestStream }

func (s *tryRawStream) TryWriteAll(p []byte) error {
	if err := s.sendCtx.Err(); err != nil {
		return context.Cause(s.sendCtx)
	}
	_, err := s.Write(p)
	return err
}

// An empty Write or TryWriteAll sends nothing, but it must still report a send
// direction that has ended, however it ended: a local Close, CancelWrite, or
// the peer's STOP_SENDING, which nothing has written into yet. A DATA frame a
// deadline interrupted changes neither: it is finished by a call with data.
func TestRawStreamEmptyWriteReportsEndedSend(t *testing.T) {
	for _, interrupted := range []bool{false, true} {
		for _, end := range []struct {
			name  string
			end   func(*RawStream, *tryRawStream)
			reset quic.StreamErrorCode // 0: Close
		}{
			{"Close", func(stream *RawStream, _ *tryRawStream) { stream.Close() }, 0},
			{"CancelWrite", func(stream *RawStream, _ *tryRawStream) { stream.CancelWrite(0x10c) }, 0x10c},
			{"STOP_SENDING", func(_ *RawStream, transport *tryRawStream) {
				transport.sendCancel(&quic.StreamError{ErrorCode: 0x10b, Remote: true})
			}, 0x10b},
		} {
			for _, call := range []struct {
				name  string
				empty func(*RawStream) error
			}{
				{"Write", func(stream *RawStream) error {
					n, err := stream.Write(nil)
					if n != 0 {
						t.Fatal("empty write reported", n, "bytes")
					}
					return err
				}},
				{"TryWriteAll", func(stream *RawStream) error { return stream.TryWriteAll(nil) }},
			} {
				if interrupted && end.reset == 0 {
					continue // Close itself fails while a DATA frame is unfinished
				}
				t.Run(fmt.Sprintf("%s/%s/interrupted=%v", end.name, call.name, interrupted), func(t *testing.T) {
					session, raw, config, _ := rawFixture(t, bytes.NewReader(nil))
					transport := &tryRawStream{raw}
					client, err := session.openRequestStream(context.Background(), transport, nil, config)
					if err != nil {
						t.Fatal(err)
					}
					stream := client.RawStream
					defer stream.CancelRead(0x10c)
					request, _ := http.NewRequest("POST", "https://example.com/raw", nil)
					if err := client.SendRequestHeader(request); err != nil {
						t.Fatal(err)
					}
					if interrupted {
						// A deadline cut off this DATA frame's header and payload.
						stream.writer.frame, stream.writer.payload = []byte{0x05}, 5
					}
					sent := len(transport.Bytes())
					if err := call.empty(stream); err != nil {
						t.Fatal("empty call on an open stream:", err)
					}
					if len(transport.Bytes()) != sent {
						t.Fatal("empty call sent a frame")
					}
					end.end(stream, transport)
					err = call.empty(stream)
					if err == nil {
						t.Fatal("empty call succeeded after the send direction ended")
					}
					var reset *quic.StreamError
					if end.reset != 0 && (!errors.As(err, &reset) || reset.ErrorCode != end.reset) {
						t.Fatalf("empty call returned %v, want the reset 0x%x", err, end.reset)
					}
					if len(transport.Bytes()) != sent {
						t.Fatal("empty call sent a frame after the send direction ended")
					}
				})
			}
		}
	}
}

// CancelRead's code must reach the stream before the QPACK wait it cancels
// wakes: the woken receive call resets the stream too, with H3_REQUEST_CANCELLED,
// and the first code is the one the peer sees.
func TestCancelReadCodeBeatsWokenQPACKWait(t *testing.T) {
	blocked := readerFrame(t, h3.Headers, []byte{2, 0, 0x80})
	for _, section := range []string{"headers", "trailers"} {
		t.Run(section, func(t *testing.T) {
			wire := blocked
			if section == "trailers" {
				wire = append(readerHeaders(t, responseFields("200")...), readerFrame(t, h3.Data, []byte("body"))...)
				wire = append(wire, blocked...)
			}
			_, decoder, stream, transport, _ := blockedFixture(t, bytes.NewReader(wire), io.Discard)
			defer stream.CancelWrite(0x10c)
			receive := func() error { _, err := stream.ReadResponse(); return err }
			if section == "trailers" {
				if err := receive(); err != nil {
					t.Fatal(err)
				}
				if _, err := io.ReadFull(stream, make([]byte, 4)); err != nil {
					t.Fatal(err)
				}
				receive = func() error { _, err := stream.Read(make([]byte, 1)); return err }
			}
			var first sync.Once
			stillWaiting := make(chan bool, 1)
			transport.interrupt = func() {
				first.Do(func() {
					decoder.mu.Lock()
					stillWaiting <- len(decoder.pending) == 1
					decoder.mu.Unlock()
				})
			}
			result := make(chan error, 1)
			go func() { result <- receive() }()
			awaitBlockedSection(t, decoder)
			stream.CancelRead(0x77)
			if !<-stillWaiting {
				t.Fatal("the QPACK wait was woken before CancelRead's code reached the stream")
			}
			if err := <-result; err == nil {
				t.Fatal("receive succeeded after CancelRead")
			}
			if reads, _ := transport.codes(); len(reads) == 0 || reads[0] != 0x77 {
				t.Fatal(reads)
			}
		})
	}
}

// Raw request streams and ExchangeRequest share this decision. A graceful peer
// close keeps a response unless a GOAWAY already read rejects its stream; the
// close can arrive before the control stream reader reaches the GOAWAY.
func TestKeptAfterClose(t *testing.T) {
	graceful := &quic.PeerCloseError{Application: true, Code: 0x100}
	for _, row := range []struct {
		name   string
		cause  error
		goaway *PeerGoaway
		id     uint64
		kept   bool
	}{
		{"graceful, GOAWAY unread", graceful, nil, 4, true},
		{"graceful, GOAWAY keeps the stream", graceful, &PeerGoaway{ID: 8, RequestIDs: true}, 4, true},
		{"graceful, GOAWAY rejects the stream", graceful, &PeerGoaway{ID: 4, RequestIDs: true}, 4, false},
		{"error close", &quic.PeerCloseError{Application: true, Code: 0x102}, nil, 4, false},
		{"transport close with the same code", &quic.PeerCloseError{Code: 0x100}, nil, 4, false},
		{"local stop", errors.New("stopped"), nil, 4, false},
	} {
		t.Run(row.name, func(t *testing.T) {
			session, _, _, _ := rawFixture(t, bytes.NewReader(nil))
			if row.goaway != nil {
				session.receiveGoaway(*row.goaway)
			}
			if kept := session.keptAfterClose(row.cause, row.id); kept != row.kept {
				t.Fatalf("kept=%v, want %v", kept, row.kept)
			}
		})
	}
}

// receivingRawStream reports when its receive side has ended, as a QUIC stream
// does: io.EOF once every byte through FIN has arrived.
type receivingRawStream struct {
	*rawTestStream
	receiveCtx context.Context
}

func (s *receivingRawStream) ReceiveContext() context.Context { return s.receiveCtx }

// A graceful close keeps a raw request stream only while the transport can
// still deliver its response: one complete through FIN. A stream still
// receiving, such as one used only for datagrams, can never be read again,
// so it releases its request slot and datagram state at once.
func TestClientStreamGracefulCloseKeepsOnlyCompleteResponses(t *testing.T) {
	for _, complete := range []bool{true, false} {
		t.Run(fmt.Sprintf("complete=%v", complete), func(t *testing.T) {
			wire := append(readerHeaders(t, responseFields("200")...), readerFrame(t, h3.Data, []byte("kept"))...)
			session, raw, config, done := rawFixture(t, bytes.NewReader(wire))
			receiveCtx, receiveCancel := context.WithCancelCause(context.Background())
			defer receiveCancel(context.Canceled)
			if complete {
				receiveCancel(io.EOF)
			}
			stream, err := session.openRequestStream(context.Background(), &receivingRawStream{raw, receiveCtx}, nil, config)
			if err != nil {
				t.Fatal(err)
			}
			request, _ := http.NewRequest("GET", "https://example.com/", nil)
			if err := stream.SendRequestHeader(request); err != nil {
				t.Fatal(err)
			}
			if err := stream.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := stream.ReadResponse(); err != nil {
				t.Fatal(err)
			}
			session.cancel(&quic.PeerCloseError{Application: true, Code: 0x100})
			if !complete {
				awaitDone(t, done)
				return
			}
			body, err := io.ReadAll(stream)
			if err != nil || string(body) != "kept" {
				t.Fatalf("complete response lost after graceful close: %q %v", body, err)
			}
			// Both directions are finished now, so the kept stream is released.
			awaitDone(t, done)
		})
	}
}
