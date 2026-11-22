package http3conn

import (
	"bytes"
	"context"
	"errors"
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
