package http3conn

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	"github.com/getlantern/photon/internal/http3wire"
)

type fakeReceive struct {
	io.Reader
	id                 quic.StreamID
	stopped            bool
	code               quic.StreamErrorCode
	entered, interrupt chan struct{}
	once               sync.Once
}

func (s *fakeReceive) StreamID() quic.StreamID { return s.id }
func (s *fakeReceive) Read(p []byte) (int, error) {
	if s.entered != nil {
		s.once.Do(func() { close(s.entered) })
		<-s.interrupt
		return 0, os.ErrDeadlineExceeded
	}
	return s.Reader.Read(p)
}
func (s *fakeReceive) SetReadDeadline(at time.Time) error {
	if !at.IsZero() && s.interrupt != nil {
		close(s.interrupt)
	}
	return nil
}
func (s *fakeReceive) CancelRead(code quic.StreamErrorCode) { s.stopped = true; s.code = code }
func TestIncomingClassification(t *testing.T) {
	in := NewIncoming(true)
	for i, typ := range []byte{0, 2, 3} {
		source := bytes.NewReader([]byte{typ, 42})
		stream := &fakeReceive{Reader: source, id: quic.StreamID(3 + 4*i)}
		got, known, err := in.Classify(context.Background(), stream)
		if err != nil || !known || got != uint64(typ) || source.Len() != 1 || stream.stopped {
			t.Fatal(got, known, err)
		}
		for _, cause := range []error{io.EOF, io.ErrUnexpectedEOF, &quic.StreamError{Remote: true}} {
			var protocol *http3wire.Error
			if !errors.As(in.ReadError(stream.id, cause), &protocol) || protocol.Code != http3wire.ClosedCriticalStream {
				t.Fatal("critical closure", cause)
			}
		}
		for _, cause := range []error{os.ErrDeadlineExceeded, context.Canceled, &quic.StreamError{Remote: false}} {
			if in.ReadError(stream.id, cause) != cause {
				t.Fatal("local failure became closure")
			}
		}
	}
	duplicate := &fakeReceive{Reader: bytes.NewReader([]byte{0}), id: 15}
	var protocol *http3wire.Error
	if _, _, err := in.Classify(context.Background(), duplicate); !errors.As(err, &protocol) || protocol.Code != http3wire.StreamCreationError {
		t.Fatal(err)
	}
	unknown := &fakeReceive{Reader: bytes.NewReader([]byte{0x21, 42}), id: 19}
	if typ, known, err := in.Classify(context.Background(), unknown); err != nil || known || typ != 0x21 || !unknown.stopped {
		t.Fatal(typ, known, err)
	}
	clientPush := &fakeReceive{Reader: bytes.NewReader([]byte{1}), id: 2}
	if _, _, err := NewIncoming(false).Classify(context.Background(), clientPush); err == nil {
		t.Fatal("client push accepted")
	}
}

type errorReader struct{ err error }

func (r errorReader) Read([]byte) (int, error) { return 0, r.err }
func TestIncomingPreludeAndCancel(t *testing.T) {
	for _, source := range []io.Reader{bytes.NewReader(nil), bytes.NewReader([]byte{0x40}), errorReader{&quic.StreamError{Remote: true}}} {
		if _, known, err := NewIncoming(true).Classify(context.Background(), &fakeReceive{Reader: source, id: 3}); err != nil || known {
			t.Fatal(known, err)
		}
	}
	stream := &fakeReceive{id: 3, entered: make(chan struct{}), interrupt: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, _, err := NewIncoming(true).Classify(ctx, stream); done <- err }()
	<-stream.entered
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancel did not interrupt")
	}
}
func TestIncomingConcurrentDuplicates(t *testing.T) {
	in := NewIncoming(true)
	var successes atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, known, err := in.Classify(context.Background(), &fakeReceive{Reader: bytes.NewReader([]byte{2}), id: quic.StreamID(3 + 4*i)})
			if err == nil && known {
				successes.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if successes.Load() != 1 {
		t.Fatal(successes.Load())
	}
}
