package http3conn

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"runtime"
	"testing"
	"testing/iotest"
	"time"

	quic "github.com/getlantern/photon"
)

func boundDatagramFixture(t *testing.T, source io.Reader) (*DatagramStream, *datagramTestStream, *Session) {
	t.Helper()
	s, unused, _ := datagramSendSession(t, datagramSinkFunc(func(context.Context, []byte) error { return nil }))
	unused.Remove()
	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(context.Canceled) })
	receiveCtx, receiveCancel := context.WithCancel(context.Background())
	t.Cleanup(receiveCancel)
	raw := &datagramTestStream{exchangeStream: &exchangeStream{serveStream: &serveStream{source: source}, sendCtx: ctx, sendCancel: cancel}, receiveCtx: receiveCtx, receiveCancel: receiveCancel}
	bound, err := s.BindDatagramStream(raw, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { bound.Release(context.Background()) })
	return bound, raw, s
}
func TestDatagramStreamReadLifetime(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		closes bool
	}{
		{"fin", io.EOF, true}, {"deadline", os.ErrDeadlineExceeded, false}, {"reset", &quic.StreamError{ErrorCode: 77}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bound, _, s := boundDatagramFixture(t, iotest.ErrReader(tc.err))
			if _, err := bound.Read(make([]byte, 1)); !errors.Is(err, tc.err) {
				t.Fatal(err)
			}
			s.datagrams.Deliver([]byte{0, 9})
			data, err := bound.request.Receive()
			if tc.closes {
				if !errors.Is(err, ErrDatagramRequestClosed) {
					t.Fatal(err)
				}
			} else if err != nil || !bytes.Equal(data, []byte{9}) {
				t.Fatal(data, err)
			}
			if err := bound.SendDatagram(context.Background(), []byte{8}); err != nil {
				t.Fatal("receive failure closed send", err)
			}
		})
	}
}
func TestDatagramStreamSendAndCleanup(t *testing.T) {
	for _, mode := range []string{"fin", "reset", "peer-stop", "release"} {
		t.Run(mode, func(t *testing.T) {
			bound, raw, s := boundDatagramFixture(t, bytes.NewReader(nil))
			var want error = ErrDatagramSendClosed
			switch mode {
			case "fin":
				if err := bound.Close(); err != nil {
					t.Fatal(err)
				}
			case "reset":
				bound.CancelWrite(42)
			case "peer-stop":
				want = &quic.StreamError{ErrorCode: 42}
				raw.sendCancel(want)
			case "release":
				if err := bound.Release(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			if err := bound.SendDatagram(context.Background(), nil); !errors.Is(err, want) {
				t.Fatal(err)
			}
			if mode != "release" {
				s.datagrams.Deliver([]byte{0, 9})
				if data, err := bound.ReceiveDatagram(context.Background()); err != nil || !bytes.Equal(data, []byte{9}) {
					t.Fatal("send closure affected receive", data, err)
				}
			}
		})
	}
}

type datagramTestStream struct {
	id quic.StreamID
	*exchangeStream
	receiveCtx    context.Context
	receiveCancel context.CancelFunc
}

func (s *datagramTestStream) ReceiveContext() context.Context { return s.receiveCtx }

func TestDatagramStreamTransportReceiveCompletion(t *testing.T) {
	bound, raw, s := boundDatagramFixture(t, bytes.NewReader([]byte("unread")))
	if err := s.datagrams.Deliver([]byte{0, 9}); err != nil {
		t.Fatal(err)
	}
	// Stop the callback to prove admission checks work without its scheduling.
	bound.stopReceiveObserver()
	raw.receiveCancel()
	if err := s.datagrams.Deliver([]byte{0, 10}); err != nil {
		t.Fatal(err)
	}
	if _, err := bound.ReceiveDatagram(context.Background()); err != ErrDatagramRequestClosed {
		t.Fatal(err)
	}
	if data, err := io.ReadAll(bound); err != nil || string(data) != "unread" {
		t.Fatal(string(data), err)
	}
	if err := bound.SendDatagram(context.Background(), nil); err != nil {
		t.Fatal("receive completion closed send", err)
	}
}

// A wakeup is only exercised once the receiver has installed its waiter.
func awaitDatagramWaiter(ctx context.Context, t *testing.T, r *DatagramRequest) {
	t.Helper()
	for {
		r.owner.mu.Lock()
		waiting := r.changed != nil
		r.owner.mu.Unlock()
		if waiting {
			return
		}
		if ctx.Err() != nil {
			t.Fatal("receiver never waited")
		}
		runtime.Gosched()
	}
}

func TestDatagramStreamReceiveCompletionWakesWaiter(t *testing.T) {
	bound, raw, _ := boundDatagramFixture(t, bytes.NewReader(nil))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := bound.ReceiveDatagram(ctx); done <- err }()
	awaitDatagramWaiter(ctx, t, bound.request)
	raw.receiveCancel()
	if err := <-done; err != ErrDatagramRequestClosed {
		t.Fatal(err)
	}
}

func TestDatagramStreamBindAfterReceiveCompletion(t *testing.T) {
	bound, raw, s := boundDatagramFixture(t, bytes.NewReader(nil))
	bound.Release(context.Background())
	raw.receiveCancel()
	// A distinct stream ID avoids lifetime reuse in the dispatcher.
	raw.id = 4
	late, err := s.BindDatagramStream(raw, false)
	if err != nil {
		t.Fatal(err)
	}
	defer late.Release(context.Background())
	if err := s.datagrams.Deliver([]byte{1, 9}); err != nil {
		t.Fatal("closed unsupported request was aborted", err)
	}
	if _, err := late.ReceiveDatagram(context.Background()); err != ErrDatagramRequestClosed {
		t.Fatal(err)
	}
}

func (s *datagramTestStream) StreamID() quic.StreamID { return s.id }

func TestBoundDatagramAbortAndDetach(t *testing.T) {
	bound, raw, s := boundDatagramFixture(t, bytes.NewReader(nil))
	if err := s.datagrams.AbortRequest(context.Background(), uint64(raw.StreamID()), 0x33); err != nil {
		t.Fatal(err)
	}
	raw.mu.Lock()
	if len(raw.readCodes) != 1 || raw.readCodes[0] != 0x33 || len(raw.writeCodes) != 1 || raw.writeCodes[0] != 0x33 {
		t.Fatal(raw.readCodes, raw.writeCodes)
	}
	raw.mu.Unlock()
	if _, err := bound.ReceiveDatagram(context.Background()); err != ErrDatagramRequestClosed {
		t.Fatal(err)
	}
	bound.DetachDatagrams()
	bound.DetachDatagrams()
	if err := s.datagrams.AbortRequest(context.Background(), uint64(raw.StreamID()), 0x33); err != nil {
		t.Fatal(err)
	}
	raw.mu.Lock()
	defer raw.mu.Unlock()
	if len(raw.readCodes) != 1 || len(raw.writeCodes) != 1 {
		t.Fatal("removed stream was reset again")
	}
}
