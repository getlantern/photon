package http3conn

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/http3wire"
)

type fakeStream struct {
	prioritySet         bool
	priority            http3wire.Priority
	writeBeforePriority bool
	bytes.Buffer
	block     bool
	entered   chan struct{}
	interrupt chan struct{}
	once      sync.Once
	fail      error
	short     bool
	cleared   bool
}

func (s *fakeStream) SetPriority(u int8, i bool) {
	s.prioritySet = true
	s.priority = http3wire.Priority{Urgency: u, Incremental: i}
}

func (s *fakeStream) Write(p []byte) (int, error) {
	if !s.prioritySet {
		s.writeBeforePriority = true
	}
	if s.block {
		close(s.entered)
		<-s.interrupt
		return 0, context.DeadlineExceeded
	}
	if s.fail != nil {
		return 0, s.fail
	}
	if s.short {
		return 0, nil
	}
	return s.Buffer.Write(p)
}
func (s *fakeStream) SetWriteDeadline(at time.Time) error {
	if !at.IsZero() {
		s.once.Do(func() { close(s.interrupt) })
	} else {
		s.cleared = true
	}
	return nil
}

type fakeTransport struct {
	streams []*fakeStream
	opens   int
	failAt  int
	aborted uint64
}

func (t *fakeTransport) OpenUni(context.Context) (SendStream, error) {
	t.opens++
	if t.opens == t.failAt {
		return nil, io.ErrClosedPipe
	}
	return t.streams[t.opens-1], nil
}
func (t *fakeTransport) Abort(code uint64, _ string) error { t.aborted = code; return nil }
func fixture() *fakeTransport                              { return &fakeTransport{streams: []*fakeStream{{}, {}, {}}} }
func TestOpenLocal(t *testing.T) {
	transport := fixture()
	settings := []http3wire.Setting{{ID: 1, Value: 128}}
	local, err := OpenLocal(context.Background(), transport, settings)
	if err != nil || local == nil || transport.opens != 3 || transport.aborted != 0 {
		t.Fatal(local, err)
	}
	expected, _ := http3wire.AppendSettings([]byte{0}, settings)
	for i, want := range [][]byte{expected, {2}, {3}} {
		s := transport.streams[i]
		if s.writeBeforePriority || !s.prioritySet || s.priority != (http3wire.Priority{Urgency: 0, Incremental: true}) {
			t.Fatal("critical stream priority not set before setup", i)
		}
		if !bytes.Equal(transport.streams[i].Bytes(), want) || !transport.streams[i].cleared {
			t.Fatal(i)
		}
	}
}
func TestOpenLocalFailures(t *testing.T) {
	for i := 1; i <= 3; i++ {
		transport := fixture()
		transport.failAt = i
		if local, err := OpenLocal(context.Background(), transport, nil); err == nil || local != nil || transport.aborted != http3wire.StreamCreationError {
			t.Fatal(i, err)
		}
	}
	for _, short := range []bool{false, true} {
		transport := fixture()
		transport.streams[1].short = short
		if !short {
			transport.streams[1].fail = io.ErrClosedPipe
		}
		if local, err := OpenLocal(context.Background(), transport, nil); err == nil || local != nil || transport.aborted != InternalError {
			t.Fatal(err)
		}
	}
	transport := fixture()
	if _, err := OpenLocal(context.Background(), transport, []http3wire.Setting{{ID: 2}}); err == nil || transport.opens != 0 || transport.aborted != 0 {
		t.Fatal("invalid settings", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := OpenLocal(ctx, transport, nil); !errors.Is(err, context.Canceled) || transport.opens != 0 {
		t.Fatal(err)
	}
}
func TestOpenLocalCancellationInterruptsWrite(t *testing.T) {
	transport := fixture()
	stream := transport.streams[0]
	stream.block = true
	stream.entered = make(chan struct{})
	stream.interrupt = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := OpenLocal(ctx, transport, nil); done <- err }()
	<-stream.entered
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || transport.aborted != InternalError || !stream.cleared {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked write did not stop")
	}
}

type cancelAfterOpen struct {
	*fakeTransport
	cancel context.CancelCauseFunc
	cause  error
}

func (t cancelAfterOpen) OpenUni(ctx context.Context) (SendStream, error) {
	stream, err := t.fakeTransport.OpenUni(ctx)
	t.cancel(t.cause)
	return stream, err
}
func TestOpenLocalCancellationBetweenOpenAndWrite(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(context.Canceled)
	cause := errors.New("caller canceled after stream allocation")
	transport := cancelAfterOpen{fakeTransport: fixture(), cancel: cancel, cause: cause}
	if local, err := OpenLocal(ctx, transport, nil); local != nil || err != cause {
		t.Fatal(local, err)
	}
	if transport.opens != 1 || transport.streams[0].Len() != 0 || transport.aborted != InternalError {
		t.Fatal("partial setup not terminated", transport.opens, transport.aborted)
	}
}
