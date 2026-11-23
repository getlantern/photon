package http3conn

import (
	"bytes"
	"context"
	"errors"
	"io"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestFieldStreamCancellation(t *testing.T) {
	var feedback bytes.Buffer
	decoder, _ := NewFieldDecoder(decoderLimits(), &feedback)
	stream, _ := NewFieldStream(decoder, 0)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := stream.Decode(ctx, []byte{2, 0, 0x80}); done <- err }()
	deadline := time.Now().Add(time.Second)
	for {
		decoder.mu.Lock()
		blocked := len(decoder.pending) == 1
		decoder.mu.Unlock()
		if blocked {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("decode never blocked")
		}
		runtime.Gosched()
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("decode stranded")
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Go(func() {
			if err := stream.Cancel(); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	decoder.Flushed()
	if !bytes.Equal(feedback.Bytes(), []byte{0x40}) {
		t.Fatalf("cancellation count: %x", feedback.Bytes())
	}
	if _, err := stream.Decode(context.Background(), []byte{0, 0}); err != ErrFieldSectionCanceled {
		t.Fatal("submitted after cancel", err)
	}
}
func TestFieldStreamSequential(t *testing.T) {
	var feedback bytes.Buffer
	decoder, _ := NewFieldDecoder(decoderLimits(), &feedback)
	stream, _ := NewFieldStream(decoder, 4)
	for i := 0; i < 2; i++ {
		fields, err := stream.Decode(context.Background(), []byte{0, 0})
		if err != nil || len(fields) != 0 {
			t.Fatal(fields, err)
		}
	}
	if feedback.Len() != 0 {
		t.Fatal("static section generated feedback")
	}
	if err := stream.Cancel(); err != nil {
		t.Fatal(err)
	}
	decoder.Flushed()
	if !bytes.Equal(feedback.Bytes(), []byte{0x44}) {
		t.Fatal(feedback.Bytes())
	}
}

func TestFieldStreamCancellationWriteFailure(t *testing.T) {
	writer := &failedEncoderWriter{fail: true}
	decoder, err := NewFieldDecoder(decoderLimits(), writer)
	if err != nil {
		t.Fatal(err)
	}
	stream, _ := NewFieldStream(decoder, 0)
	// Two blocked sections verify that a connection-fatal feedback failure also
	// releases work on a different request stream.
	other, err := decoder.Submit(4, []byte{2, 0, 0x80})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := stream.Decode(ctx, []byte{2, 0, 0x80}); done <- err }()
	deadline := time.Now().Add(time.Second)
	for {
		decoder.mu.Lock()
		blocked := len(decoder.pending) == 2
		decoder.mu.Unlock()
		if blocked {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("decode never blocked")
		}
		runtime.Gosched()
	}
	// The cancellation is queued, so the canceled stream sees its own
	// cancellation. The failed write then fails the decoder for everyone else.
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("decode stranded after write failure")
	}
	decoder.Flushed()
	if got := readySection(t, other); got.Err != io.ErrShortWrite {
		t.Fatal(got)
	}
	if err := stream.Cancel(); err != nil {
		t.Fatal("repeated cancellation changed its result", err)
	}
	if _, err := stream.Decode(context.Background(), []byte{0, 0}); err != ErrFieldSectionCanceled {
		t.Fatal("canceled stream decoded again", err)
	}
	later, _ := NewFieldStream(decoder, 8)
	if _, err := later.Decode(context.Background(), []byte{0, 0}); err != io.ErrShortWrite {
		t.Fatal("lost terminal failure on later decode", err)
	}
	if err := later.Cancel(); err != io.ErrShortWrite {
		t.Fatal("lost terminal failure on later cancellation", err)
	}
	decoder.Flushed()
	if writer.writes != 1 {
		t.Fatal("retried failed feedback", writer.writes)
	}
}
