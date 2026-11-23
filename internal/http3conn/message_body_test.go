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

	h3 "github.com/getlantern/photon/internal/http3wire"
	"github.com/getlantern/photon/internal/qpack"
)

func TestMessageBodyFramesAndTrailers(t *testing.T) {
	wire := readerHeaders(t, fs(":status", "200"), fs("content-length", "3"))
	for _, frame := range [][]byte{readerFrame(t, h3.Data, nil), readerFrame(t, h3.Data, []byte("a")), readerFrame(t, 0x21, []byte("ignored")), readerFrame(t, h3.Data, []byte("bc")), readerHeaders(t, fs("x-checksum", "ok")), readerFrame(t, 0x21, nil)} {
		wire = append(wire, frame...)
	}
	reader := responseReader(t, wire)
	if _, err := reader.Next(context.Background()); err != nil {
		t.Fatal(err)
	}
	trailers, closes := 0, 0
	complete := false
	body, err := NewMessageBody(context.Background(), reader, BodyConfig{Trailers: func(fields []qpack.DecodedField) error {
		trailers++
		if len(fields) != 1 || fields[0].Name != "x-checksum" || fields[0].Value != "ok" {
			t.Fatal(fields)
		}
		return nil
	}, Close: func(done bool) error { closes++; complete = done; return nil }})
	if err != nil {
		t.Fatal(err)
	}
	if n, err := body.Read(nil); n != 0 || err != nil {
		t.Fatal(n, err)
	}
	var output bytes.Buffer
	one := make([]byte, 1)
	for {
		n, err := body.Read(one)
		output.Write(one[:n])
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if output.String() != "abc" || trailers != 1 || closes != 0 {
		t.Fatal(output.String(), trailers, closes)
	}
	if _, err := body.Read(one); err != io.EOF {
		t.Fatal(err)
	}
	if body.Close() != nil || body.Close() != nil || closes != 1 || !complete {
		t.Fatal(closes, complete)
	}
}
func TestMessageBodyIncompleteClose(t *testing.T) {
	for _, mode := range []string{"early", "short body", "push"} {
		t.Run(mode, func(t *testing.T) {
			wire := readerHeaders(t, fs(":status", "200"), fs("content-length", "2"))
			if mode == "push" {
				wire = append(wire, readerFrame(t, h3.PushPromise, []byte{0})...)
			} else {
				wire = append(wire, readerFrame(t, h3.Data, []byte("x"))...)
			}
			reader := responseReader(t, wire)
			if _, err := reader.Next(context.Background()); err != nil {
				t.Fatal(err)
			}
			cause := errors.New("cleanup failed")
			closes := 0
			body, err := NewMessageBody(context.Background(), reader, BodyConfig{Close: func(complete bool) error {
				if complete {
					t.Error("incomplete message labeled complete")
				}
				closes++
				return cause
			}})
			if err != nil {
				t.Fatal(err)
			}
			if mode != "early" {
				_, err = io.ReadAll(body)
				if err == nil {
					t.Fatal("invalid body accepted")
				}
				if mode == "short body" {
					var message *MessageError
					if !errors.As(err, &message) {
						t.Fatal(err)
					}
				} else {
					var protocol *h3.Error
					if !errors.As(err, &protocol) || protocol.Code != h3.IDError {
						t.Fatal(err)
					}
				}
			}
			if body.Close() != cause || body.Close() != cause || closes != 1 {
				t.Fatal("cleanup count/error", closes)
			}
			if _, err = body.Read(make([]byte, 1)); err != io.ErrClosedPipe {
				t.Fatal(err)
			}
		})
	}
}
func TestMessageBodyCloseInterruptsRead(t *testing.T) {
	source, destination := io.Pipe()
	defer source.Close()
	defer destination.Close()
	reader := responseReader(t, nil)
	reader.frames = h3.NewReader(io.MultiReader(bytes.NewReader(readerHeaders(t, fs(":status", "200"))), source))
	if _, err := reader.Next(context.Background()); err != nil {
		t.Fatal(err)
	}
	calls := 0
	body, err := NewMessageBody(context.Background(), reader, BodyConfig{Close: func(complete bool) error {
		calls++
		if complete {
			t.Error("blocked body complete")
		}
		return source.CloseWithError(context.Canceled)
	}})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { _, err := body.Read(make([]byte, 1)); result <- err }()
	// The read has consumed a DATA frame header and is waiting for its payload.
	if _, err = destination.Write([]byte{0, 1}); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Go(func() {
			if err := body.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("blocked read succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("close stranded read")
	}
	if calls != 1 {
		t.Fatal("concurrent cleanup count", calls)
	}
}

func TestMessageBodyPreservesCancellationCause(t *testing.T) {
	for _, blocked := range []bool{false, true} {
		name := "before read"
		if blocked {
			name = "blocked trailers"
		}
		t.Run(name, func(t *testing.T) {
			wire := readerHeaders(t, fs(":status", "200"))
			wire = append(wire, readerFrame(t, h3.Headers, []byte{2, 0, 0x80})...)
			reader := responseReader(t, wire)
			var feedback bytes.Buffer
			decoder, err := NewFieldDecoder(decoderLimits(), &feedback)
			if err != nil {
				t.Fatal(err)
			}
			defer decoder.Close(nil)
			owner, err := NewFieldStream(decoder, 0)
			if err != nil {
				t.Fatal(err)
			}
			reader.fields = owner
			if _, err = reader.Next(context.Background()); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(context.Canceled)
			body, err := NewMessageBody(ctx, reader, BodyConfig{Close: func(complete bool) error {
				if complete {
					t.Error("rejected body complete")
				}
				return owner.Cancel()
			}})
			if err != nil {
				t.Fatal(err)
			}
			defer body.Close()
			result := make(chan error, 1)
			if !blocked {
				cancel(ErrRequestRejected)
			}
			go func() { _, err := body.Read(make([]byte, 1)); result <- err }()
			if blocked {
				deadline := time.Now().Add(time.Second)
				for {
					decoder.mu.Lock()
					pending := len(decoder.pending)
					decoder.mu.Unlock()
					if pending == 1 {
						break
					}
					if time.Now().After(deadline) {
						cancel(context.DeadlineExceeded)
						<-result
						t.Fatal("body did not block")
					}
					runtime.Gosched()
				}
				cancel(ErrRequestRejected)
			}
			select {
			case err := <-result:
				if err != ErrRequestRejected {
					t.Fatal("lost request cause", err)
				}
			case <-time.After(time.Second):
				t.Fatal("body stranded")
			}
			if _, err = body.Read(make([]byte, 1)); err != ErrRequestRejected {
				t.Fatal("lost sticky cause", err)
			}
			if err = body.Close(); err != nil {
				t.Fatal(err)
			}
			decoder.Flushed()
			if !bytes.Equal(feedback.Bytes(), []byte{0x40}) {
				t.Fatalf("cancellation count: %x", feedback.Bytes())
			}
		})
	}
}

type cancelingBodySource struct {
	cancel context.CancelCauseFunc
	err    error
}

func (s cancelingBodySource) Read([]byte) (int, error) { s.cancel(ErrRequestRejected); return 0, s.err }
func TestMessageBodyDoesNotRelabelIndependentFailure(t *testing.T) {
	for _, cause := range []error{&h3.Error{Code: h3.FrameUnexpected, Reason: "invalid frame"}, errors.New("transport failed")} {
		reader := responseReader(t, readerHeaders(t, fs(":status", "200")))
		if _, err := reader.Next(context.Background()); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancelCause(context.Background())
		reader.frames = h3.NewReader(cancelingBodySource{cancel: cancel, err: cause})
		body, err := NewMessageBody(ctx, reader, BodyConfig{Close: func(bool) error { return nil }})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = body.Read(make([]byte, 1)); err != cause {
			t.Fatal("reclassified independent failure", err, cause)
		}
		if _, err = body.Read(make([]byte, 1)); err != cause {
			t.Fatal("lost original failure", err)
		}
		body.Close()
		cancel(context.Canceled)
	}
}
