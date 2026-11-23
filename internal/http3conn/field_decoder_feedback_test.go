package http3conn

import (
	"bytes"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	h3 "github.com/getlantern/photon/internal/http3wire"
	"github.com/getlantern/photon/internal/qpack"
)

// stalledFeedback is a decoder stream whose first write blocks until released.
// Everything the decoder produces meanwhile has to queue behind it.
type stalledFeedback struct {
	mu      sync.Mutex
	written bytes.Buffer
	writes  int
	entered chan struct{}
	release chan struct{}
	fail    error // returned by the stalled write once released
}

func newStalledFeedback() *stalledFeedback {
	return &stalledFeedback{entered: make(chan struct{}), release: make(chan struct{})}
}
func (w *stalledFeedback) Write(p []byte) (int, error) {
	w.mu.Lock()
	w.writes++
	first := w.writes == 1
	w.mu.Unlock()
	if first {
		close(w.entered)
		<-w.release
		if w.fail != nil {
			return 0, w.fail
		}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.written.Write(p)
}

// stall leaves the decoder's writer blocked on the cancellation of stream 12.
func stall(t *testing.T, writer *stalledFeedback) *FieldDecoder {
	t.Helper()
	d, err := NewFieldDecoder(decoderLimits(), writer)
	if err != nil {
		t.Fatal(err)
	}
	if err := decoderUpdate(t, d, qpack.EncoderInstruction{Type: qpack.SetDynamicCapacity, Index: 128}); err != nil {
		t.Fatal(err)
	}
	if err := d.Cancel(12); err != nil {
		t.Fatal(err)
	}
	select {
	case <-writer.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("feedback never reached the decoder stream")
	}
	return d
}

func queued(d *FieldDecoder) []qpack.DecoderInstruction {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]qpack.DecoderInstruction(nil), d.queue...)
}

var insertX = qpack.EncoderInstruction{Type: qpack.InsertLiteralName, Name: "x", Value: "v"}

// Results do not wait for the decoder stream, and what was queued behind a
// stalled write goes out in the order the decoder produced it.
func TestFieldDecoderFeedbackQueuesInOrder(t *testing.T) {
	writer := newStalledFeedback()
	d := stall(t, writer)
	first, _ := d.Submit(0, []byte{2, 0, 0x80})
	second, _ := d.Submit(4, []byte{2, 0, 0x80})
	// One insertion completes both sections; two more are only counted.
	for i := 0; i < 3; i++ {
		if err := decoderUpdate(t, d, insertX); err != nil {
			t.Fatal(err)
		}
	}
	for _, result := range []<-chan DecodedSection{first, second} {
		if decoded := readySection(t, result); decoded.Err != nil || len(decoded.Fields) != 1 {
			t.Fatal("section waited for the decoder stream:", decoded)
		}
	}
	if err := d.Cancel(8); err != nil {
		t.Fatal(err)
	}
	want := []qpack.DecoderInstruction{
		{Type: qpack.SectionAcknowledgment, Value: 0}, {Type: qpack.SectionAcknowledgment, Value: 4},
		{Type: qpack.InsertCountIncrement, Value: 2}, // two adjacent increments, merged
		{Type: qpack.StreamCancellation, Value: 8},
	}
	if got := queued(d); len(got) != len(want) {
		t.Fatalf("queued %+v", got)
	} else {
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("queued %+v", got)
			}
		}
	}
	close(writer.release)
	d.Flushed()
	if got := writer.written.Bytes(); !bytes.Equal(got, []byte{0x4c, 0x80, 0x84, 0x02, 0x48}) {
		t.Fatalf("decoder stream %x", got)
	}
	if len(queued(d)) != 0 {
		t.Fatal("feedback left queued")
	}
}

// An increment counts from what the instructions before it acknowledged, so
// increments are merged only when nothing lies between them.
func TestFieldDecoderFeedbackKeepsSeparatedIncrements(t *testing.T) {
	writer := newStalledFeedback()
	d := stall(t, writer)
	decoderUpdate(t, d, insertX)
	d.Cancel(8)
	decoderUpdate(t, d, insertX)
	close(writer.release)
	d.Flushed()
	if got := writer.written.Bytes(); !bytes.Equal(got, []byte{0x4c, 0x01, 0x48, 0x01}) {
		t.Fatalf("decoder stream %x", got)
	}
}

// A peer that never reads its decoder stream cannot grow the queue without
// limit: past the bound the decoder fails and tells its owner.
func TestFieldDecoderFeedbackBacklogIsBounded(t *testing.T) {
	for _, graceful := range []bool{false, true} {
		writer := newStalledFeedback()
		d := stall(t, writer)
		d.maxQueued = 3
		drained := errors.New("peer drained")
		var reported error
		d.feedbackFailed = func(err error) error {
			reported = err
			if graceful {
				return drained
			}
			return nil
		}
		blocked, _ := d.Submit(100, []byte{2, 0, 0x80})
		for id := uint64(16); id < 28; id += 4 {
			if err := d.Cancel(id); err != nil {
				t.Fatal(id, err)
			}
		}
		err := d.Cancel(28)
		var protocol *h3.Error
		if !errors.As(reported, &protocol) || protocol.Code != h3.ExcessiveLoad {
			t.Fatal("owner not told of the backlog:", reported)
		}
		// A graceful close merely leaves the feedback undeliverable.
		want := error(protocol)
		if graceful {
			want = drained
		}
		if err != want || readySection(t, blocked).Err != want {
			t.Fatal(graceful, err)
		}
		if len(queued(d)) > 4 {
			t.Fatal("backlog kept growing:", len(queued(d)))
		}
		if err := d.Cancel(32); err != want {
			t.Fatal("decoder usable past its backlog:", err)
		}
		close(writer.release)
		d.Flushed()
		if got := writer.written.Bytes(); !bytes.Equal(got, []byte{0x4c}) {
			t.Fatalf("feedback written after the decoder ended: %x", got)
		}
	}
}

// Closing the decoder does not wait for a write the peer is holding up, and
// nothing is written afterwards.
func TestFieldDecoderCloseWithStalledFeedback(t *testing.T) {
	for _, detach := range []bool{false, true} {
		writer := newStalledFeedback()
		d := stall(t, writer)
		blocked, _ := d.Submit(0, []byte{2, 0, 0x80})
		d.Cancel(8)
		cause := errors.New("connection stopped")
		returnsPromptly(t, "ending the decoder", func() {
			if detach {
				d.Detach(cause)
			} else {
				d.Close(cause)
			}
		})
		if readySection(t, blocked).Err != cause {
			t.Fatal("blocked section not released")
		}
		returnsPromptly(t, "Cancel", func() {
			if err := d.Cancel(16); err != cause {
				t.Error(err)
			}
		})
		close(writer.release)
		d.Flushed()
		if got := writer.written.Bytes(); !bytes.Equal(got, []byte{0x4c}) {
			t.Fatalf("feedback written after the decoder ended: %x", got)
		}
	}
}

// The owner interrupts the decoder stream with a deadline when it shuts down.
// That must not become the decoder's error: readers treat an expired deadline
// as retryable. The owner's Close supplies the cause.
func TestFieldDecoderInterruptedFeedback(t *testing.T) {
	writer := newStalledFeedback()
	writer.fail = os.ErrDeadlineExceeded
	d := stall(t, writer)
	reports := 0
	d.feedbackFailed = func(error) error { reports++; return nil }
	blocked, _ := d.Submit(0, []byte{2, 0, 0x80})
	close(writer.release)
	d.Flushed()
	select {
	case decoded := <-blocked:
		t.Fatal("interruption decided a waiting section:", decoded.Err)
	default:
	}
	if err := d.Cancel(8); err != nil || reports != 0 {
		t.Fatal("interruption failed the decoder:", err, reports)
	}
	d.Flushed()
	if writer.writes != 1 {
		t.Fatal("wrote to an interrupted decoder stream")
	}
	cause := errors.New("connection stopped")
	d.Close(cause)
	if readySection(t, blocked).Err != cause {
		t.Fatal("section did not get the owner's cause")
	}
}

// The backlog can overflow under any call that produces feedback, and ending
// the decoder there releases every waiting section. The call must still finish
// its own work on the state it started with, and must leave the decoder free:
// a later Close returns.
func TestFieldDecoderBacklogOverflowAtEachEntryPoint(t *testing.T) {
	drained := errors.New("peer drained")
	for _, graceful := range []bool{false, true} {
		for _, entry := range []string{"Submit", "Cancel", "EncoderInstruction"} {
			name := entry + "/fail"
			if graceful {
				name = entry + "/detach"
			}
			t.Run(name, func(t *testing.T) {
				writer := newStalledFeedback()
				defer close(writer.release)
				d := stall(t, writer)
				d.feedbackFailed = func(error) error {
					if graceful {
						return drained
					}
					return nil
				}
				// One entry is in the table; two sections wait for a second.
				if err := decoderUpdate(t, d, insertX); err != nil {
					t.Fatal(err)
				}
				first, _ := d.Submit(0, []byte{3, 0, 0x80})
				second, _ := d.Submit(4, []byte{3, 0, 0x80})
				d.mu.Lock()
				d.maxQueued = len(d.queue)
				d.mu.Unlock()
				overflowed := error(drained)
				finished := make(chan error, 1)
				go func() {
					switch entry {
					case "Submit":
						// Decodes at once and owes an acknowledgment.
						result, err := d.Submit(8, []byte{2, 0, 0x80})
						if err == nil {
							err = (<-result).Err
						}
						finished <- err
					case "Cancel":
						finished <- d.Cancel(0)
					case "EncoderInstruction":
						// Completes both waiting sections: two acknowledgments.
						wire, _ := qpack.AppendEncoderInstruction(nil, insertX)
						finished <- d.EncoderInstruction(wire)
					}
				}()
				var err error
				select {
				case err = <-finished:
				case <-time.After(3 * time.Second):
					t.Fatal(entry, "never returned from the overflow")
				}
				var protocol *h3.Error
				if !graceful {
					if !errors.As(err, &protocol) || protocol.Code != h3.ExcessiveLoad {
						t.Fatal(err)
					}
					overflowed = protocol
				} else if entry != "Submit" && err != drained || entry == "Submit" && err != nil {
					// A section that decoded before the detach keeps its fields.
					t.Fatal(err)
				}
				// Each waiting section was either completed by the call or
				// released by the decoder's end; none is left waiting.
				for i, result := range []<-chan DecodedSection{first, second} {
					decoded := readySection(t, result)
					switch {
					case entry == "EncoderInstruction":
						if decoded.Err != nil || len(decoded.Fields) != 1 {
							t.Fatal("section the insertion completed:", decoded)
						}
					case entry == "Cancel" && i == 0:
						if decoded.Err != ErrFieldSectionCanceled {
							t.Fatal("canceled section:", decoded.Err)
						}
					default:
						if decoded.Err != overflowed {
							t.Fatal("waiting section:", decoded.Err)
						}
					}
				}
				returnsPromptly(t, "Close", func() { d.Close(errors.New("connection stopped")) })
			})
		}
	}
}
