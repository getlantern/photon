package http3conn

import (
	"bytes"
	"context"
	"errors"
	"io"
	"runtime"
	"testing"
	"time"

	h3 "github.com/getlantern/photon/internal/http3wire"
	"github.com/getlantern/photon/internal/qpack"
)

func readerFrame(t *testing.T, typ uint64, body []byte) []byte {
	t.Helper()
	b, err := h3.AppendHeader(nil, h3.Header{Type: typ, Length: uint64(len(body))})
	if err != nil {
		t.Fatal(err)
	}
	return append(b, body...)
}
func readerHeaders(t *testing.T, fields ...qpack.DecodedField) []byte {
	t.Helper()
	b := []byte{0, 0}
	for _, f := range fields {
		var err error
		b, err = qpack.AppendFieldLine(b, qpack.FieldLine{Type: qpack.LiteralName, Name: f.Name, Value: f.Value})
		if err != nil {
			t.Fatal(err)
		}
	}
	return readerFrame(t, h3.Headers, b)
}
func responseReader(t *testing.T, wire []byte) *MessageReader {
	t.Helper()
	d, err := NewFieldDecoder(decoderLimits(), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close(nil) })
	f, err := NewFieldStream(d, 0)
	if err != nil {
		t.Fatal(err)
	}
	r, err := NewMessageReader(bytes.NewReader(wire), f, MessageConfig{Kind: ResponseFields, RequestMethod: "GET"}, 1024)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func TestMessageReaderLifecycle(t *testing.T) {
	wire := readerHeaders(t, fs(":status", "103"))
	wire = append(wire, readerHeaders(t, fs(":status", "200"), fs("content-length", "3"))...)
	wire = append(wire, readerFrame(t, 0x21, []byte("extension"))...)
	wire = append(wire, readerFrame(t, h3.Data, []byte("abc"))...)
	wire = append(wire, readerHeaders(t, fs("x-checksum", "ok"))...)
	r := responseReader(t, wire)
	ctx := context.Background()
	for _, want := range []SectionKind{InformationalSection, InitialSection} {
		event, err := r.Next(ctx)
		if err != nil || event.Section != want || event.Header.Type != h3.Headers {
			t.Fatal(event, err)
		}
	}
	event, err := r.Next(ctx)
	if err != nil || event.Header.Type != 0x21 {
		t.Fatal(event, err)
	}
	if _, err = r.Next(ctx); !errors.Is(err, h3.ErrPayloadPending) {
		t.Fatal("payload backpressure", err)
	}
	if b, err := io.ReadAll(r); err != nil || string(b) != "extension" {
		t.Fatal(string(b), err)
	}
	if event, err = r.Next(ctx); err != nil || event.Header.Type != h3.Data {
		t.Fatal(event, err)
	}
	var one [1]byte
	if n, err := r.Read(one[:]); n != 1 || err != nil || one[0] != 'a' {
		t.Fatal(n, err)
	}
	if b, err := io.ReadAll(r); err != nil || string(b) != "bc" {
		t.Fatal(string(b), err)
	}
	event, err = r.Next(ctx)
	if err != nil || event.Section != TrailerSection || event.Fields[0].Value != "ok" {
		t.Fatal(event, err)
	}
	for i := 0; i < 2; i++ {
		if _, err = r.Next(ctx); err != io.EOF {
			t.Fatal("complete message", err)
		}
	}
}
func TestMessageReaderFailures(t *testing.T) {
	headers := readerHeaders(t, fs(":status", "200"), fs("content-length", "3"))
	for _, tc := range []struct {
		name    string
		wire    []byte
		code    uint64
		message bool
	}{
		{"empty DATA before headers", readerFrame(t, h3.Data, nil), h3.FrameUnexpected, false},
		{"control on request", readerFrame(t, h3.Settings, nil), h3.FrameUnexpected, false},
		{"truncated HEADERS", []byte{1, 3, 0}, h3.FrameError, false},
		{"oversized HEADERS", []byte{1, 0x44, 1}, h3.ExcessiveLoad, false},
		{"missing headers", nil, 0, true},
		{"short body", append(bytes.Clone(headers), readerFrame(t, h3.Data, []byte("ab"))...), 0, true},
		{"long body", append(bytes.Clone(headers), readerFrame(t, h3.Data, []byte("abcd"))...), 0, true},
		{"truncated DATA", append(bytes.Clone(headers), []byte{0, 3, 'a'}...), h3.FrameError, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := responseReader(t, tc.wire)
			var err error
			for err == nil {
				_, err = r.Next(context.Background())
				if err == nil {
					_, err = io.Copy(io.Discard, r)
				}
			}
			if tc.message {
				var me *MessageError
				if !errors.As(err, &me) {
					t.Fatal(err)
				}
			} else {
				var he *h3.Error
				if !errors.As(err, &he) || he.Code != tc.code {
					t.Fatal(err)
				}
			}
			if _, again := r.Next(context.Background()); again != err {
				t.Fatal("lost terminal error", again, err)
			}
		})
	}
}

func TestMessageReaderBlockedQPACK(t *testing.T) {
	for _, outcome := range []string{"resume", "cancel", "connection failure"} {
		t.Run(outcome, func(t *testing.T) {
			var feedback bytes.Buffer
			decoder, err := NewFieldDecoder(decoderLimits(), &feedback)
			if err != nil {
				t.Fatal(err)
			}
			defer decoder.Close(nil)
			if err := decoderUpdate(t, decoder, qpack.EncoderInstruction{Type: qpack.SetDynamicCapacity, Index: 128}); err != nil {
				t.Fatal(err)
			}
			owner, err := NewFieldStream(decoder, 0)
			if err != nil {
				t.Fatal(err)
			}
			// RIC=1, Base=1; dynamic entry 0 will contain :status=200.
			section := []byte{2, 0, 0x80}
			wire := readerFrame(t, h3.Headers, section)
			wire = append(wire, readerFrame(t, h3.Data, []byte("body"))...)
			reader, err := NewMessageReader(bytes.NewReader(wire), owner, MessageConfig{Kind: ResponseFields, RequestMethod: "GET"}, 1024)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			type result struct {
				event MessageEvent
				err   error
			}
			done := make(chan result, 1)
			go func() { event, err := reader.Next(ctx); done <- result{event, err} }()
			// Observe ownership before releasing updates or cancellation; no timing sleep.
			timeout := time.Now().Add(time.Second)
			for {
				decoder.mu.Lock()
				blocked := len(decoder.pending) == 1
				decoder.mu.Unlock()
				if blocked {
					break
				}
				if time.Now().After(timeout) {
					cancel()
					decoder.Close(context.DeadlineExceeded)
					<-done
					t.Fatal("reader never blocked")
				}
				runtime.Gosched()
			}
			failure := errors.New("connection closed")
			switch outcome {
			case "resume":
				if err := decoderUpdate(t, decoder, qpack.EncoderInstruction{Type: qpack.InsertLiteralName, Name: ":status", Value: "200"}); err != nil {
					t.Fatal(err)
				}
			case "cancel":
				cancel()
			case "connection failure":
				decoder.Close(failure)
			}
			var got result
			select {
			case got = <-done:
			case <-time.After(time.Second):
				cancel()
				decoder.Close(context.DeadlineExceeded)
				t.Fatal("reader stranded")
			}
			if outcome == "resume" {
				if got.err != nil || got.event.Section != InitialSection || got.event.Control.Status != 200 {
					t.Fatal(got)
				}
				decoder.Flushed()
				if !bytes.Equal(feedback.Bytes(), []byte{0x80}) {
					t.Fatalf("ACK: %x", feedback.Bytes())
				}
				event, err := reader.Next(ctx)
				if err != nil || event.Header.Type != h3.Data {
					t.Fatal(event, err)
				}
				body, err := io.ReadAll(reader)
				if err != nil || string(body) != "body" {
					t.Fatal(string(body), err)
				}
				if _, err = reader.Next(ctx); err != io.EOF {
					t.Fatal(err)
				}
				return
			}
			want := failure
			if outcome == "cancel" {
				want = context.Canceled
				decoder.Flushed()
				if !bytes.Equal(feedback.Bytes(), []byte{0x40}) {
					t.Fatalf("cancel: %x", feedback.Bytes())
				}
			}
			if got.err != want {
				t.Fatal(got.err, want)
			}
			if _, err := reader.Next(context.Background()); err != want {
				t.Fatal("terminal Next", err)
			}
			if _, err := reader.Read(make([]byte, 8)); err != want {
				t.Fatal("terminal Read", err)
			}
			decoder.mu.Lock()
			pending := len(decoder.pending)
			decoder.mu.Unlock()
			if pending != 0 {
				t.Fatal("retained blocked request", pending)
			}
			if outcome == "cancel" {
				if err := owner.Cancel(); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(feedback.Bytes(), []byte{0x40}) {
					t.Fatal("duplicate cancellation")
				}
				// Delayed inserts must not revive this message or create an acknowledgment.
				if err := decoderUpdate(t, decoder, qpack.EncoderInstruction{Type: qpack.InsertLiteralName, Name: ":status", Value: "200"}); err != nil {
					t.Fatal(err)
				}
				decoder.Flushed()
				if !bytes.Equal(feedback.Bytes(), []byte{0x40, 1}) {
					t.Fatalf("late feedback: %x", feedback.Bytes())
				}
			}
		})
	}
}
