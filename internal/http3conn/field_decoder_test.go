package http3conn

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"testing"

	h3 "github.com/getlantern/photon/internal/http3wire"
	"github.com/getlantern/photon/internal/qpack"
)

func decoderLimits() qpack.DecoderLimits {
	return qpack.DecoderLimits{MaxTableCapacity: 128, MaxBlockedStreams: 4, MaxBufferedBytes: 4096, MaxEncodedSection: 1024, MaxDecodedSection: 4096, MaxFields: 32, MaxInstructionLiteralBytes: 1024}
}
func decoderUpdate(t *testing.T, d *FieldDecoder, update qpack.EncoderInstruction) error {
	t.Helper()
	wire, err := qpack.AppendEncoderInstruction(nil, update)
	if err != nil {
		t.Fatal(err)
	}
	return d.EncoderInstruction(wire)
}
func readySection(t *testing.T, ch <-chan DecodedSection) DecodedSection {
	t.Helper()
	select {
	case result, ok := <-ch:
		if !ok {
			t.Fatal("empty result channel")
		}
		if _, ok = <-ch; ok {
			t.Fatal("multiple results")
		}
		return result
	default:
		t.Fatal("result not ready")
		return DecodedSection{}
	}
}
func TestFieldDecoderResume(t *testing.T) {
	var feedback bytes.Buffer
	d, err := NewFieldDecoder(decoderLimits(), &feedback)
	if err != nil {
		t.Fatal(err)
	}
	if err := decoderUpdate(t, d, qpack.EncoderInstruction{Type: qpack.SetDynamicCapacity, Index: 128}); err != nil {
		t.Fatal(err)
	}
	a, err := d.Submit(0, []byte{2, 0, 0x80})
	if err != nil {
		t.Fatal(err)
	}
	b, err := d.Submit(4, []byte{2, 0, 0x80})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-a:
		t.Fatal("premature completion")
	default:
	}
	if _, err = d.Submit(0, []byte{0, 0}); !errors.Is(err, qpack.ErrPendingSection) {
		t.Fatal(err)
	}
	if err := decoderUpdate(t, d, qpack.EncoderInstruction{Type: qpack.InsertLiteralName, Name: "x", Value: "value"}); err != nil {
		t.Fatal(err)
	}
	for _, ch := range []<-chan DecodedSection{a, b} {
		result := readySection(t, ch)
		if result.Err != nil || len(result.Fields) != 1 || result.Fields[0] != fs("x", "value") {
			t.Fatal(result)
		}
	}
	if !bytes.Equal(feedback.Bytes(), []byte{0x80, 0x84}) {
		t.Fatalf("feedback %x", feedback.Bytes())
	}
	c, err := d.Submit(0, []byte{2, 0, 0x80})
	if err != nil {
		t.Fatal(err)
	}
	if readySection(t, c).Err != nil {
		t.Fatal("next section failed")
	}
}
func TestFieldDecoderCancelClose(t *testing.T) {
	var feedback bytes.Buffer
	d, _ := NewFieldDecoder(decoderLimits(), &feedback)
	a, _ := d.Submit(0, []byte{2, 0, 0x80})
	b, _ := d.Submit(4, []byte{2, 0, 0x80})
	if err := d.Cancel(0); err != nil {
		t.Fatal(err)
	}
	if readySection(t, a).Err != ErrFieldSectionCanceled {
		t.Fatal("cancel did not release")
	}
	cause := errors.New("connection stopped")
	d.Close(cause)
	if readySection(t, b).Err != cause {
		t.Fatal("close did not release")
	}
	if _, err := d.Submit(8, nil); err != cause {
		t.Fatal(err)
	}
	if d.Cancel(8) != cause || d.EncoderInstruction(nil) != cause {
		t.Fatal("lost terminal error")
	}
	if !bytes.Equal(feedback.Bytes(), []byte{0x40}) {
		t.Fatal("cancel feedback", feedback.Bytes())
	}
}
func TestFieldDecoderFailures(t *testing.T) {
	for _, decompression := range []bool{false, true} {
		d, _ := NewFieldDecoder(decoderLimits(), io.Discard)
		a, _ := d.Submit(0, []byte{2, 0, 0xff})
		var err error
		code := QPACKEncoderStreamError
		if decompression {
			decoderUpdate(t, d, qpack.EncoderInstruction{Type: qpack.SetDynamicCapacity, Index: 128})
			err = decoderUpdate(t, d, qpack.EncoderInstruction{Type: qpack.InsertLiteralName, Name: "x", Value: "v"})
			code = QPACKDecompressionFailed
		} else {
			err = d.EncoderInstruction([]byte{0})
		}
		var protocol *h3.Error
		if !errors.As(err, &protocol) || protocol.Code != code || readySection(t, a).Err != err {
			t.Fatal("mapping", err, code)
		}
	}
	writer := &failedEncoderWriter{fail: true}
	d, _ := NewFieldDecoder(decoderLimits(), writer)
	a, _ := d.Submit(0, []byte{2, 0, 0x80})
	decoderUpdate(t, d, qpack.EncoderInstruction{Type: qpack.SetDynamicCapacity, Index: 128})
	err := decoderUpdate(t, d, qpack.EncoderInstruction{Type: qpack.InsertLiteralName, Name: "x", Value: "v"})
	if err != io.ErrShortWrite || readySection(t, a).Err != err {
		t.Fatal(err)
	}
	writes := writer.writes
	d.Cancel(0)
	if writer.writes != writes {
		t.Fatal("retried uncertain feedback")
	}
}
func TestFieldDecoderCancelResumeRace(t *testing.T) {
	for i := 0; i < 50; i++ {
		d, _ := NewFieldDecoder(decoderLimits(), io.Discard)
		decoderUpdate(t, d, qpack.EncoderInstruction{Type: qpack.SetDynamicCapacity, Index: 128})
		result, _ := d.Submit(0, []byte{2, 0, 0x80})
		wire, _ := qpack.AppendEncoderInstruction(nil, qpack.EncoderInstruction{Type: qpack.InsertLiteralName, Name: "x", Value: "v"})
		var wg sync.WaitGroup
		wg.Go(func() {
			if err := d.Cancel(0); err != nil {
				t.Error(err)
			}
		})
		wg.Go(func() {
			if err := d.EncoderInstruction(wire); err != nil {
				t.Error(err)
			}
		})
		wg.Wait()
		got := readySection(t, result)
		if got.Err != nil && got.Err != ErrFieldSectionCanceled {
			t.Fatal(got)
		}
	}
}
func TestSettingsFieldDecoder(t *testing.T) {
	s, _ := NewSettingsState(true, []h3.Setting{{ID: h3.QPACKMaxTableCapacity, Value: 256}})
	if _, err := s.NewFieldDecoder(decoderLimits(), io.Discard); !errors.Is(err, qpack.ErrDecoderConfig) {
		t.Fatal("advertised capacity exceeds budget", err)
	}
	s, _ = NewSettingsState(true, nil)
	d, err := s.NewFieldDecoder(decoderLimits(), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	err = decoderUpdate(t, d, qpack.EncoderInstruction{Type: qpack.SetDynamicCapacity, Index: 1})
	var protocol *h3.Error
	if !errors.As(err, &protocol) || protocol.Code != QPACKEncoderStreamError {
		t.Fatal("ignored advertised zero capacity", err)
	}
}
