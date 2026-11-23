package http3conn

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"

	h3 "github.com/getlantern/photon/internal/http3wire"
	"github.com/getlantern/photon/internal/qpack"
)

func encoderLimits() EncoderLimits {
	return EncoderLimits{Capacity: 128, MaxBlocked: 4, MaxSections: 32, MaxReferences: 64, MaxEncodedSection: 4096, MaxFields: 32, MaxInstruction: 1024, MaxFieldSection: 4096}
}
func applyEncoderBytes(t *testing.T, peer *qpack.DecoderTable, updates *bytes.Buffer) {
	t.Helper()
	for updates.Len() > 0 {
		n, err := peer.ConsumeEncoderUpdate(updates.Bytes(), 4096)
		if err != nil {
			t.Fatal(err)
		}
		updates.Next(n)
	}
}
func TestFieldEncoderCapacityWrap(t *testing.T) {
	var updates bytes.Buffer
	e, err := NewFieldEncoder(SettingsSnapshot{QPACKCapacity: 1024, QPACKBlockedStreams: 1, MaxFieldSectionSize: 4096}, encoderLimits(), &updates)
	if err != nil {
		t.Fatal(err)
	}
	peer, _ := qpack.NewDecoderTable(1024)
	applyEncoderBytes(t, peer, &updates)
	if peer.Capacity() != 128 {
		t.Fatal("local capacity not applied")
	}
	for i := 0; i < 80; i++ {
		field := fs("x-value", fmt.Sprintf("unique-%03d", i))
		if inserted, err := e.Insert(field); err != nil || !inserted {
			t.Fatal(i, inserted, err)
		}
		applyEncoderBytes(t, peer, &updates)
		wire, err := e.Encode(uint64(i*4), []qpack.DecodedField{field})
		if err != nil {
			t.Fatal(err)
		}
		prefix, fields, err := peer.DecodeSection(wire, 4096, 32)
		if err != nil || len(fields) != 1 || fields[0] != field || prefix.RequiredInsertCount != uint64(i+1) {
			t.Fatal(i, prefix, fields, err)
		}
		if err := e.Feedback(qpack.DecoderInstruction{Type: qpack.SectionAcknowledgment, Value: uint64(i * 4)}); err != nil {
			t.Fatal(err)
		}
	}
}
func TestFieldEncoderLimits(t *testing.T) {
	var updates bytes.Buffer
	peerSettings := SettingsSnapshot{QPACKCapacity: 1024, QPACKBlockedStreams: 0, MaxFieldSectionSize: 40}
	e, err := NewFieldEncoder(peerSettings, encoderLimits(), &updates)
	if err != nil {
		t.Fatal(err)
	}
	field := fs("x", "value")
	e.Insert(field)
	peer, _ := qpack.NewDecoderTable(1024)
	applyEncoderBytes(t, peer, &updates)
	block, err := e.Encode(0, []qpack.DecodedField{field})
	if err != nil {
		t.Fatal(err)
	}
	prefix, _, err := peer.DecodeSection(block, 100, 4)
	if err != nil || prefix.RequiredInsertCount != 0 {
		t.Fatal("peer blocked limit ignored", prefix, err)
	}
	if err := e.Feedback(qpack.DecoderInstruction{Type: qpack.InsertCountIncrement, Value: 1}); err != nil {
		t.Fatal(err)
	}
	block, err = e.Encode(4, []qpack.DecodedField{field})
	if err != nil {
		t.Fatal(err)
	}
	prefix, _, err = peer.DecodeSection(block, 100, 4)
	if err != nil || prefix.RequiredInsertCount != 1 {
		t.Fatal("acknowledged entry not used", prefix, err)
	}
	if _, err := e.Encode(8, []qpack.DecodedField{fs("x", "12345678")}); !errors.Is(err, qpack.ErrSectionLimit) {
		t.Fatal("peer field limit ignored", err)
	}
	if _, err := e.Encode(8, []qpack.DecodedField{field}); err != nil {
		t.Fatal("local rejection poisoned encoder", err)
	}
	sensitive := fs("authorization", "secret")
	sensitive.NeverIndex = true
	if inserted, err := e.Insert(sensitive); err != nil || inserted {
		t.Fatal("sensitive insertion")
	}
}

// failedEncoderWriter is also written by a decoder's feedback goroutine, so its
// fields are safe to set and read while one runs.
type failedEncoderWriter struct {
	fail   atomic.Bool
	writes atomic.Int32
}

func failingEncoderWriter() *failedEncoderWriter {
	w := &failedEncoderWriter{}
	w.fail.Store(true)
	return w
}

func (w *failedEncoderWriter) Write(b []byte) (int, error) {
	w.writes.Add(1)
	if w.fail.Load() {
		return len(b) / 2, nil
	}
	return len(b), nil
}
func TestFieldEncoderTerminalFailures(t *testing.T) {
	writer := &failedEncoderWriter{}
	e, err := NewFieldEncoder(SettingsSnapshot{QPACKCapacity: 1024, MaxFieldSectionSize: 4096}, encoderLimits(), writer)
	if err != nil {
		t.Fatal(err)
	}
	writer.fail.Store(true)
	if _, err = e.Insert(fs("x", "value")); err != io.ErrShortWrite {
		t.Fatal(err)
	}
	writes := writer.writes.Load()
	if _, next := e.Encode(0, nil); next != err {
		t.Fatal(next)
	}
	if _, next := e.Insert(fs("y", "v")); next != err {
		t.Fatal(next)
	}
	if e.Feedback(qpack.DecoderInstruction{}) != err || writer.writes.Load() != writes {
		t.Fatal("retried failed writer")
	}
	e, _ = NewFieldEncoder(SettingsSnapshot{MaxFieldSectionSize: 4096}, encoderLimits(), io.Discard)
	err = e.Feedback(qpack.DecoderInstruction{Type: qpack.InsertCountIncrement, Value: 1})
	var protocol *h3.Error
	if !errors.As(err, &protocol) || protocol.Code != QPACKDecoderStreamError {
		t.Fatal(err)
	}
	if _, next := e.Encode(0, nil); next != err {
		t.Fatal("feedback error not sticky")
	}
}
func TestFieldEncoderConcurrent(t *testing.T) {
	var updates bytes.Buffer
	e, _ := NewFieldEncoder(SettingsSnapshot{QPACKCapacity: 1024, MaxFieldSectionSize: 4096}, encoderLimits(), &updates)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Go(func() {
			e.Insert(fs("x", "value"))
			if _, err := e.Encode(0, []qpack.DecodedField{fs("x", "value")}); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	peer, _ := qpack.NewDecoderTable(1024)
	applyEncoderBytes(t, peer, &updates)
	if peer.InsertCount() != 1 {
		t.Fatal("duplicate insertion", peer.InsertCount())
	}
}

func TestSettingsConstructFieldEncoder(t *testing.T) {
	s, _ := NewSettingsState(false, nil)
	writer := &failedEncoderWriter{}
	if _, err := s.NewFieldEncoder(encoderLimits(), writer); err == nil || writer.writes.Load() != 0 {
		t.Fatal("constructed before settings")
	}
	if err := s.ApplyPeer([]h3.Setting{{ID: h3.QPACKMaxTableCapacity, Value: 64}, {ID: h3.MaxFieldSectionSize, Value: 0}}); err != nil {
		t.Fatal(err)
	}
	e, err := s.NewFieldEncoder(encoderLimits(), writer)
	if err != nil || writer.writes.Load() != 1 {
		t.Fatal(err)
	}
	if _, err := e.Encode(0, []qpack.DecodedField{fs(":status", "200")}); !errors.Is(err, qpack.ErrSectionLimit) {
		t.Fatal("zero field limit ignored", err)
	}
	if _, err := e.Encode(0, nil); err != nil {
		t.Fatal(err)
	}
	failure := s.ApplyPeer(nil)
	if _, err := s.NewFieldEncoder(encoderLimits(), writer); err != failure || writer.writes.Load() != 1 {
		t.Fatal("constructed after error", err)
	}
}
