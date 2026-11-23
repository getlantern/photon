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

func TestEarlyEncoderPreservesReferences(t *testing.T) {
	settings := []h3.Setting{{ID: h3.QPACKMaxTableCapacity, Value: 1024}, {ID: h3.QPACKBlockedStreams, Value: 1}, {ID: h3.MaxFieldSectionSize, Value: 100}}
	var updates bytes.Buffer
	e, err := NewEarlyFieldEncoder(settings, encoderLimits(), &updates)
	if err != nil {
		t.Fatal(err)
	}
	peer, _ := qpack.NewDecoderTable(1024)
	applyEncoderBytes(t, peer, &updates)
	field := fs("x-early", "retained")
	if ok, err := e.Insert(field); err != nil || !ok {
		t.Fatal(ok, err)
	}
	applyEncoderBytes(t, peer, &updates)
	block, err := e.Encode(0, []qpack.DecodedField{field})
	if err != nil {
		t.Fatal(err)
	}
	prefix, _, err := peer.DecodeSection(block, 100, 4)
	if err != nil || prefix.RequiredInsertCount != 1 {
		t.Fatal(prefix, err)
	}
	table := e.table
	live := append([]h3.Setting(nil), settings...)
	live[1].Value = 4
	live[2].Value = 4096
	if err := e.ApplyPeerSettings(live); err != nil {
		t.Fatal(err)
	}
	if e.table != table || e.table.InsertCount() != 1 || updates.Len() != 0 {
		t.Fatal("table reset or unexpected update")
	}
	second, err := e.Encode(4, []qpack.DecodedField{field})
	if err != nil {
		t.Fatal(err)
	}
	secondPrefix, _, err := peer.DecodeSection(second, 100, 4)
	if err != nil || secondPrefix.RequiredInsertCount != 1 {
		t.Fatal("increased blocked-stream limit not applied", err)
	}
	if err := e.Feedback(qpack.DecoderInstruction{Type: qpack.SectionAcknowledgment, Value: 0}); err != nil {
		t.Fatal("early feedback lost", err)
	}
	if e.table.KnownReceivedCount() != 1 {
		t.Fatal("feedback state reset")
	}
	// Increased field-section credit is usable, subject to original local policy.
	fields := []qpack.DecodedField{fs("x-large", string(bytes.Repeat([]byte{'a'}, 200)))}
	if _, err := e.Encode(8, fields); err != nil {
		t.Fatal("live field limit not applied", err)
	}
}

func TestEarlyEncoderZeroCapacityPromotion(t *testing.T) {
	var updates bytes.Buffer
	e, err := NewEarlyFieldEncoder([]h3.Setting{{ID: h3.MaxFieldSectionSize, Value: 100}}, encoderLimits(), &updates)
	if err != nil {
		t.Fatal(err)
	}
	if updates.Len() != 0 {
		t.Fatal("instructions with zero peer capacity")
	}
	field := fs("x-new", "value")
	if ok, err := e.Insert(field); err != nil || ok {
		t.Fatal(ok, err)
	}
	if err := e.ApplyPeerSettings([]h3.Setting{{ID: h3.QPACKMaxTableCapacity, Value: 1024}, {ID: h3.QPACKBlockedStreams, Value: 10}, {ID: h3.MaxFieldSectionSize, Value: 8192}}); err != nil {
		t.Fatal(err)
	}
	peer, _ := qpack.NewDecoderTable(1024)
	applyEncoderBytes(t, peer, &updates)
	if peer.Capacity() != encoderLimits().Capacity {
		t.Fatal("local capacity policy lost")
	}
	if e.limits.MaxFieldSection != encoderLimits().MaxFieldSection {
		t.Fatal("local field limit policy lost")
	}
	if ok, err := e.Insert(field); err != nil || !ok {
		t.Fatal(ok, err)
	}
	applyEncoderBytes(t, peer, &updates)
	block, err := e.Encode(0, []qpack.DecodedField{field})
	if err != nil {
		t.Fatal(err)
	}
	prefix, got, err := peer.DecodeSection(block, 100, 4)
	if err != nil || prefix.RequiredInsertCount != 1 || len(got) != 1 || got[0] != field {
		t.Fatal(prefix, got, err)
	}
}

func TestEarlyEncoderTransitionFailure(t *testing.T) {
	var updates bytes.Buffer
	settings := []h3.Setting{{ID: h3.QPACKMaxTableCapacity, Value: 1024}, {ID: h3.MaxFieldSectionSize, Value: 100}}
	e, err := NewEarlyFieldEncoder(settings, encoderLimits(), &updates)
	if err != nil {
		t.Fatal(err)
	}
	updates.Reset()
	bad := append([]h3.Setting(nil), settings...)
	bad[0].Value = 2048
	err = e.ApplyPeerSettings(bad)
	var protocol *h3.Error
	if !errors.As(err, &protocol) || protocol.Code != QPACKDecoderStreamError {
		t.Fatal(err)
	}
	if updates.Len() != 0 || e.limits.MaxFieldSection != 100 {
		t.Fatal("failed promotion mutated state")
	}
	if _, next := e.Encode(0, nil); next != err {
		t.Fatal("terminal error lost")
	}
	if e.ApplyPeerSettings(settings) != err {
		t.Fatal("failed promotion retried")
	}
	// A write failure while activating capacity is terminal as well.
	e, err = NewEarlyFieldEncoder([]h3.Setting{{ID: h3.MaxFieldSectionSize, Value: 100}}, encoderLimits(), earlyFailWriter{})
	if err != nil {
		t.Fatal(err)
	}
	settings[0].Value = 128
	if err := e.ApplyPeerSettings(settings); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal(err)
	}
	if _, err := e.Insert(fs("x", "v")); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal("continued after uncertain critical write", err)
	}
}

type earlyFailWriter struct{}

func (earlyFailWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestEarlyEncoderConcurrentPromotion(t *testing.T) {
	e, err := NewEarlyFieldEncoder([]h3.Setting{{ID: h3.MaxFieldSectionSize, Value: 100}}, encoderLimits(), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	for i := 0; i < 4; i++ {
		workers.Add(1)
		go func(id uint64) {
			defer workers.Done()
			for j := 0; j < 100; j++ {
				if _, err := e.Encode(id, []qpack.DecodedField{fs(":method", "GET")}); err != nil {
					t.Error(err)
					return
				}
			}
		}(uint64(i * 4))
	}
	if err := e.ApplyPeerSettings([]h3.Setting{{ID: h3.MaxFieldSectionSize, Value: 200}}); err != nil {
		t.Fatal(err)
	}
	workers.Wait()
}
