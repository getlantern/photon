package http3conn

import (
	"fmt"
	"io"
	"sync"

	h3 "github.com/getlantern/photon/internal/http3wire"
	"github.com/getlantern/photon/internal/qpack"
)

type EncoderLimits struct {
	Capacity                                     uint64
	MaxBlocked, MaxSections, MaxReferences       int
	MaxEncodedSection, MaxFields, MaxInstruction int
	MaxFieldSection                              uint64
}

// FieldEncoder owns a connection's QPACK encoder and its critical-stream writes.
// Construct once from live settings or ticket-bound remembered settings. The writer is the local encoder
// stream after its type prefix; it must be exclusive and have a bounded deadline
// or connection cancellation that interrupts writes. No callbacks may reenter.
// Calls are serialized. Field sections on each request stream must additionally
// be encoded and sent in order by that stream's owner. Any constructor/write or
// feedback failure requires connection termination; uncertain writes are never
// retried. Field validation and HTTP message semantics are separate.
type FieldEncoder struct {
	policy EncoderLimits
	early  *RememberedSettings
	mu     sync.Mutex
	table  *qpack.EncoderTable
	writer io.Writer
	limits EncoderLimits
	err    error
}

func NewFieldEncoder(peer SettingsSnapshot, limits EncoderLimits, writer io.Writer) (*FieldEncoder, error) {
	if writer == nil || limits.Capacity > qpack.MaxInteger || limits.MaxFieldSection > qpack.MaxInteger || limits.MaxBlocked < 0 || limits.MaxSections < 0 || limits.MaxReferences < 0 || limits.MaxEncodedSection < 0 || limits.MaxFields < 0 || limits.MaxInstruction < 0 {
		return nil, fmt.Errorf("http3: invalid encoder limits or writer")
	}
	policy := limits
	blocked := limits.MaxBlocked
	if peer.QPACKBlockedStreams < uint64(blocked) {
		blocked = int(peer.QPACKBlockedStreams)
	}
	// Required Insert Count uses the peer's advertised maximum, not our smaller
	// chosen capacity. Clamping the maximum here would corrupt wraparound coding.
	table, err := qpack.NewEncoderTable(peer.QPACKCapacity, blocked, limits.MaxSections, limits.MaxReferences)
	if err != nil {
		return nil, err
	}
	limits.Capacity = min(limits.Capacity, peer.QPACKCapacity)
	limits.MaxFieldSection = min(limits.MaxFieldSection, peer.MaxFieldSectionSize)
	e := &FieldEncoder{table: table, writer: writer, limits: limits, policy: policy}
	if limits.Capacity != 0 {
		update, err := table.Apply(qpack.EncoderInstruction{Type: qpack.SetDynamicCapacity, Index: limits.Capacity})
		if err != nil {
			return nil, err
		}
		if err = e.write(update); err != nil {
			return nil, err
		}
	}
	return e, nil
}

// Encode retains successful dynamic references until decoder feedback arrives.
// A rejected section has no table effect. Insertion is deliberately opt-in.
func (e *FieldEncoder) Encode(streamID uint64, fields []qpack.DecodedField) ([]byte, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.err != nil {
		return nil, e.err
	}
	return e.table.EncodeFields(streamID, fields, e.limits.MaxEncodedSection, e.limits.MaxFieldSection, e.limits.MaxFields)
}

// Insert writes each committed update before another operation can use it.
// NeverIndex and entries that cannot fit safely fall back without insertion.
func (e *FieldEncoder) Insert(field qpack.DecodedField) (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.err != nil {
		return false, e.err
	}
	update, inserted, err := e.table.InsertField(field, e.limits.MaxInstruction)
	if err != nil || !inserted {
		return false, err
	}
	if err = e.write(update); err != nil {
		return false, err
	}
	return true, nil
}
func (e *FieldEncoder) Feedback(instruction qpack.DecoderInstruction) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.err != nil {
		return e.err
	}
	if err := e.table.ApplyFeedback(instruction); err != nil {
		e.err = &h3.Error{Code: QPACKDecoderStreamError, Reason: err.Error()}
	}
	return e.err
}
func (e *FieldEncoder) write(update []byte) error {
	n, err := e.writer.Write(update)
	if err == nil && n != len(update) {
		err = io.ErrShortWrite
	}
	if err != nil {
		e.err = err
	}
	return err
}

// NewFieldEncoder constructs the encoder from validated peer SETTINGS. The owner
// calls it once; construction before SETTINGS or after a settings error fails
// without touching the writer. This API does not send with speculative defaults.
func (s *SettingsState) NewFieldEncoder(limits EncoderLimits, writer io.Writer) (*FieldEncoder, error) {
	s.mu.RLock()
	peer, ready, err := s.peer, s.received, s.err
	s.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	if !ready {
		return nil, fmt.Errorf("http3: peer settings not received")
	}
	return NewFieldEncoder(peer, limits, writer)
}

// NewEarlyFieldEncoder initializes a fresh QPACK table with remembered peer
// limits. It never restores dynamic entries from another connection. The owner
// binds remembered settings to the ticket and reconstructs the session on rejection.
func NewEarlyFieldEncoder(remembered []h3.Setting, limits EncoderLimits, writer io.Writer) (*FieldEncoder, error) {
	r, err := RememberSettings(remembered)
	if err != nil {
		return nil, err
	}
	e, err := NewFieldEncoder(r.snapshot, limits, writer)
	if err != nil {
		return nil, err
	}
	e.early = &r
	return e, nil
}

// ApplyPeerSettings promotes this early encoder after accepted 0-RTT, preserving
// inserted entries, outstanding sections and decoder feedback state. A failure
// is terminal; the owner must close the connection. Invoke once for live SETTINGS.
func (e *FieldEncoder) ApplyPeerSettings(settings []h3.Setting) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.err != nil {
		return e.err
	}
	if e.early == nil {
		e.err = &h3.Error{Code: h3.FrameUnexpected, Reason: "unexpected encoder SETTINGS"}
		return e.err
	}
	peer, err := settingsSnapshot(settings)
	if err == nil {
		err = e.early.validate(peer, settings)
	}
	if err != nil {
		e.err = err
		return err
	}
	blocked := e.policy.MaxBlocked
	if peer.QPACKBlockedStreams < uint64(blocked) {
		blocked = int(peer.QPACKBlockedStreams)
	}
	if err = e.table.ApplyEarlyLimits(peer.QPACKCapacity, blocked); err != nil {
		e.err = err
		return err
	}
	capacity := min(e.policy.Capacity, peer.QPACKCapacity)
	if capacity != e.limits.Capacity {
		update, err := e.table.Apply(qpack.EncoderInstruction{Type: qpack.SetDynamicCapacity, Index: capacity})
		if err != nil {
			e.err = err
			return err
		}
		if err = e.write(update); err != nil {
			return err
		}
	}
	e.limits.Capacity = capacity
	e.limits.MaxFieldSection = min(e.policy.MaxFieldSection, peer.MaxFieldSectionSize)
	e.early = nil
	return nil
}
