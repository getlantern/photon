package http3wire

import (
	"errors"
	"io"

	"github.com/getlantern/photon/internal/wire"
)

const (
	ClosedCriticalStream uint64 = 0x104
	IDError              uint64 = 0x108
	MissingSettings      uint64 = 0x10a
)

type StreamKind uint8

const (
	ControlStream StreamKind = iota
	RequestStream
	PushStream
)

// ValidatePlacement applies RFC 9114's frame/stream table and sender roles.
// Unknown extensions remain permitted; their semantics need separate handling.
// SETTINGS ordering and message ordering require per-stream state.
func ValidatePlacement(kind StreamKind, peerIsServer bool, typ uint64) error {
	allowed := true
	switch typ {
	case 2, 6, 8, 9:
		allowed = false
	case Data, Headers:
		allowed = kind == RequestStream || kind == PushStream
	case Settings, CancelPush, Goaway:
		allowed = kind == ControlStream
	case PriorityUpdateRequest, PriorityUpdatePush:
		allowed = kind == ControlStream && !peerIsServer
	case MaxPushID:
		allowed = kind == ControlStream && !peerIsServer
	case PushPromise:
		allowed = kind == RequestStream && peerIsServer
	}
	if !allowed {
		return &Error{FrameUnexpected, "frame not permitted on this stream or from this sender"}
	}
	return nil
}

// ControlEvent is one fully validated control frame. CancelPush identifiers
// still need validation against connection-wide push state at the endpoint.
type ControlEvent struct {
	Type          uint64
	Settings      []Setting
	ID            uint64
	Priority      Priority
	PriorityValid bool
}

// ControlReader consumes a control stream after its stream-type prefix. It
// requires SETTINGS first, bounds their buffering, skips unknown frames with
// constant scratch space, and tracks GOAWAY/MAX_PUSH_ID monotonicity. The
// connection owner must enforce one control stream per peer and translate reset
// errors to critical-stream closure; source errors other than EOF are preserved.
// Reads can block; stream deadlines and cancellation belong to the source.
type ControlReader struct {
	frames                               *Reader
	peerIsServer                         bool
	maxSettingsBytes, maxSettingsEntries int
	settings                             bool
	goaway, maxPush                      uint64
	hasGoaway, hasMaxPush                bool
	err                                  error
	scratch                              [4096]byte
}

func NewControlReader(source io.Reader, peerIsServer bool, maxSettingsBytes, maxSettingsEntries int) (*ControlReader, error) {
	if source == nil || maxSettingsBytes < 0 || maxSettingsEntries < 0 {
		return nil, errors.New("http3: invalid control reader limits or source")
	}
	return &ControlReader{frames: NewReader(source), peerIsServer: peerIsServer, maxSettingsBytes: maxSettingsBytes, maxSettingsEntries: maxSettingsEntries}, nil
}
func (r *ControlReader) fail(err error) (ControlEvent, error) {
	if errors.Is(err, io.EOF) {
		err = &Error{ClosedCriticalStream, "control stream closed"}
	}
	r.err = err
	return ControlEvent{}, err
}
func (r *ControlReader) Next() (ControlEvent, error) {
	if r.err != nil {
		return ControlEvent{}, r.err
	}
	for {
		h, err := r.frames.Next()
		if err != nil {
			return r.fail(err)
		}
		if !r.settings && h.Type != Settings {
			return r.fail(&Error{MissingSettings, "control stream must begin with SETTINGS"})
		}
		if err := ValidatePlacement(ControlStream, r.peerIsServer, h.Type); err != nil {
			return r.fail(err)
		}
		switch h.Type {
		case Settings:
			if r.settings {
				return r.fail(&Error{FrameUnexpected, "repeated SETTINGS"})
			}
			if h.Length > uint64(r.maxSettingsBytes) {
				return r.fail(&Error{ExcessiveLoad, "SETTINGS payload limit"})
			}
			payload, err := io.ReadAll(r.frames)
			if err != nil {
				return r.fail(err)
			}
			settings, err := ParseSettings(payload, r.maxSettingsEntries)
			if err != nil {
				return r.fail(err)
			}
			r.settings = true
			return ControlEvent{Type: Settings, Settings: settings}, nil
		case PriorityUpdateRequest, PriorityUpdatePush:
			// Priority signals are small; impose an independent allocation bound
			// before reading a peer-controlled length. The limit includes the ID.
			if h.Length > 4096+8 {
				return r.fail(&Error{ExcessiveLoad, "PRIORITY_UPDATE payload limit"})
			}
			payload, err := io.ReadAll(r.frames)
			if err != nil {
				return r.fail(err)
			}
			id, n, err := wire.ConsumeVarint(payload)
			if err != nil {
				return r.fail(&Error{FrameError, "truncated PRIORITY_UPDATE identifier"})
			}
			if h.Type == PriorityUpdateRequest && id%4 != 0 {
				return r.fail(&Error{IDError, "priority target is not a request stream"})
			}
			priority, valid := ParsePriority(string(payload[n:]))
			return ControlEvent{Type: h.Type, ID: id, Priority: priority, PriorityValid: valid}, nil
		case CancelPush, Goaway, MaxPushID:
			if h.Length == 0 || h.Length > 8 {
				return r.fail(&Error{FrameError, "invalid identifier payload length"})
			}
			var payload [8]byte
			if _, err := io.ReadFull(r.frames, payload[:int(h.Length)]); err != nil {
				return r.fail(err)
			}
			id, err := ParseID(payload[:int(h.Length)])
			if err != nil {
				return r.fail(err)
			}
			switch h.Type {
			case Goaway:
				if (r.peerIsServer && id%4 != 0) || (r.hasGoaway && id > r.goaway) {
					return r.fail(&Error{IDError, "invalid or increasing GOAWAY identifier"})
				}
				r.goaway, r.hasGoaway = id, true
			case MaxPushID:
				if r.hasMaxPush && id < r.maxPush {
					return r.fail(&Error{IDError, "decreasing MAX_PUSH_ID"})
				}
				r.maxPush, r.hasMaxPush = id, true
			}
			return ControlEvent{Type: h.Type, ID: id}, nil
		default:
			if _, err := io.CopyBuffer(io.Discard, r.frames, r.scratch[:]); err != nil {
				return r.fail(err)
			}
		}
	}
}
