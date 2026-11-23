package wire

// Draft-ietf-quic-multipath-21 section 4. Path IDs remain uint64 at the wire
// boundary so the engine can report PROTOCOL_VIOLATION for out-of-limit IDs,
// rather than incorrectly turning syntactically valid frames into encoding errors.
const (
	FramePathAck                uint64 = 0x3e
	FramePathAckECN             uint64 = 0x3f
	FramePathAbandon            uint64 = 0x3e75
	FramePathStatusBackup       uint64 = 0x3e76
	FramePathStatusAvailable    uint64 = 0x3e77
	FramePathNewConnectionID    uint64 = 0x3e78
	FramePathRetireConnectionID uint64 = 0x3e79
	FrameMaxPathID              uint64 = 0x3e7a
	FramePathsBlocked           uint64 = 0x3e7b
	FramePathCIDsBlocked        uint64 = 0x3e7c
)

func multipathFrameType(typ uint64) bool {
	return typ == FramePathAck || typ == FramePathAckECN || (typ >= FramePathAbandon && typ <= FramePathCIDsBlocked)
}

type PathAckFrame struct {
	PathID uint64
	AckFrame
}

func (f PathAckFrame) Type() uint64 {
	if f.ECN != nil {
		return FramePathAckECN
	}
	return FramePathAck
}
func (f PathAckFrame) Append(b []byte) ([]byte, error) {
	return f.AckFrame.appendPrefix(b, f.Type(), f.PathID)
}

type PathAbandonFrame struct{ PathID, ErrorCode uint64 }

func (PathAbandonFrame) Type() uint64 { return FramePathAbandon }
func (f PathAbandonFrame) Append(b []byte) ([]byte, error) {
	return appendFields(b, f.Type(), f.PathID, f.ErrorCode)
}

type PathStatusFrame struct {
	PathID, Sequence uint64
	Backup           bool
}

func (f PathStatusFrame) Type() uint64 {
	if f.Backup {
		return FramePathStatusBackup
	}
	return FramePathStatusAvailable
}
func (f PathStatusFrame) Append(b []byte) ([]byte, error) {
	return appendFields(b, f.Type(), f.PathID, f.Sequence)
}

type PathNewConnectionIDFrame struct {
	PathID uint64
	NewConnectionIDFrame
}

func (PathNewConnectionIDFrame) Type() uint64 { return FramePathNewConnectionID }
func (f PathNewConnectionIDFrame) Append(b []byte) ([]byte, error) {
	return f.NewConnectionIDFrame.appendPrefix(b, f.Type(), f.PathID)
}

type PathRetireConnectionIDFrame struct{ PathID, Sequence uint64 }

func (PathRetireConnectionIDFrame) Type() uint64 { return FramePathRetireConnectionID }
func (f PathRetireConnectionIDFrame) Append(b []byte) ([]byte, error) {
	return appendFields(b, f.Type(), f.PathID, f.Sequence)
}

type MaxPathIDFrame struct{ Maximum uint64 }

func (MaxPathIDFrame) Type() uint64                      { return FrameMaxPathID }
func (f MaxPathIDFrame) Append(b []byte) ([]byte, error) { return appendFields(b, f.Type(), f.Maximum) }

type PathsBlockedFrame struct{ Maximum uint64 }

func (PathsBlockedFrame) Type() uint64 { return FramePathsBlocked }
func (f PathsBlockedFrame) Append(b []byte) ([]byte, error) {
	return appendFields(b, f.Type(), f.Maximum)
}

type PathCIDsBlockedFrame struct{ PathID, NextSequence uint64 }

func (PathCIDsBlockedFrame) Type() uint64 { return FramePathCIDsBlocked }
func (f PathCIDsBlockedFrame) Append(b []byte) ([]byte, error) {
	return appendFields(b, f.Type(), f.PathID, f.NextSequence)
}
