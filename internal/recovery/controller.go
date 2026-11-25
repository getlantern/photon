package recovery

import "time"

// Controller is one path's congestion controller. The owner asks it whether a
// packet fits (CanSend, Window) and reports what happens to sent packets.
// Calls are serialized by the owner.
type Controller interface {
	// Window is the congestion window in bytes, which also sets the pacing
	// rate.
	Window() uint64
	// CanSend reports whether a packet of size may be sent with flight bytes
	// already in flight. Packets that are not in flight, and probes, are
	// always allowed.
	CanSend(flight uint64, size int, inFlight, probe bool) bool
	// OnACK reports newly acknowledged packets, with the delivery rate their
	// acknowledgment measured.
	OnACK(ACKEvent)
	// OnLoss reports packets declared lost.
	OnLoss(packets []Packet, now time.Time)
	// OnECN reports an ECN-CE increase for a packet sent at sent.
	OnECN(sent, now time.Time)
	// OnPersistentCongestion reports persistent congestion (RFC 9002 section
	// 7.6).
	OnPersistentCongestion()
	// Reset returns the controller to its initial state for a new network
	// path (RFC 9000 section 9.4), in place, since the owner may share it
	// between views of the same path.
	Reset(maxDatagram uint64)
}

// ACKEvent is one ACK's newly acknowledged packets.
type ACKEvent struct {
	Acked []Packet
	// Rate is the delivery rate this ACK measured; Rate.Valid is false when
	// it measured none.
	Rate RateSample
	// Limited is true when the sender was application- or flow-control
	// limited, with room left in the window, when the ACK arrived.
	Limited bool
	Now     time.Time
}

var _ Controller = (*NewReno)(nil)
