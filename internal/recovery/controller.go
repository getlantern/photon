package recovery

import (
	"errors"
	"time"
)

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
	// PacingRate is the rate, in bytes per second, at which the controller
	// wants packets paced, or 0 to pace from Window and the smoothed RTT.
	PacingRate() uint64
	// OnPacketSent reports an in-flight packet as it is sent.
	OnPacketSent(SentEvent)
	// OnACK reports newly acknowledged packets, with the delivery rate their
	// acknowledgment measured.
	OnACK(ACKEvent)
	// OnLoss reports packets declared lost.
	OnLoss(LossEvent)
	// OnECN reports an ECN-CE increase for a packet sent at sent.
	OnECN(sent, now time.Time)
	// OnPersistentCongestion reports persistent congestion (RFC 9002 section
	// 7.6).
	OnPersistentCongestion()
	// ForcesAppLimited reports whether the controller is holding the
	// sender below what the path can carry, as BBR's ProbeRTT does, so the
	// owner marks the path application limited and its rate samples are
	// known to understate the path.
	ForcesAppLimited() bool
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
	// Flight is the bytes in flight once the ACK, and any loss it declared,
	// has been processed.
	Flight uint64
	Now    time.Time
}

// SentEvent is one in-flight packet as it is sent.
type SentEvent struct {
	Bytes int
	// Flight is the bytes in flight before the packet, and AppLimited
	// whether the path was application limited, when it was sent.
	Flight     uint64
	AppLimited bool
	Now        time.Time
}

// LossEvent is one detection of lost packets.
type LossEvent struct {
	Lost []Packet
	// TotalLost is the path's lost in-flight bytes, these included, which
	// with each packet's Delivery.Lost gives the loss since it was sent. MTU
	// probes are not counted: their loss is not congestion.
	TotalLost uint64
	// TotalDelivered is the path's delivered in-flight bytes.
	TotalDelivered uint64
	// Flight is the bytes in flight once these packets are removed.
	Flight uint64
	Now    time.Time
}

var _ Controller = (*NewReno)(nil)

// Algorithm selects a congestion controller.
type Algorithm uint8

const (
	// NewRenoAlgorithm is RFC 9002's NewReno, the default.
	NewRenoAlgorithm Algorithm = iota
	// BBRAlgorithm is BBRv3, as in draft-ietf-ccwg-bbr-07.
	BBRAlgorithm
)

// NewController returns a controller of algorithm a for maxDatagram-byte
// packets.
func NewController(a Algorithm, maxDatagram uint64) (Controller, error) {
	if maxDatagram < 1200 || maxDatagram > 65527 {
		return nil, errors.New("quic: invalid congestion datagram size")
	}
	switch a {
	case BBRAlgorithm:
		return NewBBR(maxDatagram), nil
	case NewRenoAlgorithm:
	default:
		return nil, errors.New("quic: unknown congestion control algorithm")
	}
	c, err := NewCongestion(maxDatagram)
	if err != nil {
		return nil, err
	}
	return c, nil
}
