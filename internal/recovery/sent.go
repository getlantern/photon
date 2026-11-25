package recovery

import (
	"cmp"
	"errors"
	"slices"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

type Space uint8

const (
	Initial Space = iota
	Handshake
	Application
)

var ErrACK = errors.New("quic: ACK acknowledges an unsent packet or has invalid ranges")
var ErrCapacity = errors.New("quic: sent packet recovery capacity exceeded")

type Packet struct {
	ECN                    ECNMark // outgoing IP marking; zero means Not-ECT
	Number                 uint64
	SentAt                 time.Time
	Bytes                  int
	AckEliciting, InFlight bool
	MTUProbe               bool // dedicated padded size probe; loss is not congestion evidence
	// Delivery is set by Sent.OnSent for in-flight packets.
	Delivery DeliveryState
}
type Events struct {
	Acked, Lost []Packet
	// RecoveredPackets and RecoveredBytes report newly ACKed transmissions
	// previously declared lost, within the bounded congestion-history horizon.
	// They do not re-enter Acked or change bytes in flight. Discarded/evicted
	// history cannot produce corrections; duplicate ACKs report zero.
	RecoveredPackets, RecoveredBytes uint64
	// NewlyECT includes late ACKs of packets already declared lost, exactly once.
	NewlyECT [2]uint64
	// ECN is checked only after ACK range and ACK-of-unsent validation.
	// Failure disables marking; it is not a connection-level ACK error.
	ECN ECNFeedbackResult
	// ECNCongestion identifies a validated CE increase with a send timestamp
	// from the current network path, suitable for congestion recovery epochs.
	ECNCongestion bool
	ECNSentAt     time.Time
	// Rate is the delivery rate the ACK measured.
	Rate RateSample
}
type packetSpace struct {
	ecnFeedback       ECNFeedback
	ecnSent           [2]uint64
	ecnPending        map[uint64]ecnTransmission
	ackOnly           ackOnlyOrder
	ackNodes          map[uint64]*ackOnlyNode
	packets           map[uint64]Packet
	rejected          map[uint64]struct{} // locally rejected packets; never ACKable
	next              uint64
	largestAcked      uint64
	hasAck            bool
	lossTime          time.Time
	lastEliciting     time.Time
	previousEliciting time.Time // timestamp before the latest eliciting commit
}

// Sent tracks packets, not frame delivery. Multiple packet transmissions can
// carry the same CRYPTO/STREAM data; frame retirement belongs to the engine.
// Instances represent one path; packet-number spaces share RTT estimates.
type Sent struct {
	ecnTestUntil  time.Time
	ecnPath       ECNPath
	history       congestionHistory
	rate          deliveryRate
	RTT           RTT
	spaces        [3]packetSpace
	BytesInFlight uint64
	capacity      int
}

func NewSent(capacity int) (*Sent, error) {
	if capacity < 1 || capacity > 65536 {
		return nil, errors.New("quic: invalid sent packet capacity")
	}
	r := &Sent{RTT: NewRTT(), capacity: capacity, history: congestionHistory{capacity: 6 * capacity}}
	for i := range r.spaces {
		r.spaces[i].packets = make(map[uint64]Packet)
		r.spaces[i].ackNodes = make(map[uint64]*ackOnlyNode)
	}
	return r, nil
}
func (r *Sent) CanTrack(space Space) bool {
	return space <= Application && len(r.spaces[space].packets) < r.capacity
}

// CanRecord reserves full metadata for in-flight packets. Pure ACK history is
// expendable: forgetting a timestamp may omit an RTT sample, but never changes
// flight accounting or validation against the monotonically increasing next PN.
func (r *Sent) CanRecord(space Space, inFlight bool) bool {
	return space <= Application && (!inFlight || r.CanTrack(space) || r.spaces[space].ackOnly.Len() > 0)
}

// OnSent requires contiguous packet numbers starting at zero in each space.
// This makes ACK-of-unsent validation exact; intentional packet-number skipping
// requires a separate skipped-number ledger before it can be supported.
func (r *Sent) OnSent(space Space, p Packet) error {
	if space > Application || p.ECN > ECT0 || p.Number > wire.MaxVarint || p.Bytes < 0 || p.Bytes > 65535 || (p.AckEliciting && !p.InFlight) || (p.MTUProbe && (space != Application || !p.AckEliciting || p.Bytes < 1200)) {
		return errors.New("quic: invalid sent packet")
	}
	s := &r.spaces[space]
	if p.Number != s.next {
		return errors.New("quic: noncontiguous packet number")
	}
	if !r.CanRecord(space, p.InFlight) || (p.ECN != NotECT && (!r.CanMarkECN(space) || s.ecnSent[p.ECN.index()] == wire.MaxVarint)) {
		return ErrCapacity
	}
	if !r.CanTrack(space) && s.ackOnly.Len() > 0 {
		old := s.ackOnly.Front().number
		r.remove(s, s.packets[old])
	}
	if p.InFlight {
		r.rate.stamp(&p, r.BytesInFlight)
	}
	// A full in-flight ledger must not prevent pure congestion feedback.
	if r.CanTrack(space) {
		s.packets[p.Number] = p
		if !p.InFlight {
			s.ackNodes[p.Number] = s.ackOnly.PushBack(p.Number)
		}
	}
	if p.ECN != NotECT {
		if s.ecnPending == nil {
			s.ecnPending = make(map[uint64]ecnTransmission)
		}
		s.ecnPending[p.Number] = ecnTransmission{marking: p.ECN, sentAt: p.SentAt, currentPath: true}
		s.ecnSent[p.ECN.index()]++
	}
	if p.ECN == ECT0 {
		if r.ecnPath.State() == ECNTesting && r.ecnPath.count == 0 {
			r.ecnTestUntil = p.SentAt.Add(multiply(r.RTT.PTO(false, false, 0, 0), 3))
		}
		r.ecnPath.Sent(space, p.Number)
	}
	r.history.sent(space, p, r.RTT.Sampled)
	s.next++
	if p.AckEliciting {
		s.previousEliciting = s.lastEliciting
		s.lastEliciting = p.SentAt
	}
	if p.InFlight {
		r.BytesInFlight += uint64(p.Bytes)
	}
	return nil
}
func validACK(f wire.AckFrame) bool {
	if len(f.Ranges) == 0 {
		return false
	}
	for i, a := range f.Ranges {
		if a.Smallest > a.Largest || a.Largest > wire.MaxVarint {
			return false
		}
		if i > 0 && f.Ranges[i-1].Smallest < a.Largest+2 {
			return false
		}
	}
	return true
}
func order(p []Packet) {
	slices.SortFunc(p, func(a, b Packet) int { return cmp.Compare(a.Number, b.Number) })
}
func (r *Sent) remove(s *packetSpace, p Packet) {
	delete(s.packets, p.Number)
	if node := s.ackNodes[p.Number]; node != nil {
		s.ackOnly.Remove(node)
		delete(s.ackNodes, p.Number)
	}
	if p.InFlight {
		r.BytesInFlight -= uint64(p.Bytes)
	}
}

// OnACK samples only the ACK frame's largest newly acknowledged packet and
// only when some newly acknowledged packet was ack-eliciting (RFC 9002 5.1).
// It returns newly acknowledged and newly lost transmissions exactly once.
func (r *Sent) OnACK(space Space, f wire.AckFrame, now time.Time, delay, maxDelay time.Duration, confirmed bool) (Events, error) {
	var events Events
	if space > Application || !validACK(f) {
		return events, ErrACK
	}
	s := &r.spaces[space]
	for pn := range s.rejected {
		if f.Acknowledges(pn) {
			return events, ErrACK
		}
	}
	largest := f.Ranges[0].Largest
	if largest >= s.next {
		return events, ErrACK
	}
	largestPacket, largestNew := s.packets[largest]
	ecnPacket, haveECNPacket := s.ecnPending[largest]
	events.NewlyECT = s.ackECN(f)
	events.ECN = s.ecnFeedback.Check(largest, f.ECN, s.ecnSent, events.NewlyECT)
	r.ecnPath.ACK(space, f, events.ECN)
	if events.ECN.CE > 0 {
		if largestNew {
			events.ECNCongestion = true
			events.ECNSentAt = largestPacket.SentAt
		} else if haveECNPacket && ecnPacket.currentPath {
			events.ECNCongestion = true
			events.ECNSentAt = ecnPacket.sentAt
		}
	}
	events.RecoveredPackets, events.RecoveredBytes = r.history.ack(space, f)
	// Allocate only for newly acknowledged, retained packets. Counting first
	// avoids geometric slice growth for cumulative ACKs without reserving from
	// peer-controlled range widths (or allocating for duplicate ACKs).
	lookup := useACKLookup(f, len(s.packets))
	count := 0
	if lookup {
		for _, span := range f.Ranges {
			for pn := span.Smallest; pn <= span.Largest; pn++ {
				if _, ok := s.packets[pn]; ok {
					count++
				}
			}
		}
	} else {
		for pn := range s.packets {
			if f.Acknowledges(pn) {
				count++
			}
		}
	}
	if count > 0 {
		events.Acked = make([]Packet, 0, count)
	}
	eliciting := false
	var sampler rateSampler
	retire := func(p Packet) {
		r.rate.acked(&sampler, p, now)
		events.Acked = append(events.Acked, p)
		eliciting = eliciting || p.AckEliciting
		r.remove(s, p)
	}
	if lookup {
		// Wire ranges descend. Visit them in reverse so retirement produces
		// ascending packet numbers directly, including disjoint ranges.
		for i := len(f.Ranges) - 1; i >= 0; i-- {
			span := f.Ranges[i]
			for pn := span.Smallest; pn <= span.Largest; pn++ {
				if p, ok := s.packets[pn]; ok {
					retire(p)
				}
			}
		}
	} else {
		for pn, p := range s.packets {
			if f.Acknowledges(pn) {
				retire(p)
			}
		}
	}
	if largestNew && eliciting {
		if space != Application {
			delay = 0
		}
		r.RTT.Update(now.Sub(largestPacket.SentAt), delay, maxDelay, confirmed)
	}
	events.Rate = r.rate.sample(&sampler, now, r.RTT.Minimum, r.RTT.Sampled)
	if !s.hasAck || largest > s.largestAcked {
		s.largestAcked = largest
		s.hasAck = true
	}
	events.Lost = r.DetectLoss(space, now)
	if !lookup {
		order(events.Acked)
	}
	return events, nil
}

// DetectLoss implements packet threshold 3 and time threshold 9/8, and records
// the next time-threshold deadline. Unacknowledged tail packets require PTO;
// elapsed time alone never marks them lost without a later acknowledged packet.
func (r *Sent) DetectLoss(space Space, now time.Time) []Packet {
	if space > Application {
		return nil
	}
	s := &r.spaces[space]
	s.lossTime = time.Time{}
	if !s.hasAck {
		return nil
	}
	var lost []Packet
	delay := r.RTT.LossDelay()
	for pn, p := range s.packets {
		if pn >= s.largestAcked {
			continue
		}
		at := p.SentAt.Add(delay)
		if s.largestAcked-pn >= 3 || !at.After(now) {
			lost = append(lost, p)
			r.remove(s, p)
		} else if s.lossTime.IsZero() || at.Before(s.lossTime) {
			s.lossTime = at
		}
	}
	order(lost)
	for _, p := range lost {
		r.ecnPath.Lost(space, p.Number)
	}
	r.history.lost(space, lost)
	return lost
}
func (r *Sent) LossTime() (time.Time, Space, bool) {
	var at time.Time
	var space Space
	for i, s := range r.spaces {
		if !s.lossTime.IsZero() && (at.IsZero() || s.lossTime.Before(at)) {
			at = s.lossTime
			space = Space(i)
		}
	}
	return at, space, !at.IsZero()
}

// Discard removes a space's recovery state without treating its packets as loss.
// It is called at the encryption-level retirement milestone.
func (r *Sent) Discard(space Space) {
	if space > Application {
		return
	}
	r.history.discard(space)
	s := &r.spaces[space]
	for _, p := range s.packets {
		r.remove(s, p)
	}
	s.lossTime = time.Time{}
	for i := r.ecnPath.count - 1; i >= 0; i-- {
		probe := r.ecnPath.probes[i]
		if probe.space == space {
			r.ecnPath.forget(space, probe.number)
		}
	}
	s.ecnFeedback = ECNFeedback{}
	s.ecnPending = nil
	s.ecnSent = [2]uint64{}
}

// ResetForRetry forgets recovery evidence and timers but preserves packet-number
// continuity. A Retry is not an acknowledgment of any individual packet.
func (r *Sent) ResetForRetry() {
	previousECN := r.ecnPath.State()
	var rejected [3]map[uint64]struct{}
	var next [3]uint64
	for i := range r.spaces {
		next[i] = r.spaces[i].next
		rejected[i] = r.spaces[i].rejected
	}
	fresh, _ := NewSent(r.capacity)
	*r = *fresh
	if previousECN != ECNDisabled {
		r.EnableECN()
	}
	if previousECN == ECNFailed {
		r.ecnPath.state = ECNFailed
	}
	for i := range r.spaces {
		r.spaces[i].next = next[i]
		r.spaces[i].rejected = rejected[i]
	}
}

// Abandon returns retained outstanding transmissions as lost and releases their
// recovery metadata. Unlike key discard, the owner must recover reliable frame
// data on another path. Packet-number continuity is never reset by this method.
func (r *Sent) Abandon(space Space) []Packet {
	if space > Application {
		return nil
	}
	lost := make([]Packet, 0, len(r.spaces[space].packets))
	for _, p := range r.spaces[space].packets {
		lost = append(lost, p)
	}
	order(lost)
	r.Discard(space)
	return lost
}

// ResetForPathChange starts a new network recovery context after migration.
// Outstanding transmissions are returned for retransmission by the frame owner;
// they are not congestion-loss events. Packet-number continuity and ACK-of-unsent
// validation are retained. Late ACKs for old packets cannot provide RTT samples,
// flight accounting, or congestion events in the new context.
func (r *Sent) ResetForPathChange() [3][]Packet {
	var outstanding [3][]Packet
	// The receiver does not reset cumulative ECN counts when this path changes.
	// Keep marking evidence for late ACKs, independently of flight/RTT history.
	var ecnFeedback [3]ECNFeedback
	var ecnSent [3][2]uint64
	var ecnPending [3]map[uint64]ecnTransmission
	for i := range r.spaces {
		for pn, evidence := range r.spaces[i].ecnPending {
			evidence.currentPath = false
			r.spaces[i].ecnPending[pn] = evidence
		}
		ecnFeedback[i] = r.spaces[i].ecnFeedback
		ecnSent[i], ecnPending[i] = r.spaces[i].ecnSent, r.spaces[i].ecnPending
		outstanding[i] = r.Abandon(Space(i))
	}
	r.ResetForRetry()
	for i := range r.spaces {
		r.spaces[i].ecnFeedback = ecnFeedback[i]
		r.spaces[i].ecnSent, r.spaces[i].ecnPending = ecnSent[i], ecnPending[i]
	}
	return outstanding
}

// RejectMTUProbe cancels the latest committed application packet after a local
// EMSGSIZE rejection. The owner must call it synchronously before another send
// or receive. It preserves the packet number and rejects future ACKs for it.
// A bounded ledger exhaustion returns false so the owner can fail the connection.
func (r *Sent) RejectMTUProbe(number uint64) bool {
	return r.rejectLocalPacket(number, true, false)
}

// RejectLargePacket cancels a locally rejected oversized ordinary application
// packet under the same synchronous send-result contract as RejectMTUProbe.
func (r *Sent) RejectLargePacket(number uint64) bool {
	return r.rejectLocalPacket(number, false, true)
}

// RejectMigrationProbe cancels a latest ordinary application packet that the
// owner identifies as a migration probe rejected with zero bytes written. The
// owner must prevent subsequent sends until completion. Failure requires the
// caller to retain its ordinary fatal-error policy; packet numbers are never reused.
func (r *Sent) RejectMigrationProbe(number uint64) bool {
	return r.rejectLocalPacket(number, false, false)
}

func (r *Sent) rejectLocalPacket(number uint64, probe, requireLarge bool) bool {
	s := &r.spaces[Application]
	p, ok := s.packets[number]
	if !ok || p.MTUProbe != probe || !p.AckEliciting || !p.InFlight || (requireLarge && p.Bytes <= 1200) || number+1 != s.next || len(s.rejected) >= r.capacity {
		return false
	}
	if s.rejected == nil {
		s.rejected = make(map[uint64]struct{})
	}
	s.rejected[number] = struct{}{}
	if marking, ok := s.ecnPending[number]; ok {
		s.ecnSent[marking.marking.index()]--
		r.ecnPath.forget(Application, number)
		delete(s.ecnPending, number)
	}
	r.remove(s, p)
	if e := r.history.index[transmissionID{Application, number}]; e != nil {
		delete(r.history.index, transmissionID{Application, number})
		r.history.order.Remove(e)
	}
	s.lastEliciting = s.previousEliciting
	// The newest packet cannot contribute to lossTime: no larger packet has
	// been acknowledged before this synchronous send-result callback.
	return true
}
