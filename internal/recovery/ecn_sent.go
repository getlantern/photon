package recovery

import (
	"github.com/getlantern/photon/internal/wire"
	"time"
)

type ecnTransmission struct {
	marking     ECNMark
	sentAt      time.Time
	currentPath bool
}

// ECNMark is an outgoing IP ECN codepoint. CE is a network congestion signal,
// not a marking the sender may originate.
type ECNMark uint8

const (
	NotECT ECNMark = 0
	ECT1   ECNMark = 1
	ECT0   ECNMark = 2
)

func (m ECNMark) index() int {
	if m == ECT0 {
		return 0
	}
	return 1
}

// CanMarkECN reports whether another marked packet can retain exact evidence
// until acknowledged. At capacity the owner can continue sending Not-ECT
// packets. Each space is bounded independently.
func (r *Sent) CanMarkECN(space Space) bool {
	return space <= Application && len(r.spaces[space].ecnPending) < r.capacity
}

// reclaimECN, at capacity, drops the evidence of this path's packets no
// longer tracked: declared lost, or ACK-only with their metadata evicted.
// Late ACKs of those are rare, and one that comes then reports more marked
// packets than it newly acknowledges, which feedback validation allows, so
// evidence for truly lost packets doesn't fill the ledger and end marking
// for good. Old-path evidence stays: it keeps the old path's late feedback
// from being attributed to this one. It scans only once a packet with
// evidence has left the ledger since the last scan.
func (r *Sent) reclaimECN(space Space) {
	if space > Application {
		return
	}
	s := &r.spaces[space]
	if len(s.ecnPending) < r.capacity || !s.ecnReclaimable {
		return
	}
	s.ecnReclaimable = false
	for pn, evidence := range s.ecnPending {
		if _, ok := s.packets[pn]; !ok && evidence.currentPath {
			delete(s.ecnPending, pn)
			s.ecnUntracked.remove(pn)
		}
	}
}

// ECNSent returns cumulative ECT(0)/ECT(1) transmissions in this recovery
// context, including lost and acknowledged packets, excluding local rejections.
// Discard and Retry clear the context. Migration preserves cumulative counts
// and pending marking evidence because the peer retains its counters. The owner
// must revalidate the new path separately without zeroing cumulative feedback.
func (r *Sent) ECNSent(space Space) [2]uint64 {
	if space > Application {
		return [2]uint64{}
	}
	return r.spaces[space].ecnSent
}

// EnableECN starts path testing after the owner establishes that its UDP writer
// supports per-datagram marking. Calling it again does not revive a failed path.
func (r *Sent) EnableECN() {
	if r.ecnPath.State() == ECNDisabled {
		r.ecnPath = *NewECNPath()
	}
}

func (r *Sent) ECNState() ECNPathState { return r.ecnPath.State() }

// ECNMarking selects a marking without advancing time. Production senders use
// ECNMarkingAt to enforce the testing deadline. The owner
// must put this exact marking in Packet.ECN and the outgoing IP datagram.
func (r *Sent) ECNMarking(space Space) ECNMark {
	r.reclaimECN(space)
	if !r.CanMarkECN(space) {
		return NotECT
	}
	return r.ecnPath.Marking()
}

// RejectECNMarking changes the latest committed packet to Not-ECT after a
// synchronous kernel rejection of its ancillary ECN data. No bytes may have
// left the host and no subsequent send/receive may have occurred. The owner may
// then send the identical ciphertext once without marking. Packet number,
// flight bytes, and frame delivery remain committed to that same transmission.
func (r *Sent) RejectECNMarking(space Space, number uint64) bool {
	if space > Application {
		return false
	}
	s := &r.spaces[space]
	evidence, ok := s.ecnPending[number]
	if !ok || !evidence.currentPath || number+1 != s.next {
		return false
	}
	s.ecnSent[evidence.marking.index()]--
	delete(s.ecnPending, number)
	s.ecnUntracked.remove(number)
	if packet, ok := s.packets[number]; ok {
		packet.ECN = NotECT
		s.packets[number] = packet
	}
	r.ecnPath.state = ECNFailed
	return true
}

// ECNMarkingAt bounds testing by three base PTO durations without exponential backoff measured
// from the first probe. The deadline is fixed even as PTO backoff grows, so a
// network dropping all ECT packets cannot consume the whole handshake timeout.
// Existing send/PTO scheduling supplies wakeups; this does not schedule traffic.
func (r *Sent) ECNMarkingAt(space Space, now time.Time) ECNMark {
	if r.ecnPath.count > 0 && !r.ecnTestUntil.IsZero() && !now.Before(r.ecnTestUntil) {
		r.ecnPath.endTesting()
	}
	return r.ECNMarking(space)
}

// ackECN is called only after ACK range/unsent validation. acked are the
// packets f newly acknowledges that recovery still tracks; marks of packets it
// no longer tracks are found through ecnUntracked by f's ranges. Neither
// enumerates a peer-supplied range or scans the retained evidence.
func (s *packetSpace) ackECN(frame wire.AckFrame, acked []Packet) (newly [2]uint64) {
	if len(s.ecnPending) == 0 {
		return
	}
	count := func(pn uint64) bool {
		if evidence, ok := s.ecnPending[pn]; ok {
			newly[evidence.marking.index()]++
			delete(s.ecnPending, pn)
		}
		return true
	}
	for _, p := range acked {
		count(p.Number)
	}
	s.ecnUntracked.covered(frame, count)
	return
}
