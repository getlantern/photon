package transport

import (
	"errors"
	"time"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
)

// pathApplication owns a path's application packet-number, ACK, loss, RTT,
// congestion and PTO state. Initial/Handshake remain connection-level spaces.
// TLS keys and AEAD usage limits remain shared across paths. The owner maps
// reliable frames to (path ID, packet number), including cross-path retransmits.
type pathApplication struct {
	routeBlocked     bool // source publication/capacity wakeup, not a pacing deadline
	lastSentDatagram bool // latest committed packet; no payload retained

	candidateChallenges  *Received
	candidateCIDControls *Received
	controlResponses     pathResponseQueue
	mtuChallenge         *wire.PathChallengeFrame // one bounded baseline control slot
	mtuNeedsValidation   bool                     // fallback requires fresh expanded path validation
	mtuBlackhole         mtuBlackholeState
	mtuRaiseAt           time.Time  // delayed upward recheck after a completed lower search
	mtuTimeoutAt         time.Time  // discovery ACK wait; independent of recovery loss
	mtuNextProbe         time.Time  // minimum spacing for staged discovery sends
	mtu                  *mtuSearch // nil until discovery is explicitly enabled by the owner
	// Fixed per-path response slot transferred from a committed migration.
	migrationResponse *wire.PathResponseFrame
	migration         *migrationSelection
	localMigration    *migrationCandidate
	network           *pathTuple
	probePending      bool
	evacuated         bool // sent items have surviving owners; the map only serves late ACKs
	pacingAt          time.Time
	validation        *pathValidation
	budget            *Amplification
	keyNumbers        keyPhaseNumbers
	space             *numberSpace
	sent              *recovery.Sent
	congestion        recovery.Controller
	timer             *recovery.Timer
	pacer             *recovery.Pacer
}

func newPathApplication(algorithm recovery.Algorithm) *pathApplication {
	received, _ := NewReceived(128)
	sent, _ := recovery.NewSent(maxSentPackets)
	congestion, _ := recovery.NewController(algorithm, 1200)
	return &pathApplication{validation: new(pathValidation), budget: new(Amplification), space: &numberSpace{received: received, sent: make(map[uint64]*sendItem)}, sent: sent, congestion: congestion, timer: new(recovery.Timer), pacer: new(recovery.Pacer)}
}

// application allocates packet state only once the owner initiates/accepts a
// network path, never merely because a CID or status frame arrived. The owner
// must enforce client-only initiation and perform network address validation.
func (p *pathCIDs) application(id uint64) (*pathApplication, error) {
	if p.abandoned[id] != nil {
		return nil, errPathAbandoned
	}
	s := p.paths[id]
	if s == nil {
		return nil, errors.New("quic: no CID state for application path")
	}
	if s.application == nil {
		s.application = newPathApplication(p.congestionControl)
	}
	return s.application, nil
}

func (a *pathApplication) recordSent(packet recovery.Packet) error {
	if packet.Number != a.space.nextPN {
		return errors.New("quic: incorrect path packet number")
	}
	sent := recovery.SentEvent{Bytes: packet.Bytes, Flight: a.sent.BytesInFlight, AppLimited: a.sent.AppLimited(), Now: packet.SentAt}
	if err := a.sent.OnSent(recovery.Application, packet); err != nil {
		return err
	}
	if packet.InFlight {
		a.congestion.OnPacketSent(sent)
	}
	a.space.nextPN++
	a.space.sentAny = true
	a.space.largestSent = packet.Number
	return nil
}

// ack borrows receive state only for immediate serialized packet encoding.
func (a *pathApplication) ack(id uint64, now time.Time, exponent uint8) (wire.PathAckFrame, error) {
	f, err := a.space.received.ackView(now, exponent)
	return wire.PathAckFrame{PathID: id, AckFrame: f}, err
}

// receiveACK uses the ACK's path ID, not the path carrying the ACK packet.
// A base ACK always names path zero. Existing abandoned paths still accept late
// ACKs during retention; ACKs after disposal are silently ignored.
func (p *pathCIDs) receiveACK(frame wire.Frame, now time.Time, exponent uint8, maxDelay time.Duration, limited bool) (recovery.Events, error) {
	var id uint64
	var ack wire.AckFrame
	switch f := frame.(type) {
	case wire.AckFrame:
		ack = f
	case wire.PathAckFrame:
		id = f.PathID
		ack = f.AckFrame
	default:
		return recovery.Events{}, errors.New("quic: expected ACK frame")
	}
	if id > p.localMax {
		return recovery.Events{}, pathCIDViolation(frame.Type(), "ACK path exceeds advertised maximum")
	}
	if exponent > 20 || maxDelay < 0 {
		return recovery.Events{}, errors.New("quic: invalid peer ACK delay configuration")
	}
	s := p.paths[id]
	if s == nil || s.application == nil {
		if p.abandoned[id] != nil {
			return recovery.Events{}, nil
		}
		return recovery.Events{}, pathCIDViolation(frame.Type(), "ACK for path without sent packets")
	}
	a := s.application
	events, err := a.sent.OnACK(recovery.Application, ack, now, recovery.DecodeACKDelay(ack.Delay, exponent), maxDelay, true)
	if err != nil {
		return recovery.Events{}, pathCIDViolation(frame.Type(), err.Error())
	}
	if a.mtu != nil {
		// Recovery can have discarded a previously lost probe's packet metadata;
		// the bounded search ledger still permits its authenticated late ACK.
		for i := 0; i < a.mtu.attempts; i++ {
			if ack.Acknowledges(a.mtu.numbers[i]) {
				a.mtu.acknowledged(a.mtu.numbers[i])
				break
			}
		}
		a.noteMTULoss(events.Lost)
		a.observeMTUBlackhole(events.Acked, events.Lost, now)
		a.updateMTURaise(now)
	}
	a.timer.Progress(events, true)
	if events.ECNCongestion {
		a.congestion.OnECN(events.ECNSentAt, now)
	}
	a.congestion.OnLoss(recovery.LossEvent{Lost: events.Lost, TotalLost: a.sent.LostBytes(), Flight: a.sent.BytesInFlight, Now: now})
	if len(events.Lost) > 0 && a.sent.PersistentCongestion(maxDelay) {
		a.congestion.OnPersistentCongestion()
		a.sent.RTT.OnPersistentCongestion()
	}
	a.congestion.OnACK(recovery.ACKEvent{Acked: events.Acked, Rate: events.Rate, Limited: limited, Flight: a.sent.BytesInFlight, Now: now})
	return events, nil
}

// noteMultipathSendIdle marks each path application limited when a send pass
// found nothing to send on it though its window had room (see
// noteBaseSendIdle).
func (c *Conn) noteMultipathSendIdle() {
	if c.multipath == nil {
		return
	}
	for id, state := range c.multipath.paths {
		if app := state.application; app != nil && c.multipath.abandoned[id] == nil && app.sent.BytesInFlight < app.congestion.Window() && !c.pathHasData(id) {
			app.sent.MarkAppLimited()
		}
	}
}

// pathHasData reports whether path id has something it could send: an
// unacknowledged item in its own queue, or data on another path it may borrow
// (mayBorrow, mayLend, borrowable, as nextSharedPathData uses them). Stream
// data is queued on one path and shared, so a path's own queue being empty
// does not make it application limited. The scheduler's transient waits, for
// an owed ACK or the path's own queue, don't count: they pass within a send
// pass and don't leave the window unused.
func (c *Conn) pathHasData(id uint64) bool {
	own := c.multipath.paths[id].application
	if own == nil {
		return false
	}
	held := false
	for _, item := range own.space.queue {
		if c.sendable(item) {
			return true
		}
		held = held || !item.acked
	}
	// nextSharedPathData doesn't borrow onto a non-empty queue. Items held
	// there, such as CID advertisements while forwarding is blocked, keep it
	// non-empty across send passes, so the path can't borrow until they go.
	if held || !c.mayBorrow(id) {
		return false
	}
	for source, state := range c.multipath.paths {
		if !c.mayLend(source, id) {
			continue
		}
		for _, item := range state.application.space.queue {
			if c.borrowable(item, id) {
				return true
			}
		}
	}
	return false
}

// arm runs after send/ACK/credit changes. For post-handshake paths, no Initial
// anti-deadlock timer is needed; path validation probes belong to the owner.
func (a *pathApplication) arm(now time.Time, maxDelay time.Duration, amplificationBlocked bool) {
	if a.probePending {
		return // Rearm only after the requested probe actually leaves the sender.
	}
	a.timer.Arm(a.sent, now, recovery.TimerState{Confirmed: true, PeerValidated: true, MaxACKDelay: maxDelay, AmplificationBlocked: amplificationBlocked})
}

// receiveReliableACK validates packet recovery first, then completes shared
// frame items and prunes their copies across paths. Completion updates stream
// state and connection-wide queued bytes in the eventual live engine owner.
func (p *pathCIDs) receiveReliableACK(frame wire.Frame, now time.Time, exponent uint8, maxDelay time.Duration, limited bool, complete func(*sendItem) error) (recovery.Events, error) {
	if complete == nil {
		return recovery.Events{}, errors.New("quic: missing reliable ACK completion handler")
	}
	events, err := p.receiveACK(frame, now, exponent, maxDelay, limited)
	if err != nil {
		return events, err
	}
	var id uint64
	var ack wire.AckFrame
	switch f := frame.(type) {
	case wire.AckFrame:
		ack = f
	case wire.PathAckFrame:
		id = f.PathID
		ack = f.AckFrame
	}
	if state := p.paths[id]; state != nil && state.application != nil {
		if err := acknowledgeItems(state.application.space, ack, complete); err != nil {
			return events, err
		}
	}
	for _, state := range p.paths {
		if state.application != nil {
			pruneAcknowledged(state.application.space)
		}
	}
	return events, nil
}

func (a *pathApplication) noteMTULoss(packets []recovery.Packet) {
	if a.mtu == nil {
		return
	}
	for _, p := range packets {
		if p.MTUProbe {
			a.mtu.lost(p.Number)
		}
	}
}
