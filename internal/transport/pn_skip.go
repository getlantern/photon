package transport

import (
	"errors"
	"math/rand/v2"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
)

// Packet-number skipping (RFC 9000 Section 21.4): an application packet
// number is now and then left unsent, so a peer acknowledging packets it
// hasn't received, to inflate the congestion window, eventually acknowledges
// one that was never sent and fails the connection. Skips start about every
// pnSkipInitialPeriod packets and grow sparser, to pnSkipMaxPeriod, as in
// quic-go.
var (
	pnSkipInitialPeriod uint64 = 256
	pnSkipMaxPeriod     uint64 = 128 << 10
)

var errSkipMismatch = errors.New("quic: skipped packet number out of step with recovery")

// pnSkipper schedules a space's skips. Its zero value schedules the first.
type pnSkipper struct {
	at     uint64 // the packet number to skip
	period uint64 // zero until the first skip is scheduled
}

// schedule picks the next skip at least three packets after next, so no two
// skipped numbers are adjacent, and on average period packets away.
func (k *pnSkipper) schedule(next uint64) {
	if k.period == 0 {
		k.period = pnSkipInitialPeriod
	}
	k.at = next + 3 + rand.Uint64N(2*k.period)
	k.period = min(2*k.period, pnSkipMaxPeriod)
}

// nextPacketNumber is the number s's next 1-RTT packet takes: nextPN, or
// the number after it when a skip is due. Nothing is consumed until
// commitPacketNumber, so a packet built and then held back, by congestion,
// pacing or a full ledger, leaves no trace.
func (s *numberSpace) nextPacketNumber() uint64 {
	if s.skip.period == 0 {
		s.skip.schedule(s.nextPN)
	}
	if s.nextPN >= s.skip.at && s.nextPN+1 < wire.MaxVarint {
		return s.nextPN + 1
	}
	return s.nextPN
}

// commitPacketNumber records in sent the number skipped to reach pn, from
// nextPacketNumber, just before the packet carrying pn is sealed. Committing
// then, rather than after the previous packet, keeps that packet the latest
// until its send result is handled, as the reject paths require.
func commitPacketNumber(s *numberSpace, sent *recovery.Sent, pn uint64) error {
	if pn == s.nextPN {
		return nil
	}
	if pn != s.nextPN+1 {
		return errSkipMismatch
	}
	skipped, err := sent.Skip(recovery.Application)
	if err != nil {
		return err
	}
	if skipped != s.nextPN {
		return errSkipMismatch
	}
	s.nextPN++
	s.skip.schedule(s.nextPN)
	return nil
}
