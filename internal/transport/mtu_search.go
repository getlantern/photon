package transport

import (
	"errors"

	"github.com/getlantern/photon/internal/wire"
)

const (
	minimumQUICMTU    = 1200
	maximumUDPPayload = 65527
	mtuProbeAttempts  = 3
)

// mtuSearch is the size-search component of DPLPMTUD, not a packet scheduler.
// Its owner must first validate the 1200-byte baseline, supply the smaller of
// the peer/local UDP payload limits, and give it only authenticated ACK/loss
// events for dedicated padded probes on this path. It is connected to staged per-path
// packet emission. Except for an explicitly configured original-path starting
// size, no ordinary packet size may increase before acknowledged().
// The serialized connection owner supplies congestion, pacing, timers, probe
// loss exclusions, black-hole detection and path-change notifications.
type mtuSearch struct {
	// fixed retains packet-size/fallback state without sending discovery probes.
	fixed                     bool
	confirmed, ceiling, upper int
	candidate                 int
	numbers                   [mtuProbeAttempts]uint64
	attempts                  int
	pending                   bool
	lastNumber                uint64
	sentAny                   bool
}

func newMTUSearch(ceiling int) (mtuSearch, error) {
	if ceiling < minimumQUICMTU || ceiling > maximumUDPPayload {
		return mtuSearch{}, errors.New("quic: invalid MTU search ceiling")
	}
	return mtuSearch{confirmed: minimumQUICMTU, ceiling: ceiling, upper: ceiling}, nil
}

// next returns a proposed UDP payload size, not permission to send. Repeated
// calls are side-effect free, allowing congestion or amplification to block it.
func (m *mtuSearch) next() (int, bool) {
	if m.fixed || m.pending || m.confirmed < minimumQUICMTU || m.confirmed >= m.upper {
		return 0, false
	}
	if m.candidate != 0 {
		return m.candidate, true
	}
	return m.confirmed + (m.upper-m.confirmed+1)/2, true
}

// sent commits only a packet actually emitted at the proposed size. Packet
// numbers are monotonic across resets because path migration does not reset
// QUIC packet-number spaces. Invalid commits leave the search unchanged.
func (m *mtuSearch) sent(number uint64, size int) error {
	candidate, ok := m.next()
	if !ok || size != candidate || number > wire.MaxVarint || m.sentAny && number <= m.lastNumber {
		return errors.New("quic: invalid MTU probe commit")
	}
	m.candidate = candidate
	m.numbers[m.attempts] = number
	m.attempts++
	m.pending = true
	m.lastNumber, m.sentAny = number, true
	return nil
}

// acknowledged permits a late ACK for an earlier attempt of the current size.
// It never infers success from an ACK for unrelated application traffic. Once a
// size is abandoned, old evidence is discarded to keep storage bounded.
func (m *mtuSearch) acknowledged(number uint64) bool {
	for i := 0; i < m.attempts; i++ {
		if m.numbers[i] == number {
			m.confirmed = m.candidate
			m.clearAttempts()
			return true
		}
	}
	return false
}

// lost retries the same size three times before narrowing the search. A single
// loss never lowers the already-confirmed packet size or the search ceiling.
// A false return identifies a stale or unrelated event.
func (m *mtuSearch) lost(number uint64) bool {
	if !m.pending || m.attempts == 0 || m.numbers[m.attempts-1] != number {
		return false
	}
	m.pending = false
	if m.attempts == mtuProbeAttempts {
		m.upper = m.candidate - 1
		m.clearAttempts()
	}
	return true
}

func (m *mtuSearch) clearAttempts() {
	m.candidate, m.attempts = 0, 0
	m.pending = false
	clear(m.numbers[:])
}

// reset invalidates size evidence after migration or a separately detected MTU
// black hole. Baseline validation is the owner's responsibility. It preserves
// the packet-number high-water mark so delayed feedback cannot validate a probe
// on the new network tuple. It is also the primitive for a later upward recheck.
func (m *mtuSearch) reset() {
	m.confirmed, m.upper = minimumQUICMTU, m.ceiling
	m.clearAttempts()
}

// tooLarge handles a synchronous local EMSGSIZE rejection of the current probe.
// Unlike network loss it immediately narrows the bound. No attempt at the
// rejected size remains usable as evidence, even if an earlier attempt arrives.
func (m *mtuSearch) tooLarge(number uint64) bool {
	if !m.pending || m.attempts == 0 || m.numbers[m.attempts-1] != number {
		return false
	}
	m.upper = m.candidate - 1
	m.clearAttempts()
	return true
}
