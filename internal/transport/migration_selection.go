package transport

import (
	"errors"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

var errMigrationState = errors.New("quic: invalid migration selection state")

// migrationSelection keeps at most one nominated candidate in a packet-number
// space. The validated tuple is retained until commit, so failed/spoofed
// nominations cannot discard the fallback address. It is owner-confined.
// Authentication, replay processing, frame classification, and permission to
// migrate must precede observeAuthenticated; this type grants none of them.
type migrationSelection struct {
	validated          pathTuple
	candidate          *migrationCandidate
	highestNonProbing  uint64
	haveNonProbing     bool
	candidateNominated bool
	highestProbePacket uint64
	haveProbePacket    bool
}

func newMigrationSelection(validated pathTuple) (*migrationSelection, error) {
	tuple, err := canonicalPathTuple(validated.local, validated.remote)
	if err != nil {
		return nil, err
	}
	return &migrationSelection{validated: tuple}, nil
}

// observeAuthenticated handles one authenticated datagram's address evidence.
// Older non-probing packets and probing-only packets cannot nominate addresses.
// Repeated input on a candidate grants credit without restarting validation.
func (m *migrationSelection) observeAuthenticated(tuple pathTuple, pn uint64, nonProbing bool, bytes int, now time.Time, pto time.Duration) (*migrationCandidate, error) {
	tuple, err := canonicalPathTuple(tuple.local, tuple.remote)
	if err != nil {
		return nil, err
	}
	if pn > wire.MaxVarint || bytes < 1 || now.IsZero() || pto <= 0 || pto > time.Duration(1<<63-1)/3 {
		return nil, errMigrationState
	}
	m.expire(now)
	newer := nonProbing && (!m.haveNonProbing || pn > m.highestNonProbing)
	if !newer {
		if m.candidate != nil && m.candidate.tuple == tuple {
			if err := m.candidate.credit(tuple, bytes, now); err != nil {
				return nil, err
			}
			return m.candidate, nil
		}
		return nil, nil
	}
	if tuple == m.validated {
		m.highestNonProbing, m.haveNonProbing = pn, true
		if m.candidate != nil && m.candidate.requiresNomination {
			// Keep pre-migration reachability proof while original traffic
			// continues, but require a newer nomination before switching.
			m.candidateNominated = false
		} else {
			m.candidate = nil
		}
		return nil, nil
	}
	candidate := m.candidate
	if candidate == nil || candidate.tuple != tuple {
		candidate, err = newMigrationCandidate(tuple, bytes, now, pto)
	} else {
		err = candidate.credit(tuple, bytes, now)
	}
	if err != nil {
		return nil, err
	}
	m.highestNonProbing, m.haveNonProbing = pn, true
	m.candidate = candidate
	if candidate.requiresNomination {
		m.candidateNominated = true
	}
	return candidate, nil
}

func (m *migrationSelection) expire(now time.Time) {
	if m.candidate != nil && m.candidate.expire(now) {
		m.candidate = nil
	}
	// Keep the ordering watermark: an expired nomination cannot be replayed to
	// allocate a fresh candidate or restart its amplification/validation state.
}

// commit requires the exact current candidate and complete address/MTU proof.
// The owner must atomically update routing/CIDs/recovery alongside this call.
// The old validated tuple is returned for fallback/recovery bookkeeping.
func (m *migrationSelection) commit(candidate *migrationCandidate) (pathTuple, error) {
	if !m.canCommit(candidate) {
		return pathTuple{}, errMigrationState
	}
	old := m.validated
	m.validated = candidate.tuple
	m.candidate = nil
	return old, nil
}

// canCommit requires reachability and a newer non-probing nomination for
// candidates first admitted through probing. Call before mutating CID/recovery ownership.
func (m *migrationSelection) canCommit(candidate *migrationCandidate) bool {
	return candidate != nil && candidate == m.candidate && candidate.validation.state == PathValidated && (!candidate.requiresNomination || m.candidateNominated)
}

// observePreferredAuthenticated is for a server's advertised local address.
// The caller must authenticate, authorize that local socket, and check the CID.
// Probes can start validation but cannot authorize application traffic there.
// Keep a packet watermark across expiry so replay cannot renew the candidate.
func (m *migrationSelection) observePreferredAuthenticated(tuple pathTuple, pn uint64, nonProbing bool, bytes int, now time.Time, pto time.Duration) (*migrationCandidate, error) {
	return m.observeProbingAuthenticated(tuple, pn, nonProbing, bytes, now, pto, true)
}

// observeProbingAuthenticated retains reachability state without authorizing
// rebinding. The caller admits only an authenticated challenge or a preferred
// address here. A single candidate and a persistent watermark bound replay.
func (m *migrationSelection) observeProbingAuthenticated(tuple pathTuple, pn uint64, nonProbing bool, bytes int, now time.Time, pto time.Duration, preferred bool) (*migrationCandidate, error) {
	tuple, err := canonicalPathTuple(tuple.local, tuple.remote)
	if err != nil {
		return nil, err
	}
	if tuple == m.validated || pn > wire.MaxVarint || bytes < 1 || now.IsZero() || pto <= 0 || pto > time.Duration(1<<63-1)/3 {
		return nil, errMigrationState
	}
	m.expire(now)
	candidate := m.candidate
	if candidate != nil && (candidate.tuple != tuple || !candidate.requiresNomination || candidate.preferred != preferred) {
		// An unselected probe can outlive the peer's interest in that tuple.
		// Prefer a newer ordinary probe rather than pinning this bounded slot
		// forever. Do not displace preferred-address or nominated migration.
		if preferred || candidate.preferred || !candidate.requiresNomination || m.candidateNominated || !m.haveProbePacket || pn <= m.highestProbePacket {
			return nil, nil
		}
		candidate = nil
		// Publish replacement only after construction succeeds. The sender's
		// reservation cleanup will retire any CID spent on the old candidate.
	}
	if candidate == nil {
		if m.haveProbePacket && pn <= m.highestProbePacket {
			return nil, nil
		}
		candidate, err = newMigrationCandidate(tuple, bytes, now, pto)
		if err != nil {
			return nil, err
		}
		candidate.preferred = preferred
		candidate.requiresNomination = true
		m.candidate = candidate
		m.candidateNominated = false
	} else if err := candidate.credit(tuple, bytes, now); err != nil {
		return nil, err
	}
	if !m.haveProbePacket || pn > m.highestProbePacket {
		m.highestProbePacket, m.haveProbePacket = pn, true
	}
	if nonProbing && (!m.haveNonProbing || pn > m.highestNonProbing) {
		m.highestNonProbing, m.haveNonProbing = pn, true
		m.candidateNominated = true
	}
	return candidate, nil
}
