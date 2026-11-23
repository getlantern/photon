package transport

import "github.com/getlantern/photon/internal/wire"

// noteNonProbing is called only after authentication/frame parsing, and only
// for the established tuple (or first accepted tuple of a new path ID).
// Candidate-address decoding must not advance this watermark before selection.
func (s *numberSpace) noteNonProbing(pn uint64, frames []wire.Frame) {
	if s.haveNonProbing && pn <= s.highestNonProbing {
		return
	}
	for _, frame := range frames {
		if !wire.ProbingFrame(frame) {
			s.highestNonProbing, s.haveNonProbing = pn, true
			return
		}
	}
}

// migrationSelection seeds nomination ordering from the same packet-number
// space, including authenticated traffic received before multipath activation.
func (s *numberSpace) migrationSelection(validated pathTuple) (*migrationSelection, error) {
	m, err := newMigrationSelection(validated)
	if err != nil {
		return nil, err
	}
	m.highestNonProbing, m.haveNonProbing = s.highestNonProbing, s.haveNonProbing
	return m, nil
}
