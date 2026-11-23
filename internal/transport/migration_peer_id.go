package transport

// reserveMigrationID gives a candidate exclusive use of an unused peer CID.
// Reservation does not change the established tuple's current CID or mark the
// ID used: blocked output can be canceled without consuming a privacy budget.
// All operations run on the connection owner.
func (p *peerIDs) reserveMigrationID(candidate *migrationCandidate) (uint64, bool) {
	if candidate == nil {
		return 0, false
	}
	var selected uint64
	found := false
	for seq, id := range p.active {
		if id.reservation == candidate {
			return seq, true
		}
		if seq != p.current && !id.used && id.reservation == nil && len(id.cid) != 0 && (!found || seq < selected) {
			selected, found = seq, true
		}
	}
	if found {
		id := p.active[selected]
		id.reservation = candidate
		p.active[selected] = id
	}
	return selected, found
}

// migrationID returns an active reservation. A RETIRE_PRIOR_TO update can
// revoke it; callers must check again before each probe and binding commit.
func (p *peerIDs) migrationID(candidate *migrationCandidate, seq uint64) (peerID, bool) {
	id, ok := p.active[seq]
	return id, ok && candidate != nil && id.reservation == candidate
}

// sentMigrationID is called only after packet construction commits. The used
// bit is also needed to recognize stateless reset tokens on the candidate.
func (p *peerIDs) sentMigrationID(candidate *migrationCandidate, seq uint64) bool {
	id, ok := p.migrationID(candidate, seq)
	if !ok {
		return false
	}
	id.used = true
	p.active[seq] = id
	return true
}

// cancelMigrationID releases an unsent reservation. A sent ID remains excluded
// from ordinary selection and requires a reliably queued retirement; the caller
// must arrange that retirement before deleting it. The return value distinguishes
// this outstanding retirement from a cancellation requiring no wire output.
func (p *peerIDs) cancelMigrationID(candidate *migrationCandidate, seq uint64) (retire bool) {
	id, ok := p.migrationID(candidate, seq)
	if !ok {
		return false
	}
	if id.used {
		return true
	}
	id.reservation = nil
	p.active[seq] = id
	if _, ok := p.active[p.current]; !ok {
		p.current = seq
	}
	return false
}

// commitMigrationID may run only in the same owner transaction as the validated
// address switch. Former CIDs remain active for outstanding/reset state; this
// method excludes previously used IDs from selection on the new address.
// Their reliable retirement must be staged separately by the binding owner.
func (p *peerIDs) commitMigrationID(candidate *migrationCandidate, seq uint64) bool {
	id, ok := p.migrationID(candidate, seq)
	if !ok || !id.used || candidate.validation.state != PathValidated {
		return false
	}

	for oldSeq, old := range p.active {
		if oldSeq != seq && old.used && old.reservation == nil {
			old.reservation = candidate
			p.active[oldSeq] = old
		}
	}
	id.reservation = nil
	p.active[seq] = id
	p.current = seq
	return true
}
