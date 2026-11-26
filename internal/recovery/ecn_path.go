package recovery

import "github.com/getlantern/photon/internal/wire"

// ECNPathState describes sender marking policy for one network path.
type ECNPathState uint8

const (
	ECNDisabled ECNPathState = iota
	ECNTesting
	ECNUnknown
	ECNCapable
	ECNFailed
)

// ECNPath tests a network path using its first ten ECT(0) packets. It is separate
// from cumulative packet-number-space feedback, which survives migration. A new
// network path needs a new ECNPath. Its zero value disables marking.
// Callbacks must belong to this network path, except ACKs: probe identities
// ensure late old-path ACKs cannot establish the new path's capability.
type ECNPath struct {
	state  ECNPathState
	probes [10]ecnProbe
	count  int
}

type ecnProbe struct {
	space  Space
	number uint64
	lost   bool
	acked  bool // acknowledged, by validated feedback or not
	ce     bool // first acknowledged by feedback that reported only CE
}

func NewECNPath() *ECNPath             { return &ECNPath{state: ECNTesting} }
func (p *ECNPath) State() ECNPathState { return p.state }

// Marking also requires space-specific recovery capacity and socket support.
// A caller must not originate ECT markings unless both are available.
func (p *ECNPath) Marking() ECNMark {
	if p.state == ECNTesting || p.state == ECNCapable {
		return ECT0
	}
	return NotECT
}

// Sent records a successfully transmitted ECT(0) packet. Do not call for a
// locally rejected write. Coalesced QUIC packets are recorded individually.
func (p *ECNPath) Sent(space Space, number uint64) {
	if p.state != ECNTesting || space > Application || number > wire.MaxVarint {
		return
	}
	for _, probe := range p.probes[:p.count] {
		if probe.space == space && probe.number == number {
			return
		}
	}
	p.probes[p.count] = ecnProbe{space: space, number: number}
	p.count++
	if p.count == len(p.probes) {
		p.state = ECNUnknown
	}
}

// ACK consumes a result from ECNFeedback.Check after normal ACK validation.
// Successful feedback must acknowledge this path's own probe, and report a
// packet that arrived still ECT, before it can establish capability.
// Failure in any space disables the entire path. A path that remarks every
// test packet CE fails too, as quic-go does: such remarking would otherwise
// read as constant congestion.
func (p *ECNPath) ACK(space Space, frame wire.AckFrame, feedback ECNFeedbackResult) {
	if p.state == ECNDisabled || p.state == ECNFailed {
		return
	}
	if feedback.Failed {
		p.state = ECNFailed
		return
	}
	if space > Application || p.state == ECNCapable {
		return
	}
	// A probe's outcome comes only from the ACK that first acknowledges it;
	// one first acknowledged without validated feedback has none.
	acknowledged := false
	for i := range p.probes[:p.count] {
		probe := &p.probes[i]
		if probe.space == space && !probe.acked && frame.Acknowledges(probe.number) {
			probe.acked = true
			if feedback.Validated {
				acknowledged = true
				probe.ce = feedback.ECT == 0
			}
		}
	}
	if !acknowledged {
		return
	}
	if feedback.ECT > 0 {
		p.state = ECNCapable
		return
	}
	p.checkRemarked()
}

// checkRemarked fails the path once its whole test flight is sent and every
// probe has come back CE or been lost, at least one CE.
func (p *ECNPath) checkRemarked() {
	if p.state != ECNUnknown || p.count == 0 {
		return
	}
	ce := false
	for _, probe := range p.probes[:p.count] {
		if !probe.ce && !probe.lost {
			return
		}
		ce = ce || probe.ce
	}
	if ce {
		p.state = ECNFailed
	}
}

// Lost records recovery loss declarations. Only loss of the entire bounded
// test flight fails validation; duplicates and unmarked losses do not count.
func (p *ECNPath) Lost(space Space, number uint64) {
	if p.state != ECNTesting && p.state != ECNUnknown {
		return
	}
	lost := 0
	for i := range p.probes[:p.count] {
		probe := &p.probes[i]
		if probe.space == space && probe.number == number {
			probe.lost = true
		}
		if probe.lost {
			lost++
		}
	}
	if p.state == ECNUnknown && p.count > 0 && lost == p.count {
		p.state = ECNFailed
		return
	}
	p.checkRemarked()
}

// forget removes a probe that did not leave the host or whose encryption space
// was discarded without an ACK/loss outcome. Neither proves network failure.
func (p *ECNPath) forget(space Space, number uint64) {
	if p.state != ECNTesting && p.state != ECNUnknown {
		return
	}
	for i, probe := range p.probes[:p.count] {
		if probe.space == space && probe.number == number {
			copy(p.probes[i:], p.probes[i+1:p.count])
			p.count--
			p.probes[p.count] = ecnProbe{}
			p.state = ECNTesting
			return
		}
	}
}

// endTesting stops marking while outstanding test packets await feedback.
// All-probe loss can already be known when the time budget expires.
func (p *ECNPath) endTesting() {
	if p.state != ECNTesting {
		return
	}
	p.state = ECNUnknown
	p.Lost(Space(255), 0)
}
