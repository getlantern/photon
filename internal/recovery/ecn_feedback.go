package recovery

import "github.com/getlantern/photon/internal/wire"

// ECNFeedback checks cumulative feedback for one packet-number space (scoped
// to a path ID with multipath). It does not validate ACK ranges, select outgoing
// markings, detect validation-probe loss, or determine path capability. Ordinary
// migration keeps the packet-number space and its cumulative feedback; the owner
// must separately revalidate the new network path against the existing baseline.
// A failure requires the owner to disable ECN on the affected network path.
type ECNFeedback struct {
	counts      [3]uint64
	largest     uint64
	haveLargest bool
	failed      bool
}

type ECNFeedbackResult struct {
	// Validated means this advancing ACK newly acknowledges marked packets and
	// supplies consistent feedback. It is not a complete path-validation policy.
	Validated bool
	Failed    bool
	// CE is newly reported congestion after successful feedback checks.
	CE uint64
}

// Check runs only after ACK-of-unsent/range validation. sent is the cumulative
// number sent with ECT(0)/ECT(1); newly is the exact number of newly acknowledged
// packets with those markings, excluding duplicate acknowledgment evidence.
// The caller must preserve marking evidence for late acknowledgments.
func (e *ECNFeedback) Check(largest uint64, counts *[3]uint64, sent, newly [2]uint64) ECNFeedbackResult {
	if e.failed {
		return ECNFeedbackResult{Failed: true}
	}
	// RFC 9000 13.4.2.1 forbids failing validation due to reordered ACKs.
	if e.haveLargest && largest <= e.largest {
		return ECNFeedbackResult{}
	}
	fail := func() ECNFeedbackResult { e.failed = true; return ECNFeedbackResult{Failed: true} }
	if largest > wire.MaxVarint || sent[0] > wire.MaxVarint || sent[1] > wire.MaxVarint || newly[0] > sent[0] || newly[1] > sent[1] {
		return fail()
	}
	e.haveLargest, e.largest = true, largest
	if counts == nil {
		if newly[0] != 0 || newly[1] != 0 {
			return fail()
		}
		return ECNFeedbackResult{}
	}
	var delta [3]uint64
	for i, n := range counts {
		if n > wire.MaxVarint || n < e.counts[i] {
			return fail()
		}
		delta[i] = n - e.counts[i]
	}
	// Individual ECT totals detect remarking into an ECT codepoint we did not
	// send. Total marked feedback cannot exceed all marked transmissions.
	// Three bounded 62-bit counts fit in uint64 without overflow.
	if counts[0] > sent[0] || counts[1] > sent[1] || counts[0]+counts[1]+counts[2] > sent[0]+sent[1] {
		return fail()
	}
	if delta[0]+delta[2] < newly[0] || delta[1]+delta[2] < newly[1] || delta[0]+delta[1]+delta[2] < newly[0]+newly[1] {
		return fail()
	}
	e.counts = *counts
	return ECNFeedbackResult{Validated: newly[0] != 0 || newly[1] != 0, CE: delta[2]}
}
