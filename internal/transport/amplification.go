package transport

import "errors"

var ErrAmplification = errors.New("quic: path anti-amplification limit exceeded")

// Amplification tracks the RFC 9000 section 8.1 three-times receive budget for
// one unvalidated path. The connection decides which datagrams count, and marks
// validation only after the appropriate handshake/token/PATH_RESPONSE evidence.
// Clients can mark their initial server path validated for this accounting;
// this does not authenticate the server or complete QUIC path validation.
// Congestion control and anti-amplification are separate gates on every send.
type Amplification struct {
	validated bool
	credit    uint64
}

func (a *Amplification) Validate()       { a.validated = true }
func (a *Amplification) Validated() bool { return a.validated }

func (a *Amplification) Received(bytes int) error {
	if bytes < 0 {
		return errors.New("quic: negative datagram size")
	}
	n := uint64(bytes)
	maximum := ^uint64(0)
	if n > (maximum-a.credit)/3 {
		a.credit = maximum
	} else {
		a.credit += 3 * n
	}
	return nil
}
func (a *Amplification) CanSend(bytes int) bool {
	return bytes >= 0 && (a.validated || uint64(bytes) <= a.credit)
}

// Sent commits accounting only for a successfully transmitted UDP datagram.
// Failed writes and queued datagrams do not consume credit. Callers must reserve
// and commit within their serialized connection loop to avoid overspending.
func (a *Amplification) Sent(bytes int) error {
	if !a.CanSend(bytes) {
		return ErrAmplification
	}
	if !a.validated {
		a.credit -= uint64(bytes)
	}
	return nil
}
