package transport

import (
	"errors"
	"io"

	"github.com/getlantern/photon/internal/recovery"
)

// pendingForeignWrite belongs exclusively to the connection owner. While it is
// pending, the caller must not construct another packet for this connection.
// Socket owners receive only copied bytes, never this retained descriptor.
type pendingForeignWrite struct {
	out    *Outbound
	writer *packetWriter
	route  *handoffRoute
	notify chan<- struct{}
	result <-chan foreignWriteResult
	retry  bool
	done   bool
}

// beginForeignWrite consumes a capacity ticket acquired before packet creation.
// Submission failures are returned as completions, since the packet has already
// been committed by the engine and still requires owner-side error handling.
func beginForeignWrite(ticket *foreignWriteReservation, out *Outbound, notify chan<- struct{}) (*pendingForeignWrite, error) {
	if out.Conn == nil || out.Conn.pendingWrite != nil || out.Conn.closed || out.Conn.Closing() || out.Conn.Draining() {
		ticket.cancel()
		return nil, errPathBinding
	}
	p := &pendingForeignWrite{out: out.retain(), writer: ticket.writer, route: ticket.route, notify: notify}
	out.Conn.pendingWrite = p
	// Publish first-use tokens before the source can write and receive a reset.
	ticket.route.publishResetHints(out.Conn)
	result, err := ticket.submit(p.out.Data, p.out.Peer, p.out.ECN, notify)
	p.setResult(result, err)
	return p, nil
}

// release runs only on the connection owner, including connection disposal.
func (p *pendingForeignWrite) release() {
	if p.out != nil && p.out.Conn != nil && p.out.Conn.pendingWrite == p {
		p.out.Conn.pendingWrite = nil
	}
	p.done, p.out, p.result = true, nil, nil
}

func (p *pendingForeignWrite) setResult(result <-chan foreignWriteResult, err error) {
	if err != nil {
		ready := make(chan foreignWriteResult, 1)
		ready <- foreignWriteResult{err: err}
		result = ready
		select {
		case p.notify <- struct{}{}:
		default:
		}
	}
	p.result = result
}

// poll returns a completion once. Zero-byte errors may qualify for atomic write
// rollback; positive byte counts must never be passed to HandleWriteError as an
// unsent packet. Short successful writes are surfaced as io.ErrShortWrite.
// A closed connection discards the result without touching recovery state.
// This helper does not choose whether an alternative-path error closes a path
// or the connection; that decision belongs to the path controller.
func (p *pendingForeignWrite) poll() (*Outbound, foreignWriteResult, bool) {
	if p.done {
		return nil, foreignWriteResult{}, false
	}
	if p.out.Conn != nil && (p.out.Conn.closed || p.out.Conn.Closing() || p.out.Conn.Draining()) {
		p.release()
		return nil, foreignWriteResult{}, false
	}
	if !p.retry {
		var result foreignWriteResult
		select {
		case result = <-p.result:
		default:
			return nil, foreignWriteResult{}, false
		}
		out := p.out
		unsupported := errors.Is(result.err, errForeignECNUnsupported) || unsupportedECNError(result.err)
		if result.n == 0 && result.err != nil && unsupported && out.ECN != recovery.NotECT && out.ecn.sent != nil && out.ecn.sent.RejectECNMarking(out.ecn.space, out.ecn.number) {
			out.ECN = recovery.NotECT
			p.retry, p.result = true, nil
		} else {
			if result.err == nil && result.n != len(out.Data) {
				result.err = io.ErrShortWrite
			}
			p.release()
			return out, result, true
		}
	}
	// Other producers may have occupied the original slot while the socket was
	// writing. Reserve again without rebuilding the packet or rolling back ECN
	// twice. Capacity release wakes the owning handoff through route.target.
	ticket, err := p.writer.reserve(p.route)
	if errors.Is(err, errOperationWouldBlock) {
		return nil, foreignWriteResult{}, false
	}
	p.retry = false
	if err != nil {
		p.setResult(nil, err)
	} else {
		result, submitErr := ticket.submit(p.out.Data, p.out.Peer, recovery.NotECT, p.notify)
		p.setResult(result, submitErr)
	}
	return nil, foreignWriteResult{}, false
}
