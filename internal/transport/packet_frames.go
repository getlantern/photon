package transport

import (
	"errors"

	"github.com/getlantern/photon/internal/packet"
	"github.com/getlantern/photon/internal/wire"
)

// parsePacketFrames checks the entire authenticated payload before the caller
// applies frame side effects. Byte fields borrow the owned plaintext buffer.
func parsePacketFrames(plain []byte, ctx wire.FrameContext) ([]wire.Frame, bool, error) {
	if len(plain) == 0 {
		return nil, false, &TransportError{Code: 0xa, Reason: "packet payload has no frames"}
	}
	var frames []wire.Frame
	eliciting := false
	for len(plain) > 0 {
		f, n, err := wire.ConsumeFrame(plain)
		if err != nil {
			var fe *wire.FrameError
			var typ uint64
			if errors.As(err, &fe) {
				typ = fe.FrameType
			}
			return nil, false, &TransportError{Code: 7, FrameType: typ, Reason: err.Error()}
		}
		if err := wire.ValidateFrame(f, ctx, n); err != nil {
			return nil, false, &TransportError{Code: 0xa, FrameType: f.Type(), Reason: err.Error()}
		}
		eliciting = eliciting || wire.AckEliciting(f)
		if f.Type() != 0 {
			frames = append(frames, f)
		}
		plain = plain[n:]
	}
	return frames, eliciting, nil
}

// authenticatedOpenError identifies errors reported only after AEAD verification.
// They still establish authentication for reset-routing precedence.
func authenticatedOpenError(err error) bool {
	return errors.Is(err, packet.ErrReservedBits) || errors.Is(err, errKeyUpdate)
}

// packetOpenFailure counts a packet's failed authentication once, regardless
// of how many current/previous/next key candidates were tried. Nil means drop.
func (c *Conn) packetOpenFailure(err error) error {
	if authenticatedOpenError(err) {
		c.authenticatedPackets++
	}
	if errors.Is(err, packet.ErrAuthentication) {
		c.failedAuth++
		if c.failedAuth >= c.integrityLimit {
			return c.fail(aeadLimitError())
		}
	}
	if errors.Is(err, errKeyUpdate) {
		return c.fail(&TransportError{Code: 0xe, Reason: err.Error()})
	}
	if errors.Is(err, packet.ErrReservedBits) {
		return c.fail(&TransportError{Code: 0xa, Reason: err.Error()})
	}
	return nil
}
