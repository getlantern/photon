package transport

import (
	"errors"
	"time"

	"github.com/getlantern/photon/internal/packet"
	"github.com/getlantern/photon/internal/wire"
)

// ErrKeyUpdate reports a key update that is not allowed now, or a key phase
// change the peer was not allowed to make. A local update is refused before
// the handshake is confirmed; before the current keys' first packet from the
// peer arrives; until a packet sent with the current keys is acknowledged (RFC
// 9001 section 6.1); with a zero time; and, on a multipath connection, until
// three PTOs have passed since that acknowledgment. Only the acknowledgment
// case is helped by sending ack-eliciting data.
var ErrKeyUpdate = errors.New("quic: invalid application key update")

// keyPhaseNumbers contains only one path's packet-number observations. Keys,
// phase transitions, acknowledgment gates and expiry are connection-wide.
type keyPhaseNumbers struct {
	firstSent, largest, previousLargest, first uint64
	haveLargest, havePrevious, haveFirst       bool
}

type applicationKeys struct {
	keyPhaseNumbers                 // Initial path; additional paths store their own observations.
	awaitingRead, writeAcknowledged bool
	read, write, next, previous     *packet.Keys
	phase, updated, ackSent         bool
	previousExpiry                  time.Time
	updateNotBefore                 time.Time
	elicitedAt                      time.Time // last PING queued to unblock an update
}

func (c *Conn) keyNumbers(pathID uint32) *keyPhaseNumbers {
	if pathID == 0 {
		return &c.applicationKeys.keyPhaseNumbers
	}
	if c.multipath != nil {
		if s := c.multipath.paths[uint64(pathID)]; s != nil && s.application != nil {
			return &s.application.keyNumbers
		}
	}
	return nil
}

func (c *Conn) advanceKeyNumbers() {
	advance := func(n *keyPhaseNumbers, nextPN uint64) {
		n.previousLargest, n.havePrevious = n.largest, n.haveLargest
		n.firstSent = nextPN
		n.haveFirst, n.haveLargest = false, false
	}
	advance(&c.applicationKeys.keyPhaseNumbers, c.spaces[applicationSpace].nextPN)
	if c.multipath != nil {
		for id, s := range c.multipath.paths {
			if id != 0 && s.application != nil {
				advance(&s.application.keyNumbers, s.application.space.nextPN)
			}
		}
	}
}

func (c *Conn) prepareApplicationKeys() error {
	k := &c.applicationKeys
	if k.read != nil {
		return nil
	}
	read, write := c.tls.ReadKeys(levels[applicationSpace]), c.tls.WriteKeys(levels[applicationSpace])
	if read == nil || write == nil {
		return nil
	}
	next, err := read.Next()
	if err != nil {
		return err
	}
	k.read, k.write, k.next = read, write, next
	return nil
}
func (c *Conn) openApplication(raw []byte, pnOffset int, expected uint64, now time.Time) ([]byte, []byte, uint64, error) {
	return c.openApplicationPath(0, raw, pnOffset, expected, now)
}

// openApplicationPath requires CID routing to an allocated path's
// context. It does not allocate path state from an untrusted packet's CID.
func (c *Conn) openApplicationPath(pathID uint32, raw []byte, pnOffset int, expected uint64, now time.Time) ([]byte, []byte, uint64, error) {
	return c.openApplicationPathInto(nil, pathID, raw, pnOffset, expected, now)
}

// dst is scoped to synchronous processing; retained path evidence uses nil.
func (c *Conn) openApplicationPathInto(dst []byte, pathID uint32, raw []byte, pnOffset int, expected uint64, now time.Time) ([]byte, []byte, uint64, error) {
	numbers := c.keyNumbers(pathID)
	if numbers == nil {
		return nil, nil, 0, packet.ErrPacket
	}
	open := func(keys *packet.Keys) ([]byte, []byte, uint64, error) {
		if keys == nil {
			return nil, nil, 0, packet.ErrPacket
		}
		if pathID == 0 {
			return keys.OpenInto(dst, raw, pnOffset, expected)
		}
		return keys.OpenPath(raw, pnOffset, expected, pathID)
	}
	k := &c.applicationKeys
	if err := c.prepareApplicationKeys(); err != nil {
		return nil, nil, 0, err
	}
	c.expirePreviousKeys(now)
	header, plain, pn, err := open(k.read)
	if err == nil {
		if (header[0]&4 != 0) != k.phase || (k.updated && numbers.havePrevious && pn <= numbers.previousLargest) {
			return nil, nil, 0, ErrKeyUpdate
		}
		if k.awaitingRead {
			k.awaitingRead = false
			c.armPreviousKeys(now)
		}
		if !numbers.haveFirst || pn < numbers.first {
			numbers.first = pn
			numbers.haveFirst = true
		}
		if !numbers.haveLargest || pn > numbers.largest {
			numbers.largest = pn
			numbers.haveLargest = true
		}
		return header, plain, pn, nil
	}
	if errors.Is(err, packet.ErrReservedBits) {
		return nil, nil, 0, err
	}
	authFailed := errors.Is(err, packet.ErrAuthentication)
	if k.previous != nil {
		h, p, n, e := open(k.previous)
		if e == nil {
			if (h[0]&4 != 0) == k.phase || (numbers.haveFirst && n >= numbers.first) {
				return nil, nil, 0, ErrKeyUpdate
			}
			if !numbers.havePrevious || n > numbers.previousLargest {
				numbers.previousLargest = n
				numbers.havePrevious = true
			}
			return h, p, n, nil
		}
		authFailed = authFailed || errors.Is(e, packet.ErrAuthentication)
		if errors.Is(e, packet.ErrReservedBits) {
			return nil, nil, 0, e
		}
	}
	h, p, n, e := open(k.next)
	if e != nil {
		if errors.Is(e, packet.ErrReservedBits) {
			return nil, nil, 0, e
		}
		if authFailed || errors.Is(e, packet.ErrAuthentication) {
			return nil, nil, 0, packet.ErrAuthentication
		}
		return nil, nil, 0, e
	}
	if !c.confirmed || (h[0]&4 != 0) == k.phase || (numbers.haveLargest && n <= numbers.largest) || (k.updated && !k.ackSent) || k.awaitingRead {
		return nil, nil, 0, ErrKeyUpdate
	}
	write, e := k.write.Next()
	if e != nil {
		return nil, nil, 0, e
	}
	next, e := k.next.Next()
	if e != nil {
		return nil, nil, 0, e
	}
	k.previous, k.read, k.next, k.write = k.read, k.next, next, write
	c.sealed[applicationSpace] = 0
	k.phase = !k.phase
	c.advanceKeyNumbers()
	k.writeAcknowledged = false
	k.updateNotBefore = time.Time{}
	numbers.first = n
	numbers.haveFirst = true
	numbers.largest = n
	numbers.haveLargest = true
	k.updated = true
	k.ackSent = false
	c.armPreviousKeys(now)
	return h, p, n, nil
}
func (c *Conn) armPreviousKeys(now time.Time) {
	k := &c.applicationKeys
	maxDelay := 25 * time.Millisecond
	if c.peerParams != nil {
		maxDelay = time.Duration(c.peerParams.MaxAckDelay) * time.Millisecond
	}
	duration := triple(c.retentionPTO(true, maxDelay))
	k.previousExpiry = now.Add(duration)
}
func (c *Conn) noteKeyPhaseACK(f wire.Frame) {
	var pathID uint64
	var ack wire.AckFrame
	switch frame := f.(type) {
	case wire.AckFrame:
		ack = frame
	case wire.PathAckFrame:
		pathID = frame.PathID
		ack = frame.AckFrame
	default:
		return
	}
	if len(ack.Ranges) > 0 {
		c.noteKeyPhaseACKLargest(pathID, ack.Ranges[0].Largest)
	}
}

// noteKeyPhaseACKLargest records an ACK sent on pathID whose largest
// acknowledged packet number is largest.
func (c *Conn) noteKeyPhaseACKLargest(pathID, largest uint64) {
	if pathID > 1<<32-1 {
		return
	}
	numbers := c.keyNumbers(uint32(pathID))
	if numbers != nil && numbers.haveFirst && !c.applicationKeys.awaitingRead && c.applicationKeys.updated && largest >= numbers.first {
		c.applicationKeys.ackSent = true
	}
}

func (c *Conn) expirePreviousKeys(now time.Time) {
	k := &c.applicationKeys
	if k.previous != nil && !k.previousExpiry.IsZero() && !now.Before(k.previousExpiry) {
		k.previous = nil
		k.previousExpiry = time.Time{}
	}
}

// InitiateKeyUpdate queues an ack-eliciting packet under the next application
// keys. Subsequent updates require acknowledgment of the current sending phase.
// Previous receive keys remain usable until the peer answers in the new phase.
func (c *Conn) InitiateKeyUpdate() error {
	return c.InitiateKeyUpdateAt(time.Now())
}

// InitiateKeyUpdateAt uses the owner's clock for deterministic multipath update
// spacing. The first update requires confirmation; later updates additionally
// wait three times the largest path PTO after acknowledgment of the current keys.
func (c *Conn) InitiateKeyUpdateAt(now time.Time) error {
	if err := c.check(); err != nil {
		return err
	}
	k := &c.applicationKeys
	if !c.confirmed || k.awaitingRead || (k.updated && !k.writeAcknowledged) {
		return ErrKeyUpdate
	}
	if now.IsZero() || (c.multipath != nil && k.updated && (k.updateNotBefore.IsZero() || now.Before(k.updateNotBefore))) {
		return ErrKeyUpdate
	}
	if c.availableSendBytes() < 1 {
		return ErrSendBuffer
	}
	if err := c.prepareApplicationKeys(); err != nil {
		return c.fail(err)
	}
	write, err := k.write.Next()
	if err != nil {
		return c.fail(err)
	}
	next, err := k.next.Next()
	if err != nil {
		return c.fail(err)
	}
	k.previous, k.read, k.next, k.write = k.read, k.next, next, write
	c.sealed[applicationSpace] = 0
	k.phase = !k.phase
	k.updated = true
	k.awaitingRead = true
	k.ackSent = false
	c.advanceKeyNumbers()
	k.writeAcknowledged = false
	k.updateNotBefore = time.Time{}
	k.previousExpiry = time.Time{}
	return c.enqueue(applicationSpace, wire.PingFrame{}, 1)
}

// noteKeyPhaseWriteACK is called only after ACK-of-unsent validation. An ACK
// confirms our sending phase using the named path's first-sent threshold.
func (c *Conn) noteKeyPhaseWriteACK(f wire.Frame, now time.Time) {
	var id uint64
	var ack wire.AckFrame
	switch f := f.(type) {
	case wire.AckFrame:
		ack = f
	case wire.PathAckFrame:
		id = f.PathID
		ack = f.AckFrame
	default:
		return
	}
	if id > 1<<32-1 {
		return
	}
	n := c.keyNumbers(uint32(id))
	if n != nil && len(ack.Ranges) > 0 && ack.Ranges[0].Largest >= n.firstSent {
		k := &c.applicationKeys
		if !k.writeAcknowledged && k.updated && c.multipath != nil {
			maxDelay := 25 * time.Millisecond
			if c.peerParams != nil {
				maxDelay = time.Duration(c.peerParams.MaxAckDelay) * time.Millisecond
			}
			k.updateNotBefore = now.Add(triple(c.retentionPTO(true, maxDelay)))
		}
		k.writeAcknowledged = true
	}
}

// updateKeysBeforeLimit updates the write keys well before their AEAD limit,
// once they reach 3/4 of it, so the update the limit would force is never
// needed. If the update is refused because nothing sent with the current keys
// has been acknowledged yet (RFC 9001 section 6.1), which happens after the
// peer updated keys and this endpoint has sent only ACKs since, it queues a
// PING for the peer to acknowledge, at most once per PTO and only when the
// send buffer has room. Other refusals, such as multipath update spacing, are
// waited out; the remaining quarter of the limit leaves time for them.
func (c *Conn) updateKeysBeforeLimit(now time.Time) error {
	if !c.confirmed {
		return nil
	}
	keys := c.writeKeys(applicationSpace)
	if keys == nil {
		return nil
	}
	limit, _ := keys.UsageLimits()
	if c.sealed[applicationSpace] < limit-limit/4 {
		return nil
	}
	err := c.InitiateKeyUpdateAt(now)
	if err == nil || errors.Is(err, ErrSendBuffer) {
		return nil
	}
	if !errors.Is(err, ErrKeyUpdate) {
		return err
	}
	k := &c.applicationKeys
	if !k.updated || (k.writeAcknowledged && !k.awaitingRead) || c.pingQueued() {
		return nil
	}
	maxDelay := c.keepAliveACKDelay()
	pto := c.recovery.RTT.PTO(true, true, maxDelay, 0)
	var selected uint64
	if c.multipath != nil {
		var found bool
		if selected, pto, found = c.keepAlivePath(maxDelay); !found {
			return nil
		}
	}
	if !k.elicitedAt.IsZero() && now.Before(k.elicitedAt.Add(pto)) {
		return nil
	}
	if c.availableSendBytes() <= 0 {
		return nil
	}
	k.elicitedAt = now
	if c.multipath == nil {
		err = c.enqueue(applicationSpace, wire.PingFrame{}, 1)
	} else {
		err = c.queueFrameOnPath(selected, wire.PingFrame{})
	}
	if errors.Is(err, ErrSendBuffer) {
		return nil
	}
	return err
}

// pingQueued reports whether a PING is already queued in the application
// space, on any path.
func (c *Conn) pingQueued() bool {
	queued := func(queue []*sendItem) bool {
		for _, item := range queue {
			if _, ok := item.frame.(wire.PingFrame); ok && item.queued {
				return true
			}
		}
		return false
	}
	if queued(c.spaces[applicationSpace].queue) {
		return true
	}
	if c.multipath != nil {
		for _, state := range c.multipath.paths {
			if app := state.application; app != nil && queued(app.space.queue) {
				return true
			}
		}
	}
	return false
}
