package transport

import (
	"context"
	"errors"
	"time"

	"github.com/getlantern/photon/internal/stream"
	"github.com/getlantern/photon/internal/wire"
)

// A frame must remain portable to any validated 1200-byte path, including
// after migration or MTU fallback. Reserve the maximum short-header CID (20),
// four-byte packet number, AEAD tag, and STREAM ID/offset/length encodings.
// A 0-RTT long header also carries a version, both CIDs and a length.
const portableEarlyStreamPayload = 1200 - (1 + 4 + 1 + 20 + 1 + 20 + 2 + 4 + 16) - (1 + 8 + 8 + 2)

const portableStreamPayload = 1200 - (1 + 20 + 4 + 16) - (1 + 8 + 8 + 2)

type streamState struct {
	priority           streamPriority
	connectionBlocked  bool
	blockedLimit       uint64
	blockedNotified    bool
	onReceiveClosed    func(error)
	onReceiveFinalSize func(uint64)
	receiveTerminal    error
	onSendClosed       func(error)
	sendTerminal       error
	resetError         *StreamError
	onRelease          func(error, error) // opt-in automatic cleanup; callback runs on the owner
	creditReleased     bool

	accepted               bool
	readCanceled           *StreamError
	readTerminal           error
	resetQueued, finQueued bool
	partialReset           *wire.ResetStreamAtFrame
	resetReserved          bool
	reliableBoundary       uint64
	outstandingData        int
	send                   *stream.Send
	receive                *stream.Receive
	window                 uint64
	windowSample           receiveWindowSample
}
type streamSet struct {
	priorityCursor      [16]streamPriorityCursor
	dataBlockedCount    int
	dataBlockedNotified bool
	blockedLimits       [2]struct {
		limit    uint64
		notified bool
	}
	window         uint64
	windowSample   receiveWindowSample
	peerLimits     wire.EarlyLimits
	havePeerLimits bool
	closed         stream.Closed
	partialClosed  *partialResetHistory
	// Released peer streams whose credit awaits ReleaseStreamCredit. Each entry
	// holds one unreturned credit, so the peer's stream limit bounds the set.
	creditWithheld map[uint64]struct{}
	ids            *stream.IDs
	receive        *stream.Flow
	send           *stream.SendFlow
	objects        map[uint64]*streamState
}

func (c *Conn) initStreams() {
	p := c.config.Parameters
	ids, _ := stream.NewIDs(c.config.Server, p.InitialMaxStreamsBidi, p.InitialMaxStreamsUni, 0, 0)
	receive, _ := stream.NewFlow(p.InitialMaxData)
	send, _ := stream.NewSendFlow(0)
	c.streams = streamSet{window: p.InitialMaxData, ids: ids, receive: receive, send: send, objects: make(map[uint64]*streamState)}
}
func (c *Conn) getStream(id uint64) (*streamState, error) {
	if s := c.streams.objects[id]; s != nil {
		return s, nil
	}
	if !c.streams.havePeerLimits {
		return nil, stream.ErrWouldBlock
	}
	local := id&1 == 0
	if c.config.Server {
		local = !local
	}
	uni := id&2 != 0
	s := &streamState{priority: streamPriority{3, true}}
	p := c.config.Parameters
	if !uni || !local {
		limit := p.InitialMaxStreamDataBidiRemote
		if local {
			limit = p.InitialMaxStreamDataBidiLocal
		}
		if uni {
			limit = p.InitialMaxStreamDataUni
		}
		s.receive, _ = stream.NewReceive(c.streams.receive, max(1, int(limit)), limit)
		s.window = limit
	}
	if !uni || local {
		limit := peerStreamSendLimit(c.streams.peerLimits, local, uni)
		s.send, _ = stream.NewSend(id, limit, c.streams.send)
	}
	c.streams.objects[id] = s
	return s, nil
}
func (c *Conn) OpenStream(uni bool) (uint64, error) {
	if err := c.check(); err != nil {
		return 0, err
	}
	if !c.complete && !c.sendingEarly() && !c.serverApplicationReady() {
		return 0, stream.ErrWouldBlock
	}
	id, err := c.streams.ids.NextOpen(uni)
	if err != nil {
		if errors.Is(err, stream.ErrWouldBlock) {
			if signalErr := c.notifyStreamsBlocked(uni); signalErr != nil {
				return 0, signalErr
			}
		}
		return 0, err
	}
	s, err := c.getStream(id)
	if err != nil {
		return 0, err
	}
	if _, err := c.streams.ids.Open(uni); err != nil {
		return 0, c.fail(err)
	}
	s.accepted = true
	return id, nil
}
func (c *Conn) AcceptStream(uni bool) (uint64, error) {
	if err := c.check(); err != nil {
		return 0, err
	}
	id, err := c.streams.ids.NextAccept(uni)
	if err != nil {
		return 0, err
	}
	s, err := c.getStream(id)
	if err != nil {
		return 0, err
	}
	if _, err := c.streams.ids.Accept(uni); err != nil {
		return 0, c.fail(err)
	}
	s.accepted = true
	return id, nil
}

// WriteStream commits at most one bounded frame to reliable output. Partial
// writes must be retried by the caller; fin applies only to the final fragment.
func (c *Conn) WriteStream(id uint64, data []byte, fin bool) (int, error) {
	return c.writeStream(id, data, fin, nil)
}

var ErrWriteLimitReached = errors.New("write limit reached")

// WriteStreamWithLimit applies external credit only to newly admitted bytes.
// The callback runs on the owner and must not block or call connection methods.
func (c *Conn) WriteStreamWithLimit(id uint64, data []byte, limiter func(int) int) (int, error) {
	return c.writeStream(id, data, false, limiter)
}

// SendError reports why stream id's send side can no longer be written, or nil
// while it can. It changes nothing.
func (c *Conn) SendError(id uint64) error {
	if err := c.check(); err != nil {
		return err
	}
	s := c.streams.objects[id]
	if s == nil || s.send == nil {
		return stream.ErrStreamState
	}
	if s.sendTerminal != nil {
		if s.sendTerminal == context.Canceled {
			return stream.ErrSendClosed
		}
		return s.sendTerminal
	}
	return nil
}

func (c *Conn) writeStream(id uint64, data []byte, fin bool, limiter func(int) int) (int, error) {
	if err := c.SendError(id); err != nil {
		return 0, err
	}
	s := c.streams.objects[id]
	if len(data) > 0 {
		streamBlocked := s.send.Offset() == s.send.Limit()
		connectionBlocked := c.streams.send.Used() == c.streams.send.Limit()
		if streamBlocked {
			if err := c.notifyStreamDataBlocked(id, s); err != nil {
				return 0, err
			}
		}
		if connectionBlocked {
			if err := c.notifyDataBlocked(s); err != nil {
				return 0, err
			}
		}
		if streamBlocked || connectionBlocked {
			return 0, stream.ErrWouldBlock
		}
	}
	payloadLimit := portableStreamPayload
	if c.complete && c.multipath == nil {
		payloadLimit += c.basePacketPayloadLimit() - minimumQUICMTU
	}
	if c.sendingEarly() {
		payloadLimit = portableEarlyStreamPayload
	} // Worst-case 0-RTT header and STREAM encodings.
	if c.multipath != nil && c.confirmed {
		if id, ok := c.applicationEnqueuePath(); ok {
			payloadLimit = c.multipath.paths[id].application.streamPayloadLimit()
		}
	}
	reserve := 0
	// A resumed server may enable support that was absent from the ticket.
	// Reserve for early writes too, so later boundary selection stays reliable.
	if !s.resetReserved && (c.peerResetCapability() || c.sendingEarly()) {
		reserve = partialResetControlCharge
	}
	offered := min(len(data), payloadLimit)
	if offered+32+streamSplitReservation(offered)+reserve > c.availableSendBytes() {
		// A portable partial write can still make progress without fragment
		// reservations when the larger admission would exceed the queue limit.
		payloadLimit = min(payloadLimit, portableStreamPayload)
		offered = min(len(data), payloadLimit)
	}
	if offered+32+streamSplitReservation(offered)+reserve > c.availableSendBytes() {
		return 0, stream.ErrWouldBlock
	}
	limited := false
	if limiter != nil && len(data) > 0 {
		if c.multipath != nil && c.confirmed {
			if _, ok := c.survivingPath(^uint64(0)); !ok {
				return 0, c.fail(&TransportError{Code: 0x10, Reason: "no viable path for application output"})
			}
		}
		maximum := int(min(uint64(offered), s.send.Limit()-s.send.Offset(), c.streams.send.Limit()-c.streams.send.Used()))
		allowed := max(0, min(maximum, limiter(maximum)))
		limited = allowed < maximum
		if allowed == 0 {
			return 0, ErrWriteLimitReached
		}
		data = data[:allowed]
		offered = allowed
	}
	storage := c.takeSendBuffer(offered)
	frame, n, err := s.send.WriteBuffer(data, fin, payloadLimit, storage)
	if err != nil {
		c.putSendBuffer(storage)
		return 0, err
	}
	if err := c.enqueue(applicationSpace, frame, n+32+streamSplitReservation(n)); err != nil {
		return n, err
	}
	if reserve != 0 {
		s.resetReserved = true
		c.reservedResetBytes += reserve
	}
	s.outstandingData++
	s.finQueued = frame.Fin
	if frame.Fin {
		c.retireStreamDataBlocked(id)
		c.releaseConnectionBlocked(s)
		s.closeSend(context.Canceled)
	}
	if limited {
		return n, ErrWriteLimitReached
	}
	return n, nil
}
func (c *Conn) ReadStream(id uint64, dst []byte) (int, error) {
	return c.ReadStreamAt(id, dst, time.Now())
}

// ReadStreamAt supplies the consumption timestamp for deterministic engines.
func (c *Conn) ReadStreamAt(id uint64, dst []byte, now time.Time) (int, error) {
	if s := c.completedReads[id]; s != nil {
		n, err := s.receive.Read(dst)
		err = receiveStreamError(id, err)
		if err != nil && !errors.Is(err, stream.ErrWouldBlock) {
			delete(c.completedReads, id)
			if s.onRelease != nil {
				s.notifyRelease(err)
			}
		}
		return n, err
	}
	if err := c.check(); err != nil {
		return 0, err
	}
	s := c.streams.objects[id]
	if s == nil || s.receive == nil {
		return 0, stream.ErrStreamState
	}
	if s.readTerminal != nil {
		return 0, s.readTerminal
	}
	n, err := s.receive.Read(dst)
	err = receiveStreamError(id, err)
	if err != nil && !errors.Is(err, stream.ErrWouldBlock) {
		s.readTerminal = err
	}
	if n > 0 {
		if growthErr := c.growReceiveWindows(s, n, now); growthErr != nil {
			return n, c.fail(growthErr)
		}
		if updateErr := c.updateReceiveCredit(id, s); updateErr != nil {
			return n, updateErr
		}
	}
	if releaseErr := c.autoReleaseStream(id); releaseErr != nil {
		return n, releaseErr
	}
	return n, err
}
func (c *Conn) streamFailure(f wire.Frame, err error) error {
	code := uint64(0xa)
	switch {
	case errors.Is(err, stream.ErrFlowControl):
		code = 3
	case errors.Is(err, stream.ErrStreamLimit):
		code = 4
	case errors.Is(err, stream.ErrStreamState):
		code = 5
	case errors.Is(err, stream.ErrFinalSize):
		code = 6
	}
	return c.fail(&TransportError{Code: code, FrameType: f.Type(), Reason: err.Error()})
}
func (c *Conn) handleStreamFrame(f wire.Frame) error {
	var id uint64
	receive := true
	switch v := f.(type) {
	case wire.StreamFrame:
		id = v.StreamID
	case wire.ResetStreamFrame:
		id = v.StreamID
	case wire.ResetStreamAtFrame:
		id = v.StreamID
	case wire.StreamDataBlockedFrame:
		id = v.StreamID
	case wire.MaxStreamDataFrame:
		id = v.StreamID
		receive = false
	case wire.StopSendingFrame:
		id = v.StreamID
		receive = false
	case wire.MaxDataFrame:
		return c.updateConnectionSendLimit(v.Maximum)
	case wire.MaxStreamsFrame:
		return c.updateSendStreamLimit(v.Unidirectional, v.Maximum)
	case wire.DataBlockedFrame, wire.StreamsBlockedFrame:
		return nil
	default:
		return ErrUnsupportedFrame
	}
	if err := c.streams.ids.Observe(id, receive); err != nil {
		return c.streamFailure(f, err)
	}
	if c.streams.closed.Contains(id) {
		if err := c.streams.partialClosed.validate(id, f); err != nil {
			return c.streamFailure(f, err)
		}
		return nil
	}
	s, err := c.getStream(id)
	if err != nil {
		return c.streamFailure(f, err)
	}
	switch v := f.(type) {
	case wire.StreamFrame:
		err = s.receive.Push(v.Offset, v.Data, v.Fin)
	case wire.ResetStreamFrame:
		err = s.receive.Reset(v.FinalSize, v.ErrorCode)
	case wire.ResetStreamAtFrame:
		err = s.receive.ResetAt(v.FinalSize, v.ReliableSize, v.ErrorCode)
	case wire.MaxStreamDataFrame:
		err = c.updateStreamSendLimit(id, s, v.Maximum)
	case wire.StopSendingFrame:
		err = c.cancelWrite(id, v.ErrorCode, true)
	}
	if err != nil {
		return c.streamFailure(f, err)
	}
	if s.receive != nil && s.onReceiveFinalSize != nil {
		if final, known := s.receive.FinalSize(); known {
			notify := s.onReceiveFinalSize
			s.onReceiveFinalSize = nil
			notify(final)
		}
	}
	if s.receive != nil && s.receive.Complete() {
		s.closeReceive(receiveStreamError(id, s.receive.TerminalError()))
		c.retireStreamStop(id)
		if s.readCanceled != nil {
			s.receive.DiscardComplete()
			if err := c.updateReceiveCredit(id, s); err != nil {
				return err
			}
		}
	}
	switch f.(type) {
	case wire.ResetStreamFrame, wire.ResetStreamAtFrame:
		if err := c.updateReceiveCredit(id, s); err != nil {
			return err
		}
	}
	return c.autoReleaseStream(id)
}

func creditTarget(consumed, window uint64) uint64 {
	return consumed + min(window, wire.MaxVarint-consumed)
}
func (c *Conn) updateReceiveCredit(id uint64, s *streamState) error {
	if _, final := s.receive.FinalSize(); !final {
		target := creditTarget(s.receive.Offset(), s.window)
		if target-s.receive.Limit() >= max(uint64(1), s.window/2) {
			if err := c.queueCredit(wire.MaxStreamDataFrame{StreamID: id, Maximum: target}); err != nil {
				return err
			}
			if err := s.receive.SetLimit(target); err != nil {
				return c.fail(err)
			}
		}
	}
	flow := c.streams.receive
	window := c.streams.window
	if window == 0 {
		window = c.config.Parameters.InitialMaxData
	}
	target := creditTarget(flow.Consumed(), window)
	if target-flow.Limit() >= max(uint64(1), window/2) {
		if err := c.queueCredit(wire.MaxDataFrame{Maximum: target}); err != nil {
			return err
		}
		if err := flow.SetLimit(target); err != nil {
			return c.fail(err)
		}
	}
	return nil
}

// A newer absolute credit limit supersedes older reliable updates. Retiring
// those frame items does not remove their transmissions from flight accounting.
func (c *Conn) queueCredit(frame wire.Frame) error {
	same := func(old wire.Frame) bool {
		switch fresh := frame.(type) {
		case wire.MaxDataFrame:
			_, ok := old.(wire.MaxDataFrame)
			return ok
		case wire.MaxStreamsFrame:
			prior, ok := old.(wire.MaxStreamsFrame)
			return ok && prior.Unidirectional == fresh.Unidirectional
		case wire.MaxStreamDataFrame:
			prior, ok := old.(wire.MaxStreamDataFrame)
			return ok && prior.StreamID == fresh.StreamID
		}
		return false
	}
	c.retireFrames(same)
	return c.enqueue(applicationSpace, frame, 32)
}
func (c *Conn) retireFrames(same func(wire.Frame) bool) {
	if c.multipath != nil && c.confirmed {
		for _, state := range c.multipath.paths {
			if state.application != nil {
				c.retirePathFrames(state.application.space, same)
			}
		}
		return
	}
	c.retirePathFrames(&c.spaces[applicationSpace], same)
}
func (c *Conn) retirePathFrames(space *numberSpace, same func(wire.Frame) bool) {
	retire := func(item *sendItem) {
		if !item.acked && same(item.frame) {
			c.queuedBytes -= retireSendItem(item)
		}
	}
	for _, item := range space.queue {
		retire(item)
	}
	for pn, item := range space.sent {
		retire(item)
		if item.acked {
			delete(space.sent, pn)
		}
	}
	queue := space.queue[:0]
	for _, item := range space.queue {
		if !item.acked {
			queue = append(queue, item)
		}
	}
	clear(space.queue[len(queue):])
	space.queue = queue
}
