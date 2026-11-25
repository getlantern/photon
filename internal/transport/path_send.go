package transport

import (
	"encoding/binary"
	"errors"
	"time"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
)

// nextPacketOnPath commits one 1-RTT packet for a prepared path. The owner must
// immediately transmit returned bytes on that path's bound network tuple or
// report write failure through the endpoint. Nil means blocked or no output.
// Path selection and validation timer servicing belong to the scheduler.
func (c *Conn) nextPacketOnPath(id uint64, now time.Time) ([]byte, error) {
	return c.nextPacketOnPathACK(id, id, now)
}

// ackPath selects the received number space; id selects encryption, routing,
// send packet numbers, amplification and flight accounting.
func (c *Conn) nextPacketOnPathACK(id, ackPath uint64, now time.Time) ([]byte, error) {
	return c.nextPacketOnPathMode(id, ackPath, now, true)
}

// allowMTU lets the scheduler defer discovery until ordinary output is blocked.
func (c *Conn) nextPacketOnPathMode(id, ackPath uint64, now time.Time, allowMTU bool) ([]byte, error) {
	return c.nextPacketOnPathSelection(id, ackPath, now, allowMTU, true)
}
func (c *Conn) nextPacketOnPathSelection(id, ackPath uint64, now time.Time, allowMTU, allowResponses bool) ([]byte, error) {
	c.datagramECN = recovery.NotECT
	if err := c.check(); err != nil {
		return nil, err
	}
	if c.multipath == nil || !c.confirmed {
		return nil, ErrHandshakeUnconfirmed
	}
	state := c.multipath.paths[id]
	if state == nil || state.application == nil || c.multipath.abandoned[id] != nil {
		return nil, errNoPathForControl
	}
	peer, ok := state.peer.active[state.peer.current]
	if !ok || len(peer.cid) == 0 {
		return nil, nil
	}
	app := state.application
	// Once migration sends a non-probing packet, newer ordinary traffic on the
	// old tuple would cancel the peer's nomination and consume its reserved CID.
	// Hold this path's ordinary output until commit or cancellation. Candidate
	// challenge/response output uses its separate sender; other paths can run.
	// Still answer old-tuple challenges: the peer may not have completed its
	// independent validation yet and can reject migration until it gets proof.
	// PATH_RESPONSE and padding are probing, so they cannot cancel nomination.
	// Preferred-address validation sends only probing frames, so the established
	// destination must remain usable until proof permits switching.
	if candidate := app.localMigration; candidate != nil && !candidate.preferred && !candidate.explicitSwitch && candidate.validation.state != PathValidationFailed {
		if !allowResponses || ackPath != id || !app.hasPathResponse() {
			return nil, nil
		}
	}
	if app.mtuNeedsValidation && app.validation.state != PathValidating && app.validation.state != PathValidationFailed {
		if err := c.validateMultipathPath(id); err != nil {
			return nil, c.fail(err)
		}
	}
	s := app.space
	app.pacingAt = time.Time{}
	haveQueued := c.selectPublishedQueue(s)
	for haveQueued {
		item := s.queue[0]
		if !item.acked {
			frame, datagram := item.frame.(wire.DatagramFrame)
			if !datagram || datagramFitsPath(frame, app.packetPayloadLimit(), len(peer.cid)) {
				break
			}
			// Unreliable messages cannot be split after a path-size reduction.
			c.queuedBytes -= item.bytes
			c.pendingDatagrams--
			item.acked, item.queued, item.frame = true, false, nil
		}
		s.queue[0] = nil
		s.queue = s.queue[1:]
		haveQueued = c.selectPublishedQueue(s)
	}
	// A late ACK can complete the selected retransmission before the probe
	// leaves. Re-select useful data (or PING) without consuming another PTO.
	if !app.hasPathResponse() && app.probePending && app.budget.CanSend(1+len(peer.cid)+4+16+1) {
		if err := c.queuePathProbe(id); err != nil {
			return nil, c.fail(err)
		}
	}
	haveQueued = c.selectPublishedQueue(s)
	c.prioritizeStreams(s)
	mtuSize := 0
	var f wire.Frame
	var item *sendItem
	ackState := c.multipath.paths[ackPath]
	if ackState == nil || ackState.application == nil {
		return nil, errNoPathForControl
	}
	ackApp := ackState.application
	ackApp.space.expireACKDelay(now) // a send at or after the deadline carries the ACK
	if ackPath != id && !ackApp.space.ackPending {
		return nil, nil
	}
	reservedResponse := false
	response := allowResponses && ackPath == id && app.migrationResponse != nil
	if response {
		f = *app.migrationResponse
	} else if allowResponses && ackPath == id && app.controlResponses.count > 0 {
		f = app.controlResponses.frames[0]
		reservedResponse = true
	} else if allowResponses && ackPath == id && app.queuedPathResponse() {
		item = s.queue[0]
		f = item.frame
	} else if ackApp.space.ackPending && (!allowResponses || !(ackPath == id && app.probePending && haveQueued)) {
		ack, err := ackApp.ack(ackPath, now, uint8(c.config.Parameters.AckDelayExponent))
		if err != nil {
			return nil, c.fail(err)
		}
		f = ack
	} else if app.mtuChallenge != nil {
		f = *app.mtuChallenge
	} else if haveQueued {
		item = s.queue[0]
		f = item.frame
	} else if allowMTU && app.mtu != nil && (id == 0 || app.validation.state == PathValidated) {
		var ok bool
		mtuSize, ok = app.mtu.next()
		if !ok {
			return nil, nil
		}
		if now.Before(app.mtuNextProbe) {
			app.pacingAt = app.mtuNextProbe
			return nil, nil
		}
		f = wire.PingFrame{}
	} else {
		return nil, nil
	}
	if (id != 0 || app.mtuNeedsValidation) && app.validation.state != PathValidated {
		switch f.(type) {
		case wire.StreamFrame, wire.DatagramFrame:
			return nil, nil
		}
	}
	if frame, ok := f.(wire.StreamFrame); ok && len(frame.Data) > app.streamPayloadLimit() {
		if err := c.splitQueuedStream(s, 0, portableStreamPayload); err != nil {
			return nil, c.fail(err)
		}
		item = s.queue[0]
		f = item.frame
	}
	if err := c.prepareApplicationKeys(); err != nil {
		return nil, c.fail(err)
	}
	keys := c.writeKeys(applicationSpace)
	if keys == nil {
		return nil, nil
	}
	limit, _ := keys.UsageLimits()
	if c.sealed[applicationSpace] >= limit {
		if err := c.InitiateKeyUpdateAt(now); err != nil {
			return nil, c.fail(aeadLimitError())
		}
		keys = c.writeKeys(applicationSpace)
	}
	if s.nextPN >= wire.MaxVarint {
		return nil, c.exhaustPacketNumbers()
	}
	if item != nil && len(s.sent) >= maxSentPackets {
		return nil, c.fail(ErrSendBuffer)
	}
	f = c.pathWireFrame(f)
	payload, err := f.Append(c.payloadScratch[:0])
	if err != nil {
		return nil, c.fail(err)
	}
	if c.multipath.baseOnly {
		if err := wire.ValidateFrame(f, wire.FrameContext{PacketType: wire.OneRTT, SenderIsServer: c.config.Server, ResetStreamAtEnabled: c.peerParams.ResetStreamAt, MaxDatagramFrameSize: c.peerParams.MaxDatagramFrameSize}, len(payload)); err != nil {
			return nil, c.fail(err)
		}
	}
	first := c.greaseQUICBit(0x43)
	if c.applicationKeys.phase {
		first |= 4
	}
	header := append([]byte{first}, peer.cid...)
	header = binary.BigEndian.AppendUint32(header, uint32(s.nextPN))
	inFlight := wire.InFlightFrame(f)
	probe := app.probePending && wire.AckEliciting(f)
	bundled, bundledLargest := false, uint64(0)
	if mtuSize == 0 && ackPath == id && ackBundleable(ackApp.space, item, f) {
		if payload, bundledLargest, bundled, err = c.bundlePathACK(app, ackPath, f, len(header), inFlight, probe, now); err != nil {
			return nil, c.fail(err)
		}
	}
	size := len(header) + len(payload) + 16
	if mtuSize != 0 {
		if size > mtuSize {
			return nil, c.fail(errors.New("quic: MTU probe smaller than header"))
		}
		payload = append(payload, make([]byte, mtuSize-size)...)
		size = mtuSize
	}
	if mtuSize == 0 && size > app.packetPayloadLimit() {
		return nil, c.fail(errors.New("quic: multipath frame exceeds packet size"))
	}
	if (f.Type() == 0x1a || f.Type() == 0x1b) && app.budget.CanSend(1200) {
		payload = append(payload, make([]byte, 1200-size)...)
		size = 1200
	}
	// A blocked response must not suppress non-congestion-controlled ACKs.
	fallbackACK := func() ([]byte, error) {
		if allowResponses && f.Type() == 0x1b && ackApp.space.ackPending {
			return c.nextPacketOnPathSelection(id, ackPath, now, false, false)
		}
		return nil, nil
	}
	if !app.budget.CanSend(size) || !app.congestion.CanSend(app.sent.BytesInFlight, size, inFlight, probe) || !app.sent.CanRecord(recovery.Application, inFlight) {
		return fallbackACK()
	}
	if inFlight && !probe {
		if delay := app.pacer.Delay(now, app.congestion.Window(), app.sent.RTT.Smoothed, size); delay > 0 {
			b, err := fallbackACK()
			app.pacingAt = now.Add(delay)
			return b, err
		}
	}
	if c.packetRoutes != nil && (app.network == nil || !c.packetRoutes.reserve(*app.network)) {
		app.routeBlocked = true
		return nil, nil
	}
	pn := s.nextPN
	out, err := keys.SealPathInto(c.sealStorage, header, payload, pn, uint32(id))
	if err != nil {
		return nil, c.fail(err)
	}
	_, app.lastSentDatagram = f.(wire.DatagramFrame)
	c.sealed[applicationSpace]++
	if err := app.budget.Sent(len(out)); err != nil {
		return nil, c.fail(err)
	}
	if c.config.enableECN {
		app.sent.EnableECN()
	}
	marking := app.sent.ECNMarkingAt(recovery.Application, now)
	if err := app.recordSent(recovery.Packet{ECN: marking, Number: pn, SentAt: now, Bytes: len(out), AckEliciting: wire.AckEliciting(f), InFlight: inFlight, MTUProbe: mtuSize != 0}); err != nil {
		return nil, c.fail(err)
	}
	c.datagramECN = marking
	c.datagramECNCommit = ecnCommit{app.sent, recovery.Application, pn}
	if mtuSize != 0 {
		if err := app.mtu.sent(pn, mtuSize); err != nil {
			return nil, c.fail(err)
		}
		// This is an additional discovery rate limit, not RFC 8899's probe
		// timeout. Packet-loss and flight accounting remain with QUIC recovery.
		app.mtuNextProbe = now.Add(max(time.Second, app.sent.RTT.Smoothed))
		app.mtuTimeoutAt = now.Add(max(16*time.Second, triple(app.sent.RTT.PTO(true, true, time.Duration(c.peerParams.MaxAckDelay)*time.Millisecond, 0))))
	}
	if inFlight {
		app.pacer.Sent(len(out))
	}
	peer.used = true
	state.peer.active[state.peer.current] = peer
	c.noteKeyPhaseACK(f)
	c.lastPathACK.bundled, c.lastPathACK.ackSent, c.lastPathACK.pathID, c.lastPathACK.number, c.lastPathACK.phase = bundled, c.applicationKeys.ackSent, id, pn, c.applicationKeys.generation
	if bundled {
		ackApp.space.clearACK()
		c.noteKeyPhaseACKLargest(ackPath, bundledLargest)
	}
	if item != nil {
		switch f := f.(type) {
		case wire.DatagramFrame:
			c.queuedBytes -= item.bytes
			c.pendingDatagrams--
		case wire.PathChallengeFrame:
			app.validation.recordSent(f, len(out), now)
			c.queuedBytes -= item.bytes
		case wire.PathResponseFrame:
			c.queuedBytes -= item.bytes
		default:
			s.sent[pn] = item
		}
		switch f := f.(type) {
		case wire.NewConnectionIDFrame:
			c.multipath.markSent(0, f.Sequence)
		case wire.PathNewConnectionIDFrame:
			c.multipath.markSent(f.PathID, f.Sequence)
		}
		c.commitStreamPriority(item)
		item.queued = false
		s.queue[0] = nil
		s.queue = s.queue[1:]
	} else if response {
		app.migrationResponse = nil
	} else if reservedResponse {
		app.controlResponses.pop()
	} else if challenge, ok := f.(wire.PathChallengeFrame); ok {
		app.validation.recordSent(challenge, len(out), now)
		app.mtuChallenge = nil
	} else if mtuSize == 0 {
		ackApp.space.clearACK()
	}
	c.sentIdleActivity(now, wire.AckEliciting(f))
	if probe {
		app.probePending = false
	}
	maxDelay := time.Duration(c.peerParams.MaxAckDelay) * time.Millisecond
	app.arm(now, maxDelay, !app.budget.CanSend(1+len(peer.cid)+4+16+1))
	c.sentTotals.emitted(len(out))
	c.publishStatistics()
	return out, nil
}
