package transport

import (
	"github.com/getlantern/photon/internal/recovery"
	"time"
)

// Draining reports whether the socket router must still reserve this
// connection's routing identity while discarding input and sending nothing.
func (c *Conn) Draining() bool { return !c.drainUntil.IsZero() && !c.closed }
func (c *Conn) beginDraining(err error, now time.Time) error {
	if c.Draining() {
		return c.err
	}
	maxDelay := 25 * time.Millisecond
	if c.peerParams != nil {
		maxDelay = time.Duration(c.peerParams.MaxAckDelay) * time.Millisecond
	}
	c.drainUntil = now.Add(triple(c.retentionPTO(true, maxDelay)))
	if c.Closing() {
		c.drainUntil = c.closing.until
		c.closing = nil
	}
	c.fail(err)
	if c.pendingWrite != nil {
		c.pendingWrite.release()
	}
	c.retainCompletedReads()
	c.releaseTransportBuffers()
	return c.err
}

// Packet and active stream resources are discarded as draining starts. Completed
// accepted reads, if any, have separately retained their receive buffers.
// Routing identities remain until the deadline so late traffic cannot create
// another connection or trigger a stateless reset loop.
func (c *Conn) releaseTransportBuffers() {
	c.allowConnectionWindowIncrease = nil
	c.baseMTUChallenge = nil
	c.onHandshakeComplete, c.onHandshakeConfirmed = nil, nil
	c.discardEarlyKeys()
	clear(c.payloadScratch[:])
	clear(c.decodeScratch)
	c.decodeScratch = nil
	for _, buffer := range c.sendBuffers {
		clear(buffer[:cap(buffer)])
	}
	c.sendBuffers = nil
	c.cachedSendBufferBytes = 0
	c.candidateInitial = candidateInitialKeys{}
	c.initialRead, c.initialWrite, c.originalInitialRead = nil, nil, nil
	c.applicationKeys = applicationKeys{}
	for i := range c.spaces {
		c.spaces[i].queue = nil
		c.spaces[i].sent = nil
		c.spaces[i].received = nil
		c.spaces[i].ackPending = false
		c.spaces[i].discarded = true
		c.recovery.Discard(recovery.Space(i))
		c.tls.DiscardKeys(levels[i])
	}
	c.streams = streamSet{}
	c.validation = pathValidation{}
	c.datagrams = datagramBuffer{}
	c.pendingDatagrams = 0
	c.deferred = nil
	c.deferredBytes = 0
	c.pendingMultipath = nil
	c.pendingMultipathBytes = 0
	c.queuedBytes = 0
	c.reservedResetBytes = 0
	c.timer.DiscardKeys()
	c.pacingAt = time.Time{}
	c.probeAllowance = [3]bool{}
	if c.multipath != nil {
		for _, state := range c.multipath.paths {
			app := state.application
			if app == nil {
				continue
			}
			// Preserve CID/address routing while draining, but release every
			// path's payloads, received ranges, recovery ledger and probe state.
			*app.space = numberSpace{discarded: true}
			app.sent.Discard(recovery.Application)
			app.timer.DiscardKeys()
			*app.validation = pathValidation{}
			*app.budget = Amplification{}
			app.keyNumbers = keyPhaseNumbers{}
			app.probePending = false
			app.migration = nil
			app.localMigration = nil
			app.migrationResponse = nil
			app.pacingAt = time.Time{}
		}
	}
}

// An ACK proves delivery to the transport, not consumption by the application.
// Preserve already accepted, complete receive buffers across peer close. Reuse
// existing bounded storage, retaining no packet, send, or cryptographic state.
func (c *Conn) retainCompletedReads() {
	for id, s := range c.streams.objects {
		if !s.accepted || s.receive == nil || !s.receive.Complete() || s.receive.Drained() || s.readTerminal != nil {
			continue
		}
		if c.completedReads == nil {
			c.completedReads = make(map[uint64]*streamState)
		}
		c.completedReads[id] = &streamState{receive: s.receive, onRelease: s.onRelease, resetError: s.resetError}
	}
}
