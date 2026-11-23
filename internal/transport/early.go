package transport

import (
	"crypto/tls"
	"errors"
	"github.com/getlantern/photon/internal/recovery"
	"time"

	"github.com/getlantern/photon/internal/packet"
	"github.com/getlantern/photon/internal/wire"
)

// RFC 9001 section 4.9.3: retain reordered early packets for three PTOs
// from the first authenticated 1-RTT packet. Later packets never extend this.
func (c *Conn) startEarlyKeyRetirement(now time.Time) {
	if !c.config.Server || !c.earlyReadExpiry.IsZero() || c.tls.ReadKeys(tls.QUICEncryptionLevelEarly) == nil {
		return
	}
	delay := 25 * time.Millisecond
	if c.peerParams != nil {
		delay = time.Duration(c.peerParams.MaxAckDelay) * time.Millisecond
	}
	c.earlyReadExpiry = now.Add(triple(c.retentionPTO(true, delay)))
}

func (c *Conn) expireEarlyKeys(now time.Time) {
	if !c.earlyReadExpiry.IsZero() && !now.Before(c.earlyReadExpiry) {
		c.discardEarlyKeys()
	}
}

func (c *Conn) discardEarlyKeys() {
	c.tls.DiscardKeys(tls.QUICEncryptionLevelEarly)
	c.earlyReadExpiry = time.Time{}
}

// Packet numbers are shared, but early and application protection keys are not.
func (c *Conn) packetReadKeys(kind wire.PacketType, space int) *packet.Keys {
	if kind == wire.ZeroRTT {
		if !c.config.Server || !c.config.enableEarlyData {
			return nil
		}
		return c.tls.ReadKeys(tls.QUICEncryptionLevelEarly)
	}
	return c.readKeys(space)
}

// ErrEarlyDataRejected terminates the early stream generation. The eventual
// public adapter must invalidate its handles before exposing reused stream IDs.
var ErrEarlyDataRejected = errors.New("quic: early data rejected")

func (c *Conn) sendingEarly() bool {
	return c.config.enableEarlyData && !c.config.Server && !c.earlyRejected &&
		c.tls.WriteKeys(tls.QUICEncryptionLevelApplication) == nil &&
		c.tls.WriteKeys(tls.QUICEncryptionLevelEarly) != nil
}

func (c *Conn) syncClientEarly() error {
	if c.config.Server || !c.config.enableEarlyData {
		return nil
	}
	if c.tls.EarlyDataRejected() && !c.earlyRejected {
		c.earlyRejected = true
		c.rejectEarlyStreams()
	}
	if !c.earlyInitialized && c.sendingEarly() {
		p, ok := c.tls.ResumedTransport()
		if !ok {
			return wire.ErrEarlyLimits
		}
		if err := c.applyPeerStreamLimits(p.EarlyLimits); err != nil {
			return err
		}
		c.earlyInitialized = true
	}
	return nil
}

func (c *Conn) rejectEarlyStreams() {
	// TLS rejects before the client can send 1-RTT. All application output
	// belongs to the rejected generation; never replay it automatically.
	s := &c.spaces[applicationSpace]
	c.earlyRejectedPackets = s.nextPN
	seen := make(map[*sendItem]bool)
	for _, item := range s.queue {
		seen[item] = true
	}
	for _, item := range s.sent {
		seen[item] = true
	}
	for item := range seen {
		// A trimmed or MTU-split leaf shares its original item's payload and
		// metadata charge. Retire the logical group once, not each wire image.
		c.queuedBytes -= retireSendItem(item)
	}
	s.queue = nil
	s.sent = make(map[uint64]*sendItem)
	c.recovery.Discard(recovery.Application)
	c.probeAllowance[applicationSpace] = false
	c.pendingDatagrams = 0
	for _, st := range c.streams.objects {
		c.releaseResetReservation(st)
		st.closeSend(ErrEarlyDataRejected)
		st.closeReceive(ErrEarlyDataRejected)
		if st.onRelease != nil {
			st.notifyRelease(ErrEarlyDataRejected)
		}
	}
	c.initStreams()
}

// EarlyReady reports whether a client can send with remembered early keys.
func (c *Conn) EarlyReady() bool { return c.sendingEarly() }

// Servers can send 1-RTT before receiving the client Finished. Peer transport
// limits and application keys must both be installed before exposing this state.
func (c *Conn) serverApplicationReady() bool {
	return c.config.Server && c.peerParams != nil && c.streams.havePeerLimits &&
		c.tls.WriteKeys(tls.QUICEncryptionLevelApplication) != nil
}

// EarlyDataRejected reports whether TLS rejected this client's early generation.
// Like other transport state accessors, it must run on the connection owner.
func (c *Conn) EarlyDataRejected() bool { return c.earlyRejected }
