package transport

import (
	"bytes"
	"errors"
	"sort"
	"time"

	"github.com/getlantern/photon/internal/packet"
	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
)

func (c *Conn) handleRetry(raw []byte, h wire.Header, now time.Time) error {
	if c.config.Server || c.retryUsed || c.peerCIDKnown || !c.spaces[initialSpace].sentAny ||
		bytes.Equal(h.SCID, c.config.OriginalDCID) || !packet.VerifyRetry(c.config.Version, c.config.OriginalDCID, raw) {
		return nil
	}
	write, read, err := packet.InitialKeys(c.config.Version, h.SCID)
	if err != nil {
		return c.fail(err)
	}
	c.retryUsed = true
	c.candidateInitial = candidateInitialKeys{}
	c.retrySourceCID = clone(h.SCID)
	c.retryToken = clone(h.Token)
	c.peerCID = clone(h.SCID)
	c.initialWrite, c.initialRead = write, read
	c.sealed[initialSpace] = 0
	// Preserve the TLS transcript and CRYPTO offsets, but repacketize to account
	// for the new token. No Initial has been processed, so none was acknowledged.
	s := &c.spaces[initialSpace]
	seen := map[*sendItem]bool{}
	for _, item := range s.queue {
		seen[item] = true
	}
	for _, item := range s.sent {
		seen[item] = true
	}
	var frames []wire.CryptoFrame
	for item := range seen {
		if item.acked {
			continue
		}
		c.queuedBytes -= item.bytes
		if f, ok := item.frame.(wire.CryptoFrame); ok {
			frames = append(frames, f)
		}
	}
	sort.Slice(frames, func(i, j int) bool { return frames[i].Offset < frames[j].Offset })
	s.queue = nil
	s.sent = make(map[uint64]*sendItem)
	s.clearACK()
	packetSize := c.basePacketPayloadLimit()
	header, err := c.makeHeader(initialSpace, s.nextPN, packetSize)
	if err != nil {
		return c.fail(err)
	}
	capacity := packetSize - len(header) - 16 - 17 // Worst-case CRYPTO type, offset and length.
	if capacity < 1 {
		return c.fail(errors.New("quic: Retry token exceeds Initial packet budget"))
	}
	for _, f := range frames {
		for offset := 0; offset < len(f.Data); {
			end := min(offset+capacity, len(f.Data))
			if err := c.enqueue(initialSpace, wire.CryptoFrame{Offset: f.Offset + uint64(offset), Data: clone(f.Data[offset:end])}, end-offset); err != nil {
				return err
			}
			offset = end
		}
	}
	// Repacketize early data with the Retry CID and fresh application packet numbers.
	c.queueLost(applicationSpace, c.recovery.Abandon(recovery.Application))
	c.spaces[applicationSpace].sent = make(map[uint64]*sendItem)
	c.recovery.ResetForRetry()
	c.congestion.Reset(uint64(packetSize))
	c.pacer = recovery.Pacer{}
	c.pacingAt = time.Time{}
	c.timer.DiscardKeys()
	c.probeAllowance = [3]bool{}
	c.deferred = nil
	c.deferredBytes = 0
	c.armTimer(now)
	return nil
}
