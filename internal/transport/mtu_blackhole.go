package transport

import (
	"github.com/getlantern/photon/internal/recovery"
	"time"
)

// This conservative heuristic is not proof of an MTU failure. It requires
// three ordinary large-packet losses and a later acknowledged small packet.
// Congestion control still processes all ordinary losses normally.
type mtuBlackholeState struct {
	floor                  uint64
	losses                 uint8
	lastLost, lastSmallACK uint64
	haveSmallACK           bool
}

func (a *pathApplication) observeMTUBlackhole(acked, lost []recovery.Packet, now time.Time) bool {
	m := a.mtu
	if m == nil || m.confirmed <= minimumQUICMTU {
		return false
	}
	b := &a.mtuBlackhole
	// A newer successful large packet invalidates older failure evidence,
	// irrespective of ACK range order or which callback reported the loss.
	for _, p := range acked {
		if (!p.MTUProbe || p.Bytes >= m.confirmed) && p.AckEliciting && p.Bytes > minimumQUICMTU && p.Number >= b.floor {
			*b = mtuBlackholeState{floor: p.Number + 1}
		}
	}
	for _, p := range acked {
		if !p.MTUProbe && p.AckEliciting && p.Bytes <= minimumQUICMTU && p.Number >= b.floor && (!b.haveSmallACK || p.Number > b.lastSmallACK) {
			b.lastSmallACK, b.haveSmallACK = p.Number, true
		}
	}
	for _, p := range lost {
		if p.MTUProbe || !p.AckEliciting || p.Bytes <= minimumQUICMTU || p.Bytes > m.confirmed || p.Number < b.floor {
			continue
		}
		if b.losses < 3 {
			b.losses++
		}
		b.lastLost = max(b.lastLost, p.Number)
	}
	if b.losses < 3 || !b.haveSmallACK || b.lastSmallACK <= b.lastLost {
		return false
	}
	a.resetMTU()
	a.mtuNeedsValidation = true
	// Hold at the baseline before reopening upward discovery. Old probe ACKs
	// cannot restore the prior size because reset discarded their identities.
	m.upper = minimumQUICMTU
	a.updateMTURaise(now)
	return true
}
