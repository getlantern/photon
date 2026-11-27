package transport

import (
	"errors"

	"github.com/getlantern/photon/internal/wire"
)

var errNoPathForControl = errors.New("quic: no path for control response")

// queuePathControl selects a prepared sending context. Final path validation,
// amplification, congestion and pacing checks belong to the outgoing scheduler.
func (c *Conn) queuePathControl(preferred uint64, frame wire.Frame) error {
	usable := func(id uint64) bool {
		s := c.multipath.paths[id]
		return s != nil && s.application != nil && c.multipath.abandoned[id] == nil && len(s.peer.active[s.peer.current].cid) > 0
	}
	selected := preferred
	if !usable(selected) {
		found := false
		for id := range c.multipath.paths {
			if usable(id) && (!found || id < selected) {
				selected = id
				found = true
			}
		}
		if !found {
			return errNoPathForControl
		}
	}
	return c.queueFrameOnPath(selected, frame)
}

// queueFrameOnPath preserves path affinity for challenge/response frames.
func (c *Conn) queueFrameOnPath(selected uint64, frame wire.Frame) error {
	state := c.multipath.paths[selected]
	if state == nil || state.application == nil || c.multipath.abandoned[selected] != nil {
		return errNoPathForControl
	}
	frame = c.pathWireFrame(frame)
	encoded, err := frame.Append(nil)
	if err != nil {
		return err
	}
	if c.multipath.baseOnly {
		if err := wire.ValidateFrame(frame, wire.FrameContext{PacketType: wire.OneRTT, SenderIsServer: c.config.Server, ResetStreamAtEnabled: c.peerParams.ResetStreamAt, MaxDatagramFrameSize: c.peerParams.MaxDatagramFrameSize}, len(encoded)); err != nil {
			return err
		}
	}
	size := max(len(encoded), 32)
	if size > c.availableSendBytes() {
		return ErrSendBuffer
	}
	s := c.multipath.paths[selected].application.space
	s.queue = append(s.queue, &sendItem{frame: frame, bytes: size, queued: true})
	c.countRetire(s.queue[len(s.queue)-1])
	if frame.Type() == 0x1a || frame.Type() == 0x1b {
		item := s.queue[len(s.queue)-1]
		// Responses retain arrival order and precede locally generated challenges.
		// New validation work must not displace a peer's pending response.
		index := 0
		for index < len(s.queue)-1 {
			if _, ok := s.queue[index].frame.(wire.PathResponseFrame); !ok {
				break
			}
			index++
		}
		copy(s.queue[index+1:], s.queue[index:len(s.queue)-1])
		s.queue[index] = item
	}
	c.queuedBytes += size
	return nil
}

func (p *pathCIDs) receivePathsBlocked(f wire.PathsBlockedFrame) error {
	if f.Maximum > p.localMax {
		return pathCIDViolation(f.Type(), "blocked path limit exceeds advertised maximum")
	}
	return nil // Informational; granting a larger limit is local policy.
}

func (p *pathCIDs) receivePathCIDsBlocked(f wire.PathCIDsBlockedFrame) error {
	if f.PathID > p.localMax {
		return pathCIDViolation(f.Type(), "blocked CID path exceeds advertised maximum")
	}
	if p.abandoned[f.PathID] != nil {
		return nil
	}
	var next uint64
	if s := p.paths[f.PathID]; s != nil && s.local.issued {
		next = s.local.highest + 1
	}
	if f.NextSequence > next {
		return pathCIDViolation(f.Type(), "blocked CID sequence exceeds next issuance")
	}
	return nil // Informational; no allocation or automatic amplification.
}
