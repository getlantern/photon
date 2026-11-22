package transport

import "net/netip"

// CurrentRemoteAddr is safe outside the owner goroutine. It reports the last
// committed primary-path address. Additional multipath paths do not replace
// this primary identity; abandonment/shutdown retain the last value.
func (c *Conn) CurrentRemoteAddr() netip.AddrPort {
	if value := c.remoteSnapshot.Load(); value != nil {
		return *value
	}
	return netip.AddrPort{}
}

// publishRemoteAddress runs on the owner. Stored values are immutable.
func (c *Conn) publishRemoteAddress(remote netip.AddrPort) {
	if c.CurrentRemoteAddr() == remote {
		return
	}
	c.remoteSnapshot.Store(&remote)
}

// A copy-on-write snapshot keeps both addresses consistent for concurrent readers.
// Path IDs are never reused; the engine's lifetime path limit bounds retention.
type pathAddressSnapshot map[uint64]pathTuple

// CurrentPathAddresses reports the last bound tuple, including after abandonment
// or shutdown. Existence does not imply that the path is currently usable.
func (c *Conn) CurrentPathAddresses(id uint64) (local, remote netip.AddrPort, ok bool) {
	if snapshot := c.pathSnapshots.Load(); snapshot != nil {
		tuple, found := (*snapshot)[id]
		return tuple.local, tuple.remote, found
	}
	return netip.AddrPort{}, netip.AddrPort{}, false
}

// publishPathAddress runs only on the owner, after committing a binding.
func (c *Conn) publishPathAddress(id uint64, tuple pathTuple) {
	old := c.pathSnapshots.Load()
	if old != nil && (*old)[id] == tuple {
		return
	}
	next := make(pathAddressSnapshot)
	if old != nil {
		for id, value := range *old {
			next[id] = value
		}
	}
	next[id] = tuple
	c.pathSnapshots.Store(&next)
}
