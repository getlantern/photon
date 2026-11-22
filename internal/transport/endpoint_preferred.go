package transport

import (
	"errors"
	"net/netip"
	"time"
)

// startPreferredMigration chooses among already registered concrete sockets.
// It does not allocate sockets or reuse an attempted destination's CID. A failed
// validation leaves ordinary traffic on the original path and is not restarted
// on every send-loop iteration.
func (e *ServerEndpoint) startPreferredMigration(c *Conn, now time.Time) error {
	if c.config.Server || !c.confirmed || c.preferredAttempted || c.multipath == nil || c.peerParams == nil || c.peerParams.PreferredAddress == nil || c.Closing() || c.Draining() {
		return nil
	}
	state := c.multipath.paths[0]
	if state == nil || state.application == nil || c.multipath.abandoned[0] != nil {
		return nil
	}
	app := state.application
	if app.network == nil || app.localMigration != nil || (app.migration != nil && app.migration.candidate != nil) {
		return nil
	}
	p := c.peerParams.PreferredAddress
	destinations := [2]netip.AddrPort{netip.AddrPortFrom(netip.AddrFrom4(p.IPv4), p.IPv4Port), netip.AddrPortFrom(netip.AddrFrom16(p.IPv6), p.IPv6Port)}
	if app.network.local.Addr().Is6() {
		destinations[0], destinations[1] = destinations[1], destinations[0]
	}
	for _, remote := range destinations {
		if !c.preferredDestination(remote) || remote == app.network.remote {
			continue
		}
		var local netip.AddrPort
		if app.network.local.Addr().Is4() == remote.Addr().Is4() && e.locals[app.network.local] {
			local = app.network.local
		} else {
			for candidate, live := range e.locals {
				if live && candidate.Addr().Is4() == remote.Addr().Is4() && (!local.IsValid() || candidate.Compare(local) < 0) {
					local = candidate
				}
			}
		}
		if !local.IsValid() {
			continue
		}
		_, err := c.preparePreferredMigration(local, remote, now)
		if errors.Is(err, errPathCIDCapacity) {
			return nil
		} // A later CID may unblock admission.
		if err != nil {
			return err
		}
		c.preferredAttempted = true
		return nil
	}
	return nil
}
