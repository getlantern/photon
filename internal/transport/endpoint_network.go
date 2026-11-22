package transport

import (
	"errors"
	"net/netip"
)

// bindNetworkOwner installs address-aware processing before the socket actor
// starts. Concrete local addresses are required until packet-info support can
// identify destinations received through wildcard sockets. This endpoint owns
// a fixed set of sockets; output selecting an unregistered tuple is rejected.
func (e *ServerEndpoint) bindNetworkOwner(addresses ...netip.AddrPort) error {
	if len(addresses) == 0 {
		return errors.New("quic: no socket addresses")
	}
	locals := make(map[netip.AddrPort]bool)
	for _, address := range addresses {
		tuple, err := canonicalPathTuple(address, address)
		if err != nil {
			return err
		}
		if locals[tuple.local] {
			return errors.New("quic: duplicate socket address")
		}
		locals[tuple.local] = true
	}
	for _, preferred := range e.config.PreferredAddresses {
		if !locals[canonicalPeer(preferred)] {
			return errors.New("quic: preferred address has no registered socket")
		}
	}
	local := canonicalPeer(addresses[0])
	owners := make(map[*Conn]*networkOwner)
	for _, c := range e.conns {
		owner, err := newNetworkOwner(c, local, e.router.entries[c].peer)
		if err != nil {
			return err
		}
		owners[c] = owner
	}
	e.config.allowActiveMigration = e.config.EnableMigration
	e.config.preferredSocketsReady = len(e.config.PreferredAddresses) > 0
	e.local = local
	e.locals = locals
	e.networks = owners
	return nil
}
