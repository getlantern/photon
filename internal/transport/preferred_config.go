package transport

import (
	"bytes"
	"crypto/rand"
	"errors"
	"net/netip"

	"github.com/getlantern/photon/internal/wire"
)

func validatePreferredAddresses(addresses []netip.AddrPort) error {
	if len(addresses) > 2 {
		return errors.New("quic: at most two preferred addresses")
	}
	families := map[bool]bool{}
	for _, address := range addresses {
		tuple, err := canonicalPathTuple(address, address)
		if err != nil {
			return err
		}
		family := tuple.local.Addr().Is4()
		if families[family] {
			return errors.New("quic: duplicate preferred address family")
		}
		families[family] = true
	}
	return nil
}

// Called once per admitted connection, never on the shared listener template.
func makePreferredAddress(addresses []netip.AddrPort, initial []byte) (*wire.PreferredAddress, error) {
	return makePreferredAddressWithGenerator(addresses, initial, nil)
}
func makePreferredAddressWithGenerator(addresses []netip.AddrPort, initial []byte, generate func() ([]byte, error)) (*wire.PreferredAddress, error) {
	if err := validatePreferredAddresses(addresses); err != nil {
		return nil, err
	}
	if len(addresses) == 0 || len(initial) == 0 || len(initial) > 20 {
		return nil, errors.New("quic: invalid preferred address CID policy")
	}
	p := &wire.PreferredAddress{ConnectionID: make([]byte, len(initial))}
	for _, address := range addresses {
		address = canonicalPeer(address)
		if address.Addr().Is4() {
			p.IPv4, p.IPv4Port = address.Addr().As4(), address.Port()
		} else {
			p.IPv6, p.IPv6Port = address.Addr().As16(), address.Port()
		}
	}
	unique := false
	for tries := 0; tries < 16; tries++ {
		var err error
		p.ConnectionID, err = generateLocalCID(len(initial), generate)
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(p.ConnectionID, initial) {
			unique = true
			break
		}
	}
	if !unique {
		return nil, errors.New("quic: preferred CID generation repeatedly returned initial CID")
	}
	if _, err := rand.Read(p.StatelessResetToken[:]); err != nil {
		return nil, err
	}
	return p, nil
}
