package transport

import (
	"github.com/getlantern/photon/internal/wire"
	"testing"
)

func TestResetStreamAtNegotiatedAdvertisement(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		p := streamParameters()
		p.ResetStreamAt = true
		client, server := testPair(t, version, p)
		drivePair(t, client, server, false, false)
		for _, conn := range []*Conn{client, server} {
			local, remote := conn.PartialResetCapabilities()
			if !local || !remote {
				t.Fatal("missing negotiated support", local, remote)
			}
		}
	}
}
