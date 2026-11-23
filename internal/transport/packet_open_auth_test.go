package transport

import (
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/packet"
	"github.com/getlantern/photon/internal/wire"
)

func TestAuthenticatedOpenErrorsRetainResetPrecedence(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, openErr := range []error{packet.ErrReservedBits, errKeyUpdate, packet.ErrAuthentication} {
			t.Run(version.String()+"/"+openErr.Error(), func(t *testing.T) {
				c, peer := testPair(t, version)
				drivePair(t, c, peer, false, false)
				before, failed := c.authenticatedPackets, c.failedAuth
				err := c.packetOpenFailure(openErr)
				if errors.Is(openErr, packet.ErrAuthentication) {
					if err != nil || c.authenticatedPackets != before || c.failedAuth != failed+1 {
						t.Fatal("failed authentication counted as success", err)
					}
				} else {
					var transportErr *TransportError
					if !errors.As(err, &transportErr) || c.authenticatedPackets != before+1 || c.failedAuth != failed {
						t.Fatal("authenticated protocol error lost", err)
					}
				}
			})
		}
	}
}

// These local fixture packets use InitialKeys(version, "12345678"), PN zero,
// unprotected header 48 00 and three padding bytes. Unlike the public sealer,
// the fixture construction permitted reserved bits before AEAD/header protection.
func TestNextKeyReservedBitsRemainAuthenticated(t *testing.T) {
	for _, fixture := range []struct {
		version wire.Version
		encoded string
	}{
		{wire.Version1, "56e81cc0681b263abbaa7efbb102d2dee45251715b"},
		{wire.Version2, "4cefbcfb6862b02a715dae8e4068ff52a3ceef1191"},
	} {
		t.Run(fixture.version.String(), func(t *testing.T) {
			c, peer := testPair(t, fixture.version)
			drivePair(t, c, peer, false, false)
			if err := c.prepareApplicationKeys(); err != nil {
				t.Fatal(err)
			}
			next, _, err := packet.InitialKeys(fixture.version, []byte("12345678"))
			if err != nil {
				t.Fatal(err)
			}
			c.applicationKeys.next = next
			raw, err := hex.DecodeString(fixture.encoded)
			if err != nil {
				t.Fatal(err)
			}
			// Current keys fail, but successful AEAD under next keys must take precedence.
			_, _, _, err = c.openApplication(raw, 1, 0, time.Unix(200, 0))
			if !errors.Is(err, packet.ErrReservedBits) {
				t.Fatal("next-key authenticated error masked", err)
			}
		})
	}
}
