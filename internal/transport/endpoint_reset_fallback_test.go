package transport

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestEndpointResetCIDCollision(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, network := range []bool{false, true} {
			for _, mode := range []string{"authenticated", "collision", "wrong-peer", "retired", "ambiguous", "other-peer", "wrong-local", "long-collision"} {
				if mode == "wrong-local" && !network {
					continue
				}
				t.Run(fmt.Sprintf("%s/network=%v/%s", version, network, mode), func(t *testing.T) {
					a, peerA := testPair(t, version, streamParameters())
					drivePair(t, a, peerA, false, false)
					b, peerB := testPair(t, version, streamParameters())
					drivePair(t, b, peerB, false, false)
					// Independent handshakes use fixed fixture CIDs. Give the second local
					// connection a distinct routing identity before joining the shared owner.
					b.config.LocalCID = []byte("othercid")
					b.localIDs = newLocalIDs(b.config.LocalCID)
					local := netip.MustParseAddrPort("192.0.2.1:443")
					remote := netip.MustParseAddrPort("192.0.2.2:444")
					e, err := NewClientOnlyEndpoint(8, 4)
					if err != nil {
						t.Fatal(err)
					}
					defer e.Close()
					if err := e.AddClient(a, remote); err != nil {
						t.Fatal(err)
					}
					bRemote := remote
					if mode == "other-peer" {
						bRemote = netip.MustParseAddrPort("192.0.2.8:444")
					}
					if err := e.AddClient(b, bRemote); err != nil {
						t.Fatal(err)
					}
					if network {
						if err := e.bindNetworkOwner(local); err != nil {
							t.Fatal(err)
						}
					}
					now := time.Unix(101, 0)
					raw := makePeerPacket(t, peerA, applicationSpace, 1000, wire.PingFrame{})
					prefixLength := 0
					if mode == "long-collision" {
						header, err := wire.AppendLongHeader(nil, version, wire.Handshake, a.config.LocalCID, peerA.config.LocalCID, nil, 0, 4, 16)
						if err != nil {
							t.Fatal(err)
						}
						prefix := append(header, make([]byte, 16)...)
						prefixLength = len(prefix)
						raw = append(prefix, raw...)
					}
					token := b.peerIDs.active[b.peerIDs.current]
					token.used, token.hasToken = true, true
					copy(token.token[:], raw[len(raw)-16:])
					b.peerIDs.active[b.peerIDs.current] = token
					var other *Conn
					if mode == "ambiguous" {
						var thirdPeer *Conn
						other, thirdPeer = testPair(t, version, streamParameters())
						drivePair(t, other, thirdPeer, false, false)
						other.config.LocalCID = []byte("thirdcid")
						other.localIDs = newLocalIDs(other.config.LocalCID)
						otherToken := other.peerIDs.active[other.peerIDs.current]
						otherToken.used, otherToken.hasToken, otherToken.token = true, true, token.token
						other.peerIDs.active[other.peerIDs.current] = otherToken
						if err := e.AddClient(other, remote); err != nil {
							t.Fatal(err)
						}
					}
					if mode != "authenticated" {
						raw[prefixLength+1+len(a.config.LocalCID)] ^= 1
					}
					if mode == "wrong-peer" {
						remote = netip.MustParseAddrPort("192.0.2.9:444")
					}
					if mode == "retired" {
						token.hasToken = false
						b.peerIDs.active[b.peerIDs.current] = token
					}
					if mode == "other-peer" {
						remote = bRemote
					}
					if mode == "wrong-local" {
						local = netip.MustParseAddrPort("192.0.2.1:445")
						e.locals[local] = true
					}
					before := a.authenticatedPackets
					selected, reply, err := e.handleNetworkDatagram(context.Background(), raw, local, remote, now)
					if mode == "collision" || mode == "other-peer" || mode == "long-collision" {
						if selected != b || reply != nil || !errors.Is(err, ErrStatelessReset) || !b.Draining() || a.Draining() {
							t.Fatal("reset collision hidden", selected == b, err, a.Draining(), b.Draining())
						}
					} else {
						if a.Draining() || b.Draining() || (other != nil && other.Draining()) || errors.Is(err, ErrStatelessReset) {
							t.Fatal("invalid reset acceptance", err)
						}
						if mode == "authenticated" && (selected != a || a.authenticatedPackets != before+1) {
							t.Fatal("valid packet lost to reset suffix", err)
						}
					}
				})
			}
		}
	}
}
