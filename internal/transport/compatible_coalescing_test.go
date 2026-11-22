package transport

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestCompatibleVersionCoalescedTransition(t *testing.T) {
	for _, original := range []wire.Version{wire.Version1, wire.Version2} {
		selected := wire.Version1
		if selected == original {
			selected = wire.Version2
		}
		for _, order := range []string{"old-first", "old-last", "separate-old", "selected-then-old-prefix"} {
			t.Run(fmt.Sprintf("%x/%s", original, order), func(t *testing.T) {
				client, server := compatiblePair(t, original, selected)
				// Send only the beginning of ClientHello so the server can acknowledge in
				// the original version before it can inspect version transport parameters.
				item := client.spaces[initialSpace].queue[0]
				crypto, ok := item.frame.(wire.CryptoFrame)
				if !ok || len(crypto.Data) < 2 {
					t.Fatal("missing ClientHello")
				}
				item.frame = wire.CryptoFrame{Offset: crypto.Offset, Data: crypto.Data[:1]}
				remainder := wire.CryptoFrame{Offset: crypto.Offset + 1, Data: clone(crypto.Data[1:])}
				if err := client.enqueue(initialSpace, remainder, len(remainder.Data)+16); err != nil {
					t.Fatal(err)
				}
				now := time.Unix(100, 0)
				partial, err := client.NextDatagram(now)
				if err != nil || len(partial) == 0 {
					t.Fatal(err)
				}
				if err := server.HandleDatagram(partial, now); err != nil {
					t.Fatal(err)
				}
				old := drain(t, server, now)
				if len(old) != 1 || server.versionChosen {
					t.Fatal("server selected before receiving complete parameters")
				}
				oldHeader, err := server.parseHeader(old[0])
				if err != nil || oldHeader.Version != original {
					t.Fatal("not an original-version ACK", err)
				}
				_, plain, oldPN, err := server.initialWrite.Open(old[0], oldHeader.PNOffset, 0)
				if err != nil {
					t.Fatal(err)
				}
				f, _, err := wire.ConsumeFrame(plain)
				if err != nil {
					t.Fatal(err)
				}
				if _, ok := f.(wire.AckFrame); !ok {
					t.Fatal("early response was not ACK")
				}
				numbers, recovery := server.spaces[initialSpace].received, server.recovery
				beforePN := server.spaces[initialSpace].nextPN
				if order == "separate-old" {
					if err := client.HandleDatagram(old[0], now); err != nil {
						t.Fatal(err)
					}
					if client.versionChosen {
						t.Fatal("ACK fixed the version before server CRYPTO")
					}
				}
				for _, p := range drain(t, client, now) {
					if err := server.HandleDatagram(p, now); err != nil {
						t.Fatal(err)
					}
				}
				if server.config.Version != selected || server.spaces[initialSpace].received != numbers || server.recovery != recovery || server.spaces[initialSpace].nextPN != beforePN {
					t.Fatal("switch reset connection state")
				}
				flight := drain(t, server, now)
				initials, handshakes := 0, 0
				for _, p := range flight {
					h, err := server.parseHeader(p)
					if err != nil {
						t.Fatal(err)
					}
					if h.Long && h.Version != selected {
						t.Fatal("server emitted original version after selecting")
					}
					switch h.Type {
					case wire.Initial:
						_, _, pn, err := server.initialWrite.Open(p, h.PNOffset, 0)
						if err != nil || pn <= oldPN {
							t.Fatal("Initial packet number restarted", pn, err)
						}
						oldPN = pn
						initials++
					case wire.Handshake:
						handshakes++
					}
				}
				if initials == 0 || handshakes == 0 {
					t.Fatal("missing negotiated packet spaces")
				}
				combined := bytes.Join(flight, nil)
				if order == "selected-then-old-prefix" {
					if err := client.HandleDatagram(flight[0], now); err != nil {
						t.Fatal(err)
					}
					if client.config.Version != selected {
						t.Fatal("first negotiated packet did not select version")
					}
					combined = append(clone(old[0]), bytes.Join(flight[1:], nil)...)
				}
				if order == "old-first" {
					combined = append(clone(old[0]), combined...)
				}
				if order == "old-last" {
					combined = nil
					inserted := false
					for _, p := range flight {
						if !inserted && p[0]&0x80 == 0 {
							combined = append(combined, old[0]...)
							inserted = true
						}
						combined = append(combined, p...)
					}
					if !inserted {
						combined = append(combined, old[0]...)
					}
				}
				if err := client.HandleDatagram(combined, now); err != nil {
					t.Fatal(err)
				}
				if client.config.Version != selected || !client.versionChosen {
					t.Fatal("coalesced transition not selected")
				}
				if order == "selected-then-old-prefix" && !client.complete {
					t.Fatal("late original packet discarded fresh negotiated packets in its datagram")
				}
				// Duplicate the same wire image: no new encryption or reused packet nonce.
				if err := client.HandleDatagram(combined, now); err != nil {
					t.Fatal("duplicate", err)
				}
				drivePair(t, client, server, false, false)
			})
		}
	}
}
