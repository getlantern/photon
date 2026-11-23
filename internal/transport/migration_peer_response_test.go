package transport

import (
	"bytes"
	"github.com/getlantern/photon/internal/wire"
	"net/netip"
	"testing"
	"time"
)

func TestPeerMigrationPreservesResponsesOnCommit(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		client, server := sendingPathPair(t, v)
		local := netip.MustParseAddrPort("192.0.2.2:443")
		old := netip.MustParseAddrPort("192.0.2.1:444")
		changed := netip.MustParseAddrPort("192.0.2.1:445")
		if err := server.bindPreparedPath(1, local, old); err != nil {
			t.Fatal(err)
		}
		a := server.multipath.paths[1].application
		a.validation.state = PathValidated
		server.config.allowCandidateMigration = true
		now := time.Unix(101, 0)
		pn := uint64(0)
		send := func(remote netip.AddrPort, frames ...wire.Frame) []byte {
			t.Helper()
			var payload []byte
			for _, f := range frames {
				var err error
				payload, err = f.Append(payload)
				if err != nil {
					t.Fatal(err)
				}
			}
			payload = append(payload, make([]byte, 1200-len(payload))...)
			state := client.multipath.paths[1]
			raw := sealPathPayload(t, client.writeKeys(applicationSpace), state.peer.active[state.peer.current].cid, 1, pn, payload)
			pn++
			if err := server.handleBoundMultipathPacket(raw, local, remote, now); err != nil {
				t.Fatal(err)
			}
			now = now.Add(time.Millisecond)
			return raw
		}
		send(old, wire.PingFrame{})
		send(changed, wire.PingFrame{})
		if a.migration == nil || a.migration.candidate == nil {
			t.Fatal("no candidate")
		}
		candidate := a.migration.candidate
		raw, tuple, err := server.nextMigrationProbe(1, now)
		if err != nil || raw == nil || tuple.remote != changed {
			t.Fatal("candidate challenge", err)
		}
		decoded, err := client.decodeMultipathPacket(raw, now)
		if err != nil || decoded == nil {
			t.Fatal(err)
		}
		challenge, ok := decoded.frames[0].(wire.PathChallengeFrame)
		if !ok {
			t.Fatal("not challenge")
		}
		// Retire the original peer CID during candidate validation, then include
		// its obsolete advertisement in the challenge/data packet below.
		peerIDs := server.multipath.paths[1].peer
		obsolete, ok := peerIDs.active[0]
		if !ok {
			t.Fatal("missing original CID")
		}
		replacement, ok := peerIDs.active[1]
		if !ok {
			t.Fatal("missing spare CID")
		}
		cidFrame := func(seq, floor uint64, id peerID) wire.Frame {
			return wire.PathNewConnectionIDFrame{PathID: 1, NewConnectionIDFrame: wire.NewConnectionIDFrame{Sequence: seq, RetirePriorTo: floor, ConnectionID: id.cid, StatelessResetToken: id.token}}
		}
		send(changed, cidFrame(1, 1, replacement))
		beforeCIDReplay := server.queuedBytes
		expected := a.space.received.Expected()
		burst := send(changed, cidFrame(0, 0, obsolete), wire.PathChallengeFrame{Data: [8]byte{1}}, wire.PathChallengeFrame{Data: [8]byte{2}}, wire.StreamFrame{StreamID: 4, Data: []byte("withheld"), Fin: true})
		charge, queued := server.queuedBytes, len(a.space.queue)
		if charge <= beforeCIDReplay {
			t.Fatal("obsolete CID retirement not exercised")
		}
		for i := 0; i < 16; i++ {
			if err := server.handleBoundMultipathPacket(burst, local, changed, now); err != nil {
				t.Fatal("duplicate candidate", err)
			}
		}
		if server.queuedBytes != charge || len(a.space.queue) != queued {
			t.Fatal("encrypted replay queued CID retirements")
		}
		if a.space.received.Expected() != expected {
			t.Fatal("candidate replay admitted packet history")
		}

		if candidate.responsePending == nil || candidate.responseTail.count != 1 {
			t.Fatal("candidate burst lost")
		}
		payload := []byte("committing stream")
		send(changed, wire.PathResponseFrame{Data: challenge.Data}, wire.PathChallengeFrame{Data: [8]byte{3}}, wire.StreamFrame{StreamID: 0, Data: payload, Fin: true})
		if a.network.remote != changed || a.migrationResponse == nil || a.controlResponses.count != 2 || candidate.responsePending != nil || candidate.responseTail.count != 0 {
			t.Fatal("commit lost/duplicated responses")
		}
		for i := byte(1); i <= 3; i++ {
			now = now.Add(time.Millisecond)
			raw, err := server.nextPacketOnPath(1, now)
			if err != nil || len(raw) != 1200 {
				t.Fatal("response", len(raw), err)
			}
			decoded, err := client.decodeMultipathPacket(raw, now)
			if err != nil || decoded == nil {
				t.Fatal(err)
			}
			f, ok := decoded.frames[0].(wire.PathResponseFrame)
			if !ok || f.Data != ([8]byte{i}) {
				t.Fatal("response order")
			}
		}
		if err := server.handleBoundMultipathPacket(burst, local, changed, now); err != nil {
			t.Fatal("post-commit replay", err)
		}
		if server.queuedBytes != charge || len(a.space.queue) != queued {
			t.Fatal("post-commit CID processing repeated")
		}
		withheld := make([]byte, 8)
		if n, err := server.ReadStream(4, withheld); n != 8 || string(withheld) != "withheld" {
			t.Fatal("withheld data not admitted", n, err)
		}
		if a.hasPathResponse() {
			t.Fatal("duplicate response retained")
		}
		got := make([]byte, len(payload))
		if n, err := server.ReadStream(0, got); n != len(payload) || !bytes.Equal(got, payload) {
			t.Fatal("committing data not dispatched", n, err)
		}
	}
}
