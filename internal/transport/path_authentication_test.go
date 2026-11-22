package transport

import (
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestMultipathAuthenticationBeforeAdmission(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		client, server := sendingPathPair(t, version)
		client.multipath.paths[1].application.budget.Validate()
		client.multipath.paths[1].application.validation.state = PathValidated
		if err := client.queueFrameOnPath(1, wire.StreamFrame{StreamID: 0, Data: []byte("candidate"), Fin: true}); err != nil {
			t.Fatal(err)
		}
		now := time.Unix(101, 0)
		raw, err := client.nextPacketOnPath(1, now)
		if err != nil || raw == nil {
			t.Fatal(err)
		}
		app := server.multipath.paths[1].application
		beforeExpected, beforeCredit := app.space.received.Expected(), app.budget.credit
		bad := append([]byte(nil), raw...)
		bad[len(bad)-1] ^= 1
		forged, err := server.authenticateMultipathPacket(bad, now)
		if err != nil || forged != nil {
			t.Fatal("forged authentication result", err)
		}
		auth, err := server.authenticateMultipathPacket(raw, now)
		if err != nil || auth == nil || !auth.ackEliciting || len(auth.packet.frames) != 1 {
			t.Fatal("missing authenticated evidence", err)
		}
		if app.space.received.Expected() != beforeExpected || app.space.ackPending || app.budget.credit != beforeCredit || app.space.haveNonProbing {
			t.Fatal("authentication committed address/receipt state")
		}
		if _, err := server.AcceptStream(false); err == nil {
			t.Fatal("authentication dispatched stream data")
		}
		// Re-authentication remains possible if address policy declines admission.
		repeated, err := server.authenticateMultipathPacket(raw, now)
		if err != nil || repeated == nil {
			t.Fatal(err)
		}
		decoded, err := server.recordMultipathPacket(repeated, now)
		if err != nil || decoded == nil || decoded.receipt != NewPacket || !app.space.ackPending {
			t.Fatal("admission failed", err)
		}
		if err := server.dispatchMultipathPacket(decoded, now); err != nil {
			t.Fatal(err)
		}
		if _, err := server.AcceptStream(false); err != nil {
			t.Fatal(err)
		}
		duplicate, err := server.recordMultipathPacket(auth, now)
		if err != nil || duplicate == nil || duplicate.receipt == NewPacket || len(duplicate.frames) != 0 {
			t.Fatal("duplicate admission dispatched twice", err)
		}
		if len(auth.packet.frames) != 1 {
			t.Fatal("record erased authentication evidence")
		}
	}
}
