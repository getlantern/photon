package transport

import (
	"bytes"
	"testing"

	"github.com/getlantern/photon/internal/wire"
)

func TestLocalPreferredCIDIssuanceAndRetirement(t *testing.T) {
	ids := newLocalIDs([]byte("initial0"))
	address := wire.PreferredAddress{ConnectionID: []byte("prefer01"), StatelessResetToken: [16]byte{7, 8, 9}}
	if err := ids.reservePreferred(&address); err != nil {
		t.Fatal(err)
	}
	address.ConnectionID[0] = 'X'
	if !ids.accept([]byte("prefer01")) || ids.active[1].token != address.StatelessResetToken {
		t.Fatal("CID ownership or token mismatch")
	}
	if frames, err := ids.issue(2); err != nil || len(frames) != 0 {
		t.Fatal("preferred CID not counted in peer limit", frames, err)
	}
	frames, err := ids.issue(4)
	if err != nil || len(frames) != 2 || frames[0].Sequence != 2 || frames[1].Sequence != 3 {
		t.Fatal("ordinary issuance reused preferred sequence", frames, err)
	}
	for _, frame := range frames {
		if bytes.Equal(frame.ConnectionID, []byte("prefer01")) {
			t.Fatal("preferred CID reused")
		}
	}
	if retired, err := ids.retire(1, []byte("prefer01")); err == nil || retired {
		t.Fatal("retired packet's destination CID")
	}
	if retired, err := ids.retire(1, []byte("initial0")); err != nil || !retired || ids.accept([]byte("prefer01")) {
		t.Fatal("preferred CID retirement", retired, err)
	}
	if retired, err := ids.retire(1, []byte("initial0")); err != nil || retired {
		t.Fatal("duplicate retirement", retired, err)
	}
	replacement, err := ids.issue(4)
	if err != nil || len(replacement) != 1 || replacement[0].Sequence != 4 {
		t.Fatal("replacement sequence", replacement, err)
	}
	if _, retained := ids.used["prefer01"]; !retained {
		t.Fatal("retirement removed uniqueness history")
	}
}

func TestLocalPreferredCIDRejectionIsAtomic(t *testing.T) {
	for _, scenario := range []string{"zero-initial", "oversize", "empty", "different-length", "duplicate", "late", "repeat"} {
		t.Run(scenario, func(t *testing.T) {
			ids := newLocalIDs([]byte("initial0"))
			address := wire.PreferredAddress{ConnectionID: []byte("prefer01")}
			switch scenario {
			case "zero-initial":
				ids = newLocalIDs(nil)
			case "oversize":
				ids = newLocalIDs(make([]byte, 21))
				address.ConnectionID = bytes.Repeat([]byte{1}, 21)
			case "empty":
				address.ConnectionID = nil
			case "different-length":
				address.ConnectionID = []byte("other")
			case "duplicate":
				address.ConnectionID = []byte("initial0")
			case "late":
				if _, err := ids.issue(2); err != nil {
					t.Fatal(err)
				}
			case "repeat":
				if err := ids.reservePreferred(&address); err != nil {
					t.Fatal(err)
				}
			}
			active, sent, used, highest := len(ids.active), len(ids.sent), len(ids.used), ids.highest
			if err := ids.reservePreferred(&address); err == nil {
				t.Fatal("invalid reservation accepted")
			}
			if len(ids.active) != active || len(ids.sent) != sent || len(ids.used) != used || ids.highest != highest {
				t.Fatal("rejection mutated CID state")
			}
		})
	}
}
