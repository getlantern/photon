package recovery

import (
	"testing"
	"time"
)

func TestRecoveryOwnsECNPolicy(t *testing.T) {
	r := tracker(t)
	if r.ECNMarking(Application) != NotECT {
		t.Fatal("enabled without socket support")
	}
	r.EnableECN()
	at := time.Unix(100, 0)
	for pn := uint64(0); pn < 10; pn++ {
		mark := r.ECNMarking(Application)
		if mark != ECT0 {
			t.Fatal("probe marking", pn, mark)
		}
		if err := r.OnSent(Application, Packet{Number: pn, ECN: mark, SentAt: at, Bytes: 1400, InFlight: true, AckEliciting: true}); err != nil {
			t.Fatal(err)
		}
	}
	if r.ECNState() != ECNUnknown || r.ECNMarking(Application) != NotECT {
		t.Fatal("unbounded testing")
	}
	// The tenth commit was rejected locally; it must not consume a probe.
	if !r.RejectLargePacket(9) || r.ECNState() != ECNTesting {
		t.Fatal("local rejection consumed probe")
	}
	if err := r.OnSent(Application, Packet{Number: 10, ECN: r.ECNMarking(Application), SentAt: at, Bytes: 1200, InFlight: true, AckEliciting: true}); err != nil {
		t.Fatal(err)
	}
	frame := ack(10, 10)
	frame.ECN = &[3]uint64{1, 0, 0}
	if _, err := r.OnACK(Application, frame, at.Add(time.Millisecond), 0, 0, true); err != nil {
		t.Fatal(err)
	}
	if r.ECNState() != ECNCapable || r.ECNMarking(Application) != ECT0 {
		t.Fatal("validated ACK did not enable marking")
	}
	r.ResetForPathChange()
	if r.ECNState() != ECNTesting {
		t.Fatal("new path inherited capability")
	}
	old := ack(0, 0)
	old.ECN = &[3]uint64{2, 0, 0}
	if _, err := r.OnACK(Application, old, at.Add(2*time.Millisecond), 0, 0, true); err != nil {
		t.Fatal(err)
	}
	if r.ECNState() != ECNTesting {
		t.Fatal("old path ACK validated new path")
	}
}

func TestRecoveryECNFailureAndCapacity(t *testing.T) {
	r, _ := NewSent(1)
	r.EnableECN()
	if err := r.OnSent(Application, Packet{ECN: ECT0}); err != nil {
		t.Fatal(err)
	}
	if r.ECNMarking(Application) != NotECT || r.ECNMarking(Handshake) != ECT0 {
		t.Fatal("capacity not scoped to space")
	}
	if _, err := r.OnACK(Application, ack(0, 0), time.Now(), 0, 0, true); err != nil {
		t.Fatal(err)
	}
	if r.ECNState() != ECNFailed || r.ECNMarking(Handshake) != NotECT {
		t.Fatal("failure did not span spaces")
	}
	r.EnableECN()
	r.ResetForPathChange()
	if r.ECNState() != ECNFailed {
		t.Fatal("sticky failure revived without feedback rebaseline")
	}
}

func TestRecoveryECNAllProbesLost(t *testing.T) {
	r := tracker(t)
	r.EnableECN()
	at := time.Unix(100, 0)
	for pn := uint64(0); pn < 13; pn++ {
		if err := r.OnSent(Application, Packet{Number: pn, ECN: r.ECNMarking(Application), SentAt: at, Bytes: 1200, InFlight: true, AckEliciting: true}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := r.OnACK(Application, ack(12, 12), at.Add(time.Millisecond), 0, 0, true); err != nil {
		t.Fatal(err)
	}
	if r.ECNState() != ECNFailed {
		t.Fatal("loss callbacks did not fail ECN", r.ECNState())
	}
}
