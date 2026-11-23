package stream

import (
	"errors"
	"testing"

	"github.com/getlantern/photon/internal/wire"
)

func partialSender(t *testing.T, fin bool) (*Send, *SendFlow, wire.StreamFrame) {
	t.Helper()
	flow, _ := NewSendFlow(16)
	sender, _ := NewSend(4, 16, flow)
	frame, n, err := sender.Write([]byte("abcdefghijklmnop"), fin, 16)
	if err != nil || n != 16 {
		t.Fatal(n, err)
	}
	return sender, flow, frame
}

func TestSendPartialResetACKOrders(t *testing.T) {
	for _, resetFirst := range []bool{false, true} {
		s, flow, _ := partialSender(t, false)
		reset, err := s.ResetAt(7, 8)
		if err != nil || reset.FinalSize != 16 || reset.ReliableSize != 8 {
			t.Fatal(reset, err)
		}
		if _, _, err := s.Write([]byte("x"), false, 1); !errors.Is(err, ErrSendClosed) {
			t.Fatal("write after partial reset", err)
		}
		if resetFirst {
			err = s.AcknowledgeResetAt(reset)
		} else {
			err = s.AcknowledgePrefix(8)
		}
		if err != nil || s.Complete() {
			t.Fatal("one ACK obligation retired stream", err)
		}
		s.AcknowledgeReset() // No ordinary reset was sent.
		if s.Complete() {
			t.Fatal("ordinary reset ACK retired partial reset")
		}
		if resetFirst {
			err = s.AcknowledgePrefix(8)
		} else {
			err = s.AcknowledgeResetAt(reset)
		}
		if err != nil || !s.Complete() || flow.Used() != 16 {
			t.Fatal("partial reset completion/credit", err)
		}
		if err := s.AcknowledgePrefix(16); err != nil || flow.Used() != 16 {
			t.Fatal("late ACK accounting", err)
		}
	}
}

func TestSendPartialResetReduction(t *testing.T) {
	s, flow, original := partialSender(t, true)
	first, err := s.ResetAt(7, 12)
	if err != nil {
		t.Fatal(err)
	}
	// ACKing an old reset before reduction must not carry over to the new one.
	if err := s.AcknowledgeResetAt(first); err != nil {
		t.Fatal(err)
	}
	second, err := s.ResetAt(99, 6)
	if err != nil || second.ErrorCode != 7 || second.FinalSize != 16 {
		t.Fatal(second, err)
	}
	if first.ReliableSize != 12 {
		t.Fatal("new reset mutated queued frame")
	}
	if err := s.AcknowledgePrefix(6); err != nil {
		t.Fatal(err)
	}
	if err := s.AcknowledgeResetAt(first); err != nil || s.Complete() {
		t.Fatal("stale reset ACK completed reduced obligation", err)
	}
	retained, keep, err := s.RetainForRetransmission(original)
	if err != nil || !keep || string(retained.Data) != "abcdef" || retained.Fin || !original.Fin || string(original.Data) != "abcdefghijklmnop" {
		t.Fatal("retained prefix", retained, keep, err)
	}
	if _, keep, err := s.RetainForRetransmission(wire.StreamFrame{StreamID: 4, Offset: 6, Data: original.Data[6:]}); err != nil || keep {
		t.Fatal("retained abandoned suffix", err)
	}
	if flow.Used() != 16 {
		t.Fatal("trimming returned flow credit")
	}
	if err := s.AcknowledgeResetAt(second); err != nil || !s.Complete() {
		t.Fatal("new reset ACK did not complete", err)
	}
}

func TestSendPartialResetOrdinaryAndFINCompletion(t *testing.T) {
	s, _, frame := partialSender(t, false)
	partial, err := s.ResetAt(7, 8)
	if err != nil {
		t.Fatal(err)
	}
	ordinary, err := s.Reset(99)
	if err != nil || ordinary.ErrorCode != 7 || ordinary.FinalSize != 16 {
		t.Fatal(ordinary, err)
	}
	if _, keep, err := s.RetainForRetransmission(frame); err != nil || keep {
		t.Fatal("ordinary reset retained data", err)
	}
	if err := s.AcknowledgeResetAt(partial); err != nil || s.Complete() {
		t.Fatal("partial ACK retired ordinary reset", err)
	}
	s.AcknowledgeReset()
	if !s.Complete() {
		t.Fatal("ordinary reset not acknowledged")
	}
	s, _, _ = partialSender(t, true)
	if _, err := s.ResetAt(7, 8); err != nil {
		t.Fatal(err)
	}
	// The caller has proved every committed byte AND FIN acknowledged. This is
	// the alternative Data Recvd completion allowed by draft-11 section 4.
	s.AcknowledgeData()
	if !s.Complete() {
		t.Fatal("fully acknowledged FIN did not retire stream")
	}
	s, _, _ = partialSender(t, false)
	zero, err := s.ResetAt(7, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.AcknowledgeResetAt(zero); err != nil || !s.Complete() {
		t.Fatal("zero prefix required a data ACK", err)
	}
}

func TestSendPartialResetInvalidUpdates(t *testing.T) {
	s, flow, _ := partialSender(t, false)
	if _, err := s.ResetAt(7, 17); !errors.Is(err, ErrFinalSize) || s.resetAt != nil {
		t.Fatal("uncommitted reliable bytes", err)
	}
	reset, err := s.ResetAt(7, 8)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ResetAt(7, 9); !errors.Is(err, ErrStreamState) || *s.resetAt != reset {
		t.Fatal("increased reliable size", err)
	}
	if _, err := s.ResetAt(wire.MaxVarint+1, 4); !errors.Is(err, wire.ErrVarintRange) || *s.resetAt != reset {
		t.Fatal("invalid error code", err)
	}
	if err := s.AcknowledgePrefix(17); !errors.Is(err, ErrFinalSize) || s.prefixAcked != 0 {
		t.Fatal("ACK beyond committed data", err)
	}
	for _, invalid := range []wire.ResetStreamAtFrame{
		{StreamID: 8, ErrorCode: 7, FinalSize: 16, ReliableSize: 8},
		{StreamID: 4, ErrorCode: 8, FinalSize: 16, ReliableSize: 8},
		{StreamID: 4, ErrorCode: 7, FinalSize: 17, ReliableSize: 8},
		{StreamID: 4, ErrorCode: 7, FinalSize: 16, ReliableSize: 7},
	} {
		if err := s.AcknowledgeResetAt(invalid); !errors.Is(err, ErrStreamState) || s.resetAtAcked {
			t.Fatal("unrelated ACK changed state", err)
		}
	}
	if flow.Used() != 16 || s.Offset() != 16 {
		t.Fatal("invalid update changed credit")
	}
}
