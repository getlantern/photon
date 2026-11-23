package stream

import (
	"errors"
	"testing"

	"github.com/getlantern/photon/internal/wire"
)

func TestSendPartialAndFlow(t *testing.T) {
	flow, _ := NewSendFlow(6)
	s, _ := NewSend(0, 4, flow)
	other, _ := NewSend(4, 10, flow)
	data := []byte("abcdef")
	first, n, err := s.Write(data, true, 3)
	if err != nil || n != 3 || first.Fin || first.Offset != 0 || string(first.Data) != "abc" {
		t.Fatal(first, n, err)
	}
	data[0] = 'X'
	if string(first.Data) != "abc" {
		t.Fatal("frame borrows application data")
	}
	second, n, err := s.Write(data[3:], true, 10)
	if err != nil || n != 1 || second.Fin || second.Offset != 3 {
		t.Fatal(second, n, err)
	}
	if _, _, err = s.Write(data[4:], true, 10); !errors.Is(err, ErrWouldBlock) {
		t.Fatal(err)
	}
	if _, n, err = other.Write([]byte("xyz"), false, 10); err != nil || n != 2 {
		t.Fatal("shared limit", n, err)
	}
	s.UpdateLimit(20)
	if _, _, err = s.Write(data[4:], true, 10); !errors.Is(err, ErrWouldBlock) {
		t.Fatal("connection limit bypass", err)
	}
	flow.UpdateLimit(8)
	last, n, err := s.Write(data[4:], true, 10)
	if err != nil || n != 2 || !last.Fin || last.Offset != 4 || flow.Used() != 8 {
		t.Fatal(last, n, err)
	}
	if _, _, err = s.Write(nil, true, 10); !errors.Is(err, ErrSendClosed) {
		t.Fatal(err)
	}
}

func TestSendResetAndZeroCreditFIN(t *testing.T) {
	flow, _ := NewSendFlow(0)
	s, _ := NewSend(2, 0, flow)
	frame, n, err := s.Write(nil, true, 1)
	if err != nil || n != 0 || !frame.Fin || frame.Offset != 0 {
		t.Fatal(frame, n, err)
	}
	reset, err := s.Reset(42)
	if err != nil || reset.FinalSize != 0 || reset.ErrorCode != 42 {
		t.Fatal(reset, err)
	}
	again, _ := s.Reset(99)
	if again != reset {
		t.Fatal("changed reset")
	}
	s.AcknowledgeData()
	if s.Complete() {
		t.Fatal("FIN ACK completed reset sender")
	}
	s.AcknowledgeReset()
	if !s.Complete() {
		t.Fatal("reset ACK not terminal")
	}
	if _, err = s.Reset(0); !errors.Is(err, ErrSendClosed) {
		t.Fatal(err)
	}
}

func TestSendLimitsAndResetAccounting(t *testing.T) {
	flow, _ := NewSendFlow(100)
	s, _ := NewSend(0, 10, flow)
	s.UpdateLimit(5)
	flow.UpdateLimit(2)
	frame, n, err := s.Write([]byte("0123456789"), false, 100)
	if err != nil || n != 10 {
		t.Fatal("stale limits reduced credit", n, err)
	}
	frame.Append(nil)
	frame.Append(nil) // Reserializing for retransmission consumes no new credit.
	reset, err := s.Reset(7)
	if err != nil || reset.FinalSize != 10 || flow.Used() != 10 {
		t.Fatal(reset, err, flow.Used())
	}
	if err = s.UpdateLimit(wire.MaxVarint + 1); !errors.Is(err, ErrFlowControl) {
		t.Fatal(err)
	}
	if err = flow.UpdateLimit(wire.MaxVarint + 1); !errors.Is(err, ErrFlowControl) {
		t.Fatal(err)
	}
	if _, _, err = s.Write([]byte("x"), false, 1); !errors.Is(err, ErrSendClosed) {
		t.Fatal(err)
	}
}
