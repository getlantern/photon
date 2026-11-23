package stream

import (
	"bytes"
	"testing"
)

func TestWriteBufferOwnership(t *testing.T) {
	flow, _ := NewSendFlow(8)
	s, _ := NewSend(0, 8, flow)
	storage := bytes.Repeat([]byte{0xaa}, 16)
	input := []byte("abcdefghijk")
	frame, n, err := s.WriteBuffer(input, true, 16, storage)
	if err != nil || n != 8 || frame.Fin || string(frame.Data) != "abcdefgh" || &frame.Data[0] != &storage[0] {
		t.Fatal(frame, n, err)
	}
	input[0] = 'z'
	if string(frame.Data) != "abcdefgh" {
		t.Fatal("retained caller input")
	}
	before := append([]byte(nil), storage...)
	if _, _, err := s.WriteBuffer(input, false, 16, storage); err != ErrWouldBlock || !bytes.Equal(storage, before) {
		t.Fatal("blocked write modified storage", err)
	}
}
