package interop

import (
	"sync/atomic"

	"github.com/quic-go/quic-go/qlog"
	"github.com/quic-go/quic-go/qlogwriter"
)

// Untagged: tests without the ECN socket constraint record key phases with it.
type ecnSenderTrace struct {
	datagrams chan qlog.ECN
	keyPhase  atomic.Uint64
}

func (t *ecnSenderTrace) AddProducer() qlogwriter.Recorder { return t }
func (t *ecnSenderTrace) SupportsSchemas(string) bool      { return true }
func (t *ecnSenderTrace) Close() error                     { return nil }
func (t *ecnSenderTrace) RecordEvent(event qlogwriter.Event) {
	if update, ok := event.(qlog.KeyUpdated); ok {
		t.keyPhase.Store(uint64(update.KeyPhase))
	}
	p, ok := event.(qlog.PacketReceived)
	if !ok || p.Header.PacketType != qlog.PacketType1RTT {
		return
	}
	for _, frame := range p.Frames {
		if _, ok := frame.Frame.(*qlog.DatagramFrame); ok {
			select {
			case t.datagrams <- p.ECN:
			default:
			}
			return
		}
	}
}
